package objectstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/lgoyal6/gpu-broker/controlplane/internal/application"
)

const (
	maxManifestBytes = 4 << 20
	maxObjectChunks  = 4096
)

// S3Client is the SDK boundary. Conditional writes, signing, retries and
// credential refresh remain the SDK's responsibility.
type S3Client interface {
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

type S3 struct {
	client S3Client
	bucket string
	prefix string
}

type s3Chunk struct {
	SHA256 string `json:"sha256"`
	Size   int    `json:"size"`
}

type s3Manifest struct {
	Version int       `json:"version"`
	Size    int64     `json:"size"`
	Chunks  []s3Chunk `json:"chunks"`
	Sealed  bool      `json:"sealed"`
	SHA256  string    `json:"sha256,omitempty"`
}

func NewS3(client S3Client, bucket, prefix string) (*S3, error) {
	prefix = strings.Trim(prefix, "/")
	if client == nil || bucket == "" || (prefix != "" && !validS3Key(prefix)) {
		return nil, fmt.Errorf("%w: S3 client, bucket and safe prefix required", application.ErrInvalid)
	}
	return &S3{client: client, bucket: bucket, prefix: prefix}, nil
}

func validS3Key(key string) bool {
	return key != "" && len(key) <= 512 && !strings.Contains(key, "..") &&
		!strings.HasPrefix(key, "/") && path.Clean(key) == key
}

func (s *S3) root(key string) (string, error) {
	if !validS3Key(key) {
		return "", application.ErrInvalid
	}
	return path.Join(s.prefix, key), nil
}

func apiCode(err error, codes ...string) bool {
	var api smithy.APIError
	if !errors.As(err, &api) {
		return false
	}
	for _, code := range codes {
		if api.ErrorCode() == code {
			return true
		}
	}
	return false
}

func storageError(err error) error {
	return fmt.Errorf("%w: S3 storage: %w", application.ErrUnavailable, err)
}

func (s *S3) load(ctx context.Context, root string) (s3Manifest, string, error) {
	res, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(root + "/manifest.json"),
	})
	if apiCode(err, "NoSuchKey", "NotFound") {
		return s3Manifest{Version: 1, Chunks: []s3Chunk{}}, "", nil
	}
	if err != nil {
		return s3Manifest{}, "", storageError(err)
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(res.Body, maxManifestBytes+1))
	if err != nil {
		return s3Manifest{}, "", storageError(err)
	}
	if len(raw) > maxManifestBytes || aws.ToString(res.ETag) == "" {
		return s3Manifest{}, "", storageError(errors.New("manifest too large or ETag missing"))
	}
	var m s3Manifest
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&m); err != nil {
		return m, "", storageError(err)
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return m, "", storageError(errors.New("trailing manifest data"))
	}
	if m.Version != 1 || m.Size < 0 || len(m.Chunks) > maxObjectChunks {
		return m, "", storageError(errors.New("invalid manifest version or limits"))
	}
	var total int64
	for _, chunk := range m.Chunks {
		digest, err := hex.DecodeString(chunk.SHA256)
		if err != nil || len(digest) != sha256.Size || strings.ToLower(chunk.SHA256) != chunk.SHA256 ||
			chunk.Size <= 0 || chunk.Size > maxChunkBytes {
			return m, "", storageError(errors.New("invalid chunk descriptor"))
		}
		total += int64(chunk.Size)
	}
	if total != m.Size || (!m.Sealed && m.SHA256 != "") {
		return m, "", storageError(errors.New("inconsistent manifest"))
	}
	if m.Sealed {
		digest, err := hex.DecodeString(m.SHA256)
		if err != nil || len(digest) != sha256.Size || strings.ToLower(m.SHA256) != m.SHA256 {
			return m, "", storageError(errors.New("invalid seal digest"))
		}
	}
	return m, aws.ToString(res.ETag), nil
}

func (s *S3) publish(ctx context.Context, root, etag string, m s3Manifest) error {
	raw, err := json.Marshal(m)
	if err != nil {
		return err
	}
	if len(raw) > maxManifestBytes {
		return application.ErrInvalid
	}
	input := &s3.PutObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(root + "/manifest.json"),
		Body: bytes.NewReader(raw), ContentType: aws.String("application/json")}
	if etag == "" {
		input.IfNoneMatch = aws.String("*")
	} else {
		input.IfMatch = aws.String(etag)
	}
	_, err = s.client.PutObject(ctx, input)
	return err
}

func (s *S3) Append(ctx context.Context, key string, offset int64, data []byte) (int64, error) {
	if offset < 0 || len(data) > maxChunkBytes {
		return 0, application.ErrInvalid
	}
	root, err := s.root(key)
	if err != nil {
		return 0, err
	}
	m, etag, err := s.load(ctx, root)
	if err != nil {
		return 0, err
	}
	if m.Sealed {
		return m.Size, fmt.Errorf("%w: object is sealed", application.ErrConflict)
	}
	if offset != m.Size {
		return m.Size, &application.OffsetMismatchError{Committed: m.Size}
	}
	if len(data) == 0 && etag != "" {
		return m.Size, nil
	}
	if len(data) > 0 {
		if len(m.Chunks) == maxObjectChunks {
			return m.Size, application.ErrInvalid
		}
		digest := hashBytes(data)
		_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(s.bucket), Key: aws.String(root + "/chunks/" + digest),
			Body: bytes.NewReader(data), IfNoneMatch: aws.String("*"),
		})
		// The digest is the identity of an immutable chunk. A repeated write
		// after a lost response is safe; readers still verify the actual bytes.
		if err != nil && !apiCode(err, "PreconditionFailed") {
			return m.Size, storageError(err)
		}
		m.Chunks = append(m.Chunks, s3Chunk{SHA256: digest, Size: len(data)})
		m.Size += int64(len(data))
	}
	if err := s.publish(ctx, root, etag, m); err != nil {
		if !apiCode(err, "PreconditionFailed", "ConditionalRequestConflict") {
			return 0, storageError(err)
		}
		current, _, err := s.load(ctx, root)
		if err != nil {
			return 0, err
		}
		if current.Sealed {
			return current.Size, application.ErrConflict
		}
		return current.Size, &application.OffsetMismatchError{Committed: current.Size}
	}
	return m.Size, nil
}

func (s *S3) Size(ctx context.Context, key string) (int64, error) {
	root, err := s.root(key)
	if err != nil {
		return 0, err
	}
	m, _, err := s.load(ctx, root)
	return m.Size, err
}

func (s *S3) chunk(ctx context.Context, root string, c s3Chunk) ([]byte, error) {
	res, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(root + "/chunks/" + c.SHA256),
	})
	if err != nil {
		return nil, storageError(err)
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(res.Body, int64(c.Size)+1))
	if err != nil {
		return nil, storageError(err)
	}
	if len(raw) != c.Size || hashBytes(raw) != c.SHA256 {
		return nil, storageError(errors.New("artifact chunk integrity failure"))
	}
	return raw, nil
}

func (s *S3) Read(ctx context.Context, key string, offset int64, max int) ([]byte, error) {
	if offset < 0 || max < 0 || max > maxChunkBytes {
		return nil, application.ErrInvalid
	}
	root, err := s.root(key)
	if err != nil {
		return nil, err
	}
	m, etag, err := s.load(ctx, root)
	if err != nil {
		return nil, err
	}
	if etag == "" {
		return nil, application.ErrNotFound
	}
	if offset >= m.Size || max == 0 {
		return []byte{}, nil
	}
	remaining := min(int64(max), m.Size-offset)
	out := make([]byte, 0, int(remaining))
	var start int64
	for _, c := range m.Chunks {
		end := start + int64(c.Size)
		if offset < end {
			raw, err := s.chunk(ctx, root, c)
			if err != nil {
				return nil, err
			}
			first := maxInt64(0, offset-start)
			n := min(remaining, int64(c.Size)-first)
			out = append(out, raw[first:first+n]...)
			remaining -= n
			offset += n
			if remaining == 0 {
				break
			}
		}
		start = end
	}
	return out, nil
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func (s *S3) digest(ctx context.Context, root string, m s3Manifest) (string, error) {
	h := sha256.New()
	for _, c := range m.Chunks {
		raw, err := s.chunk(ctx, root, c)
		if err != nil {
			return "", err
		}
		_, _ = h.Write(raw)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func (s *S3) Digest(ctx context.Context, key string) (string, error) {
	root, err := s.root(key)
	if err != nil {
		return "", err
	}
	m, etag, err := s.load(ctx, root)
	if err != nil {
		return "", err
	}
	if etag == "" {
		return "", application.ErrNotFound
	}
	return s.digest(ctx, root, m)
}

func (s *S3) Seal(ctx context.Context, key, want string) (int64, error) {
	root, err := s.root(key)
	if err != nil {
		return 0, err
	}
	m, etag, err := s.load(ctx, root)
	if err != nil {
		return 0, err
	}
	if etag == "" {
		return 0, application.ErrNotFound
	}
	if m.Sealed {
		if m.SHA256 != want {
			return m.Size, application.ErrConflict
		}
		return m.Size, nil
	}
	got, err := s.digest(ctx, root, m)
	if err != nil {
		return 0, err
	}
	if got != want {
		return m.Size, fmt.Errorf("%w: artifact digest mismatch", application.ErrConflict)
	}
	m.Sealed, m.SHA256 = true, got
	if err := s.publish(ctx, root, etag, m); err != nil {
		if !apiCode(err, "PreconditionFailed", "ConditionalRequestConflict") {
			return 0, storageError(err)
		}
		current, _, err := s.load(ctx, root)
		if err != nil {
			return 0, err
		}
		// Another completion or a retried SDK request may have already sealed
		// exactly these bytes. Any intervening append requires a fresh digest.
		if current.Sealed && current.SHA256 == want {
			return current.Size, nil
		}
		return current.Size, fmt.Errorf("%w: artifact changed during completion", application.ErrConflict)
	}
	return m.Size, nil
}
