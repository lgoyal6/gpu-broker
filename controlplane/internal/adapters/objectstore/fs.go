// Package objectstore implements resumable, sealed artifact storage.
package objectstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/application"
)

// FS stores each object as one file. File locks serialize appends and seals
// across API processes sharing the volume, not just within one instance.
type FS struct {
	Root string
	mu   sync.Mutex
	// FailNext, when set by a fault test, makes the next n Appends fail as
	// if the volume were unavailable.
	FailNext int
}

func NewFS(root string) (*FS, error) {
	if err := os.MkdirAll(root, 0o750); err != nil {
		return nil, err
	}
	return &FS{Root: root}, nil
}

var errUnavailable = errors.New("object store unavailable")

const maxChunkBytes = 8 << 20

func hashBytes(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func (f *FS) path(key string) (string, error) {
	clean := filepath.Clean("/" + key)
	if strings.Contains(key, "..") || clean == "/" || strings.HasPrefix(strings.TrimPrefix(key, "/"), ".gpub-") {
		return "", fmt.Errorf("%w: bad object key %q", application.ErrInvalid, key)
	}
	return filepath.Join(f.Root, clean), nil
}

func (f *FS) Append(ctx context.Context, key string, offset int64, data []byte) (int64, error) {
	f.mu.Lock()
	if f.FailNext > 0 {
		f.FailNext--
		f.mu.Unlock()
		return 0, fmt.Errorf("%w: %v", application.ErrUnavailable, errUnavailable)
	}
	f.mu.Unlock()
	if offset < 0 || len(data) > maxChunkBytes {
		return 0, application.ErrInvalid
	}
	p, err := f.path(key)
	if err != nil {
		return 0, err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
		return 0, err
	}
	fh, err := lockedFile(ctx, p, os.O_CREATE|os.O_RDWR)
	if err != nil {
		return 0, err
	}
	defer fh.Close()
	if _, err := os.Stat(f.sealPath(key)); err == nil {
		return 0, fmt.Errorf("%w: object is sealed", application.ErrConflict)
	} else if !errors.Is(err, os.ErrNotExist) {
		return 0, err
	}
	st, err := fh.Stat()
	if err != nil {
		return 0, err
	}
	if st.Size() != offset {
		return st.Size(), &application.OffsetMismatchError{Committed: st.Size()}
	}
	if _, err := fh.WriteAt(data, offset); err != nil {
		return offset, err
	}
	// fsync before acknowledging: the agent discards its local copy of a
	// chunk once it sees the new offset.
	if err := fh.Sync(); err != nil {
		return offset, err
	}
	return offset + int64(len(data)), nil
}

func (f *FS) Size(_ context.Context, key string) (int64, error) {
	p, err := f.path(key)
	if err != nil {
		return 0, err
	}
	st, err := os.Stat(p)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return st.Size(), nil
}

func (f *FS) Read(ctx context.Context, key string, offset int64, max int) ([]byte, error) {
	if offset < 0 || max < 0 || max > maxChunkBytes {
		return nil, application.ErrInvalid
	}
	p, err := f.path(key)
	if err != nil {
		return nil, err
	}
	fh, err := lockedFile(ctx, p, os.O_RDONLY)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, application.ErrNotFound
		}
		return nil, err
	}
	defer fh.Close()
	buf := make([]byte, max)
	n, err := fh.ReadAt(buf, offset)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return buf[:n], nil
}

func (f *FS) Digest(ctx context.Context, key string) (string, error) {
	p, err := f.path(key)
	if err != nil {
		return "", err
	}
	fh, err := lockedFile(ctx, p, os.O_RDONLY)
	if err != nil {
		return "", err
	}
	defer fh.Close()
	return fileDigest(fh)
}

func fileDigest(fh *os.File) (string, error) {
	h := sha256.New()
	if _, err := io.Copy(h, fh); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

type fileSeal struct {
	Size   int64
	SHA256 string
}

func (f *FS) sealPath(key string) string {
	digest := sha256.Sum256([]byte(key))
	return filepath.Join(f.Root, ".gpub-seals", hex.EncodeToString(digest[:]))
}

func (f *FS) Seal(ctx context.Context, key, want string) (int64, error) {
	p, err := f.path(key)
	if err != nil {
		return 0, err
	}
	fh, err := lockedFile(ctx, p, os.O_RDONLY)
	if errors.Is(err, os.ErrNotExist) {
		return 0, application.ErrNotFound
	}
	if err != nil {
		return 0, err
	}
	defer fh.Close()
	sealPath := f.sealPath(key)
	if raw, err := os.ReadFile(sealPath); err == nil {
		var seal fileSeal
		if err := json.Unmarshal(raw, &seal); err != nil {
			return 0, fmt.Errorf("%w: invalid seal", application.ErrUnavailable)
		}
		digest, err := hex.DecodeString(seal.SHA256)
		st, statErr := fh.Stat()
		if err != nil || len(digest) != sha256.Size || seal.Size < 0 || statErr != nil || st.Size() != seal.Size {
			return 0, fmt.Errorf("%w: invalid seal", application.ErrUnavailable)
		}
		if seal.SHA256 != want {
			return 0, application.ErrConflict
		}
		return seal.Size, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return 0, err
	}
	got, err := fileDigest(fh)
	if err != nil {
		return 0, err
	}
	if got != want {
		return 0, fmt.Errorf("%w: artifact digest mismatch", application.ErrConflict)
	}
	st, err := fh.Stat()
	if err != nil {
		return 0, err
	}
	dir := filepath.Dir(sealPath)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return 0, err
	}
	tmp, err := os.CreateTemp(dir, ".seal-")
	if err != nil {
		return 0, err
	}
	defer os.Remove(tmp.Name())
	defer tmp.Close()
	raw, err := json.Marshal(fileSeal{Size: st.Size(), SHA256: got})
	if err != nil {
		return 0, err
	}
	if err := tmp.Chmod(0o640); err != nil {
		return 0, err
	}
	if _, err := tmp.Write(raw); err != nil {
		return 0, err
	}
	if err := tmp.Sync(); err != nil {
		return 0, err
	}
	if err := tmp.Close(); err != nil {
		return 0, err
	}
	if err := os.Rename(tmp.Name(), sealPath); err != nil {
		return 0, err
	}
	// Persist the directory entry before acknowledging a seal. A restarted
	// API must not accept a new append after completion was acknowledged.
	dh, err := os.Open(dir)
	if err != nil {
		return 0, err
	}
	defer dh.Close()
	if err := dh.Sync(); err != nil {
		return 0, err
	}
	return st.Size(), nil
}

func lockedFile(ctx context.Context, path string, flags int) (*os.File, error) {
	fh, err := os.OpenFile(path, flags, 0o640)
	if err != nil {
		return nil, err
	}
	for {
		if err := ctx.Err(); err != nil {
			fh.Close()
			return nil, err
		}
		err := unix.Flock(int(fh.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			return fh, nil
		}
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN) {
			fh.Close()
			return nil, err
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			fh.Close()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}
