// Package objectstore implements application.ObjectStore on a filesystem
// (a PersistentVolume in Kubernetes). See ADR 0007 for why S3 is not here.
package objectstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/lgoyal6/gpu-broker/controlplane/internal/application"
)

// FS stores each object as one file under Root. Appends are serialised per
// process; across processes the offset check makes a racing append fail
// rather than interleave, because the second writer's offset no longer
// matches the file size.
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

func (f *FS) path(key string) (string, error) {
	clean := filepath.Clean("/" + key)
	if strings.Contains(key, "..") || clean == "/" {
		return "", fmt.Errorf("%w: bad object key %q", application.ErrInvalid, key)
	}
	return filepath.Join(f.Root, clean), nil
}

func (f *FS) Append(_ context.Context, key string, offset int64, data []byte) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.FailNext > 0 {
		f.FailNext--
		return 0, fmt.Errorf("%w: %v", application.ErrUnavailable, errUnavailable)
	}
	p, err := f.path(key)
	if err != nil {
		return 0, err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
		return 0, err
	}
	fh, err := os.OpenFile(p, os.O_CREATE|os.O_RDWR, 0o640)
	if err != nil {
		return 0, err
	}
	defer fh.Close()
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

func (f *FS) Read(_ context.Context, key string, offset int64, max int) ([]byte, error) {
	p, err := f.path(key)
	if err != nil {
		return nil, err
	}
	fh, err := os.Open(p)
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

func (f *FS) Digest(_ context.Context, key string) (string, error) {
	p, err := f.path(key)
	if err != nil {
		return "", err
	}
	fh, err := os.Open(p)
	if err != nil {
		return "", err
	}
	defer fh.Close()
	h := sha256.New()
	if _, err := io.Copy(h, fh); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
