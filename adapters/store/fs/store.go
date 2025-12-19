package storefs

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/goliatone/go-export/export"
)

// SignedURLInput describes a signed URL request.
type SignedURLInput struct {
	BaseURL   string
	Key       string
	ExpiresAt time.Time
}

// SignedURLSigner signs artifact URLs.
type SignedURLSigner interface {
	SignURL(input SignedURLInput) (string, error)
}

// Store provides filesystem-backed artifact storage.
type Store struct {
	Root    string
	BaseURL string
	Signer  SignedURLSigner
	Now     func() time.Time
}

// NewStore creates a filesystem-backed artifact store.
func NewStore(root string) *Store {
	return &Store{Root: root, Now: time.Now}
}

// Put stores an artifact on disk.
func (s *Store) Put(ctx context.Context, key string, r io.Reader, meta export.ArtifactMeta) (export.ArtifactRef, error) {
	_ = ctx
	if s == nil {
		return export.ArtifactRef{}, export.NewError(export.KindInternal, "store is nil", nil)
	}
	if s.Root == "" {
		return export.ArtifactRef{}, export.NewError(export.KindValidation, "store root is required", nil)
	}
	if key == "" {
		return export.ArtifactRef{}, export.NewError(export.KindValidation, "artifact key is required", nil)
	}

	pathOnDisk, err := s.resolvePath(key)
	if err != nil {
		return export.ArtifactRef{}, err
	}

	dir := filepath.Dir(pathOnDisk)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return export.ArtifactRef{}, err
	}

	tmp, err := os.CreateTemp(dir, ".export-*")
	if err != nil {
		return export.ArtifactRef{}, err
	}
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
	}()

	size, err := io.Copy(tmp, r)
	if err != nil {
		return export.ArtifactRef{}, err
	}
	if err := tmp.Sync(); err != nil {
		return export.ArtifactRef{}, err
	}
	if err := tmp.Close(); err != nil {
		return export.ArtifactRef{}, err
	}

	if err := os.Rename(tmp.Name(), pathOnDisk); err != nil {
		return export.ArtifactRef{}, err
	}

	meta.Size = size
	if meta.CreatedAt.IsZero() {
		meta.CreatedAt = s.now()
	}
	if meta.ContentType == "" {
		meta.ContentType = mime.TypeByExtension(filepath.Ext(pathOnDisk))
	}

	if err := s.writeMeta(pathOnDisk, meta); err != nil {
		return export.ArtifactRef{}, err
	}

	return export.ArtifactRef{Key: key, Meta: meta}, nil
}

// Open reads an artifact from disk.
func (s *Store) Open(ctx context.Context, key string) (io.ReadCloser, export.ArtifactMeta, error) {
	_ = ctx
	if s == nil {
		return nil, export.ArtifactMeta{}, export.NewError(export.KindInternal, "store is nil", nil)
	}
	if s.Root == "" {
		return nil, export.ArtifactMeta{}, export.NewError(export.KindValidation, "store root is required", nil)
	}
	if key == "" {
		return nil, export.ArtifactMeta{}, export.NewError(export.KindValidation, "artifact key is required", nil)
	}

	pathOnDisk, err := s.resolvePath(key)
	if err != nil {
		return nil, export.ArtifactMeta{}, err
	}

	file, err := os.Open(pathOnDisk)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, export.ArtifactMeta{}, export.NewError(export.KindNotFound, fmt.Sprintf("artifact %q not found", key), err)
		}
		return nil, export.ArtifactMeta{}, err
	}

	meta := s.readMeta(pathOnDisk)
	if meta.ContentType == "" {
		meta.ContentType = mime.TypeByExtension(filepath.Ext(pathOnDisk))
	}
	if meta.Size == 0 {
		if info, err := file.Stat(); err == nil {
			meta.Size = info.Size()
			if meta.CreatedAt.IsZero() {
				meta.CreatedAt = info.ModTime()
			}
		}
	}

	return file, meta, nil
}

// Delete removes an artifact from disk.
func (s *Store) Delete(ctx context.Context, key string) error {
	_ = ctx
	if s == nil {
		return export.NewError(export.KindInternal, "store is nil", nil)
	}
	if s.Root == "" {
		return export.NewError(export.KindValidation, "store root is required", nil)
	}
	if key == "" {
		return export.NewError(export.KindValidation, "artifact key is required", nil)
	}

	pathOnDisk, err := s.resolvePath(key)
	if err != nil {
		return err
	}
	_ = os.Remove(pathOnDisk)
	_ = os.Remove(metaPath(pathOnDisk))
	return nil
}

// SignedURL generates a signed URL when configured.
func (s *Store) SignedURL(ctx context.Context, key string, ttl time.Duration) (string, error) {
	_ = ctx
	if s == nil {
		return "", export.NewError(export.KindInternal, "store is nil", nil)
	}
	if s.Signer == nil || s.BaseURL == "" {
		return "", export.NewError(export.KindNotImpl, "signed URLs not configured", nil)
	}
	if ttl <= 0 {
		return "", export.NewError(export.KindValidation, "signed URL TTL is required", nil)
	}
	if key == "" {
		return "", export.NewError(export.KindValidation, "artifact key is required", nil)
	}
	expires := s.now().Add(ttl)
	return s.Signer.SignURL(SignedURLInput{
		BaseURL:   strings.TrimRight(s.BaseURL, "/"),
		Key:       key,
		ExpiresAt: expires,
	})
}

func (s *Store) resolvePath(key string) (string, error) {
	clean := path.Clean("/" + key)
	rel := strings.TrimPrefix(clean, "/")
	if rel == "" || rel == "." {
		return "", export.NewError(export.KindValidation, "invalid artifact key", nil)
	}

	root, err := filepath.Abs(s.Root)
	if err != nil {
		return "", err
	}
	target := filepath.Join(root, filepath.FromSlash(rel))
	if !strings.HasPrefix(target, root+string(os.PathSeparator)) && target != root {
		return "", export.NewError(export.KindValidation, "artifact key escapes root", nil)
	}
	return target, nil
}

func (s *Store) writeMeta(pathOnDisk string, meta export.ArtifactMeta) error {
	payload, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	dir := filepath.Dir(pathOnDisk)
	tmp, err := os.CreateTemp(dir, ".meta-*")
	if err != nil {
		return err
	}
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
	}()
	if _, err := tmp.Write(payload); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), metaPath(pathOnDisk))
}

func (s *Store) readMeta(pathOnDisk string) export.ArtifactMeta {
	data, err := os.ReadFile(metaPath(pathOnDisk))
	if err != nil {
		return export.ArtifactMeta{}
	}
	var meta export.ArtifactMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return export.ArtifactMeta{}
	}
	return meta
}

func (s *Store) now() time.Time {
	if s.Now == nil {
		return time.Now()
	}
	return s.Now()
}

func metaPath(pathOnDisk string) string {
	return pathOnDisk + ".meta.json"
}
