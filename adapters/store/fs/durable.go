package storefs

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"mime"
	"os"
	"path"
	"strings"
	"time"

	"github.com/goliatone/go-export/export"
)

const durableMagic = "GOEXP001"
const durableHeaderLimit = 1 << 20

// DurableStore publishes one checksummed payload/metadata envelope by atomic
// rename. Its on-disk format differs from Store: serve through Open, never a
// static file server. Root must be private, persistent local storage supporting
// atomic rename and file/directory fsync. Existing raw-file stores are unchanged.
type DurableStore struct {
	Root string
	Now  func() time.Time
}

func NewDurableStore(root string) *DurableStore { return &DurableStore{Root: root, Now: time.Now} }

type durableHeader struct {
	Meta   export.ArtifactMeta `json:"meta"`
	SHA256 string              `json:"sha256"`
}
type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if e := r.ctx.Err(); e != nil {
		return 0, e
	}
	return r.r.Read(p)
}

func (s *DurableStore) root(key string) (*os.Root, error) {
	if s == nil || s.Root == "" {
		return nil, export.NewError(export.KindValidation, "durable store root is required", nil)
	}
	if key == "." || !fs.ValidPath(key) || strings.Contains(key, "\\") {
		return nil, export.NewError(export.KindValidation, "invalid artifact key", nil)
	}
	for part := range strings.SplitSeq(key, "/") {
		if strings.HasPrefix(part, ".export-tmp-") {
			return nil, export.NewError(export.KindValidation, "reserved artifact key", nil)
		}
	}
	if e := os.MkdirAll(s.Root, 0700); e != nil {
		return nil, e
	}
	return os.OpenRoot(s.Root)
}
func syncDir(r *os.Root, dir string) error {
	f, e := r.Open(dir)
	if e != nil {
		return e
	}
	defer f.Close()
	return f.Sync()
}
func makeDurableDirs(r *os.Root, dir string) error {
	if dir == "." {
		return nil
	}
	parent := path.Dir(dir)
	if e := makeDurableDirs(r, parent); e != nil {
		return e
	}
	if e := r.Mkdir(dir, 0700); e != nil && !os.IsExist(e) {
		return e
	}
	return syncDir(r, parent)
}
func durableTemp(r *os.Root, dir string) (*os.File, string, error) {
	name := path.Join(dir, ".export-tmp-"+rand.Text())
	f, e := r.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	return f, name, e
}

func (s *DurableStore) Put(ctx context.Context, key string, reader io.Reader, meta export.ArtifactMeta) (export.ArtifactRef, error) {
	if e := ctx.Err(); e != nil {
		return export.ArtifactRef{}, e
	}
	if reader == nil {
		return export.ArtifactRef{}, export.NewError(export.KindValidation, "artifact reader is required", nil)
	}
	r, e := s.root(key)
	if e != nil {
		return export.ArtifactRef{}, e
	}
	defer r.Close()
	dir := path.Dir(key)
	if e = makeDurableDirs(r, dir); e != nil {
		return export.ArtifactRef{}, e
	}
	payload, payloadName, e := durableTemp(r, dir)
	if e != nil {
		return export.ArtifactRef{}, e
	}
	defer func() { payload.Close(); r.Remove(payloadName) }()
	h := sha256.New()
	size, e := io.Copy(io.MultiWriter(payload, h), contextReader{ctx, reader})
	if e != nil {
		return export.ArtifactRef{}, e
	}
	meta.Size = size
	if meta.CreatedAt.IsZero() {
		if s.Now != nil {
			meta.CreatedAt = s.Now()
		} else {
			meta.CreatedAt = time.Now()
		}
	}
	if meta.ContentType == "" {
		meta.ContentType = mime.TypeByExtension(path.Ext(key))
	}
	header, e := json.Marshal(durableHeader{meta, hex.EncodeToString(h.Sum(nil))})
	if e != nil {
		return export.ArtifactRef{}, e
	}
	if len(header) > durableHeaderLimit {
		return export.ArtifactRef{}, export.NewError(export.KindValidation, "artifact metadata too large", nil)
	}
	file, name, e := durableTemp(r, dir)
	if e != nil {
		return export.ArtifactRef{}, e
	}
	defer func() { file.Close(); r.Remove(name) }()
	if _, e = file.Write([]byte(durableMagic)); e != nil {
		return export.ArtifactRef{}, e
	}
	if e = binary.Write(file, binary.BigEndian, uint64(len(header))); e != nil {
		return export.ArtifactRef{}, e
	}
	headerHash := sha256.Sum256(header)
	if _, e = file.Write(headerHash[:]); e != nil {
		return export.ArtifactRef{}, e
	}
	if _, e = file.Write(header); e != nil {
		return export.ArtifactRef{}, e
	}
	if _, e = payload.Seek(0, io.SeekStart); e != nil {
		return export.ArtifactRef{}, e
	}
	if _, e = io.Copy(file, contextReader{ctx, payload}); e != nil {
		return export.ArtifactRef{}, e
	}
	if e = file.Sync(); e != nil {
		return export.ArtifactRef{}, e
	}
	if e = file.Close(); e != nil {
		return export.ArtifactRef{}, e
	}
	if e = ctx.Err(); e != nil {
		return export.ArtifactRef{}, e
	}
	if e = r.Rename(name, key); e != nil {
		return export.ArtifactRef{}, e
	}
	if e = syncDir(r, dir); e != nil {
		return export.ArtifactRef{}, e
	}
	return export.ArtifactRef{Key: key, Meta: meta}, nil
}
func (s *DurableStore) Open(ctx context.Context, key string) (io.ReadCloser, export.ArtifactMeta, error) {
	if e := ctx.Err(); e != nil {
		return nil, export.ArtifactMeta{}, e
	}
	r, e := s.root(key)
	if e != nil {
		return nil, export.ArtifactMeta{}, e
	}
	defer r.Close()
	f, e := r.Open(key)
	if e != nil {
		return nil, export.ArtifactMeta{}, export.NewError(export.KindNotFound, "artifact unavailable", e)
	}
	fail := func(e error) (io.ReadCloser, export.ArtifactMeta, error) {
		f.Close()
		return nil, export.ArtifactMeta{}, export.NewError(export.KindInternal, "invalid durable artifact", e)
	}
	magic := make([]byte, len(durableMagic))
	if _, e = io.ReadFull(f, magic); e != nil {
		return fail(e)
	}
	if string(magic) != durableMagic {
		return fail(fmt.Errorf("invalid format"))
	}
	var length uint64
	if e = binary.Read(f, binary.BigEndian, &length); e != nil {
		return fail(e)
	}
	if length > durableHeaderLimit {
		return fail(fmt.Errorf("invalid header length"))
	}
	var expectedHeader [sha256.Size]byte
	if _, e = io.ReadFull(f, expectedHeader[:]); e != nil {
		return fail(e)
	}
	b := make([]byte, int(length))
	if _, e = io.ReadFull(f, b); e != nil {
		return fail(e)
	}
	if sha256.Sum256(b) != expectedHeader {
		return fail(fmt.Errorf("metadata checksum mismatch"))
	}
	var header durableHeader
	if e = json.Unmarshal(b, &header); e != nil {
		return fail(e)
	}
	start, e := f.Seek(0, io.SeekCurrent)
	if e != nil {
		return fail(e)
	}
	stat, e := f.Stat()
	if e != nil {
		return fail(e)
	}
	if header.Meta.Size < 0 || stat.Size()-start != header.Meta.Size {
		return fail(fmt.Errorf("payload length mismatch"))
	}
	h := sha256.New()
	if _, e = io.Copy(h, contextReader{ctx, f}); e != nil {
		return fail(e)
	}
	if hex.EncodeToString(h.Sum(nil)) != header.SHA256 {
		return fail(fmt.Errorf("checksum mismatch"))
	}
	if _, e = f.Seek(start, io.SeekStart); e != nil {
		return fail(e)
	}
	return f, header.Meta, nil
}
func (s *DurableStore) Delete(ctx context.Context, key string) error {
	if e := ctx.Err(); e != nil {
		return e
	}
	r, e := s.root(key)
	if e != nil {
		return e
	}
	defer r.Close()
	if e = r.Remove(key); os.IsNotExist(e) {
		return nil
	} else if e != nil {
		return e
	}
	return syncDir(r, path.Dir(key))
}
func (s *DurableStore) SignedURL(context.Context, string, time.Duration) (string, error) {
	return "", export.NewError(export.KindNotImpl, "durable artifacts require guarded download", nil)
}
