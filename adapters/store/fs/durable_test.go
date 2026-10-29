package storefs

import (
	"bytes"
	"context"
	"errors"
	"github.com/goliatone/go-export/export"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, errors.New("interrupted writer") }
func durableRead(t *testing.T, s *DurableStore, key string) (string, export.ArtifactMeta) {
	t.Helper()
	r, m, e := s.Open(context.Background(), key)
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	b, e := io.ReadAll(r)
	if e != nil {
		t.Fatal(e)
	}
	return string(b), m
}
func TestDurableStoreReopenAndFailureIsolation(t *testing.T) {
	root := t.TempDir()
	s := NewDurableStore(root)
	ctx := context.Background()
	_, e := s.Put(ctx, "exports/a.csv", strings.NewReader("acknowledged"), export.ArtifactMeta{Filename: "old.csv"})
	if e != nil {
		t.Fatal(e)
	}
	for _, mode := range []string{"read-failure", "invalid-metadata", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			c := ctx
			reader := io.Reader(strings.NewReader("replacement"))
			m := export.ArtifactMeta{Filename: "new.csv"}
			switch mode {
			case "read-failure":
				reader = failingReader{}
			case "invalid-metadata":
				m.CreatedAt = m.CreatedAt.AddDate(10000, 0, 0)
			case "canceled":
				var cancel context.CancelFunc
				c, cancel = context.WithCancel(ctx)
				cancel()
			}
			if _, e := s.Put(c, "exports/a.csv", reader, m); e == nil {
				t.Fatal("failed publication reported success")
			}
			body, meta := durableRead(t, NewDurableStore(root), "exports/a.csv")
			if body != "acknowledged" || meta.Filename != "old.csv" || meta.Size != 12 {
				t.Fatalf("torn publication: %q %+v", body, meta)
			}
		})
	}
	if e := s.Delete(ctx, "exports/a.csv"); e != nil {
		t.Fatal(e)
	}
	if _, _, e := NewDurableStore(root).Open(ctx, "exports/a.csv"); e == nil {
		t.Fatal("deleted artifact reappeared")
	}
}
func TestDurableStoreRejectsCorruptionAndEscapes(t *testing.T) {
	for _, mode := range []string{"payload", "metadata", "truncated"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			s := NewDurableStore(root)
			_, e := s.Put(context.Background(), "a", strings.NewReader("payload"), export.ArtifactMeta{Filename: "safe.csv"})
			if e != nil {
				t.Fatal(e)
			}
			p := filepath.Join(root, "a")
			b, e := os.ReadFile(p)
			if e != nil {
				t.Fatal(e)
			}
			switch mode {
			case "payload":
				b[len(b)-1] ^= 1
			case "metadata":
				i := bytes.Index(b, []byte("safe.csv"))
				if i < 0 {
					t.Fatal("metadata missing")
				}
				b[i] = 'x'
			case "truncated":
				b = b[:len(b)-1]
			}
			if e = os.WriteFile(p, b, 0600); e != nil {
				t.Fatal(e)
			}
			if r, _, e := s.Open(context.Background(), "a"); e == nil {
				r.Close()
				t.Fatal("corrupt artifact exposed")
			}
		})
	}
	s := NewDurableStore(t.TempDir())
	outside := t.TempDir()
	if e := os.Symlink(outside, filepath.Join(s.Root, "escape")); e != nil {
		t.Fatal(e)
	}
	for _, key := range []string{"../outside", "a/../b", "/absolute", ".", ".export-tmp-secret", "escape/outside"} {
		if _, e := s.Put(context.Background(), key, strings.NewReader("private"), export.ArtifactMeta{}); e == nil {
			t.Fatalf("invalid/root escape accepted: %s", key)
		}
	}
	entries, e := os.ReadDir(outside)
	if e != nil {
		t.Fatal(e)
	}
	if len(entries) != 0 {
		t.Fatal("write escaped root")
	}
}
func TestDurableStoreConcurrentReadersSeeMatchingMetadata(t *testing.T) {
	s := NewDurableStore(t.TempDir())
	ctx := context.Background()
	_, e := s.Put(ctx, "a", strings.NewReader("first"), export.ArtifactMeta{Filename: "first"})
	if e != nil {
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 24)
	for range 12 {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, e := s.Put(ctx, "a", strings.NewReader("second"), export.ArtifactMeta{Filename: "second"})
			errs <- e
		}()
		go func() {
			defer wg.Done()
			r, m, e := s.Open(ctx, "a")
			if e == nil {
				b, er := io.ReadAll(r)
				r.Close()
				if er != nil {
					e = er
				} else if string(b) != m.Filename {
					e = errors.New("payload/metadata generation mismatch")
				}
			}
			errs <- e
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
}
