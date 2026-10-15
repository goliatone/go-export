package storefs

import (
	"bytes"
	"context"
	"github.com/goliatone/go-export/export"
	"io"
	"testing"
)

func TestLegacyStoreFailedMetadataDoesNotReplaceArtifact(t *testing.T) {
	s := NewStore(t.TempDir())
	ctx := context.Background()
	_, e := s.Put(ctx, "a.csv", bytes.NewBufferString("old"), export.ArtifactMeta{Filename: "old.csv"})
	if e != nil {
		t.Fatal(e)
	}
	// Invalid metadata is rejected before replacing acknowledged bytes.
	// Force metadata serialization failure using an invalid time outside RFC3339.
	m := export.ArtifactMeta{Filename: "new.csv"}
	m.CreatedAt = m.CreatedAt.AddDate(10000, 0, 0)
	_, e = s.Put(ctx, "a.csv", bytes.NewBufferString("new"), m)
	if e == nil {
		t.Fatal("invalid metadata accepted")
	}
	r, meta, e := s.Open(ctx, "a.csv")
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	b, _ := io.ReadAll(r)
	if string(b) != "old" || meta.Filename != "old.csv" {
		t.Fatalf("failed Put replaced acknowledged artifact: %q %+v", b, meta)
	}
}
