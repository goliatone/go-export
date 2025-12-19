package storefs

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/goliatone/go-export/export"
)

func TestStore_PutOpenDelete(t *testing.T) {
	root := t.TempDir()
	store := NewStore(root)

	ref, err := store.Put(context.Background(), "exports/test.csv", bytes.NewBufferString("hello"), export.ArtifactMeta{
		ContentType: "text/csv",
		Filename:    "test.csv",
	})
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if ref.Meta.Size != 5 {
		t.Fatalf("expected size 5, got %d", ref.Meta.Size)
	}
	if ref.Meta.CreatedAt.IsZero() {
		t.Fatalf("expected created_at set")
	}

	reader, meta, err := store.Open(context.Background(), "exports/test.csv")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	data, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(data) != "hello" {
		t.Fatalf("expected payload, got %q", string(data))
	}
	if meta.Filename != "test.csv" {
		t.Fatalf("expected filename, got %q", meta.Filename)
	}

	if err := store.Delete(context.Background(), "exports/test.csv"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, _, err := store.Open(context.Background(), "exports/test.csv"); err == nil {
		t.Fatalf("expected not found after delete")
	}
}

func TestStore_SignedURL_NotConfigured(t *testing.T) {
	store := NewStore(t.TempDir())
	_, err := store.SignedURL(context.Background(), "exports/test.csv", time.Minute)
	if err == nil {
		t.Fatalf("expected signed URL error")
	}
	if exportErr, ok := err.(*export.ExportError); !ok || exportErr.Kind != export.KindNotImpl {
		t.Fatalf("expected not implemented error, got %v", err)
	}
}
