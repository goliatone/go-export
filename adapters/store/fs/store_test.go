package storefs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/goliatone/go-export/export"
)

type captureSigner struct {
	input SignedURLInput
}

func (s *captureSigner) SignURL(input SignedURLInput) (string, error) {
	s.input = input
	return fmt.Sprintf("%s/%s?expires=%d", input.BaseURL, input.Key, input.ExpiresAt.Unix()), nil
}

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

func TestStore_SignedURL(t *testing.T) {
	store := NewStore(t.TempDir())
	store.BaseURL = "https://example.test/exports"
	signer := &captureSigner{}
	store.Signer = signer
	store.Now = func() time.Time {
		return time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	}

	url, err := store.SignedURL(context.Background(), "exports/test.csv", 5*time.Minute)
	if err != nil {
		t.Fatalf("signed url: %v", err)
	}
	expected := "https://example.test/exports/exports/test.csv?expires=1704110700"
	if url != expected {
		t.Fatalf("unexpected url: %q", url)
	}
	if signer.input.Key != "exports/test.csv" {
		t.Fatalf("unexpected signer key: %q", signer.input.Key)
	}
}
