package exporthttp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/goliatone/go-export/adapters/exportapi"
	storefs "github.com/goliatone/go-export/adapters/store/fs"
	"github.com/goliatone/go-export/export"
)

type stubSigner struct{}

func (stubSigner) SignURL(input storefs.SignedURLInput) (string, error) {
	return fmt.Sprintf("%s/%s?expires=%d", input.BaseURL, input.Key, input.ExpiresAt.Unix()), nil
}

func TestHandler_SignedURLRedirect(t *testing.T) {
	store := storefs.NewStore(t.TempDir())
	store.BaseURL = "https://example.test/exports"
	store.Signer = stubSigner{}

	tracker := export.NewMemoryTracker()
	svc := export.NewService(export.ServiceConfig{
		Tracker: tracker,
		Store:   store,
	})

	ref, err := store.Put(context.Background(), "exports/exp-1.csv", bytes.NewBufferString("id,name\n1,alice\n"), export.ArtifactMeta{
		Filename:    "users.csv",
		ContentType: "text/csv",
	})
	if err != nil {
		t.Fatalf("store put: %v", err)
	}
	if _, err := tracker.Start(context.Background(), export.ExportRecord{
		ID:         "exp-1",
		Definition: "users",
		Format:     export.FormatCSV,
		State:      export.StateCompleted,
		Artifact:   ref,
	}); err != nil {
		t.Fatalf("tracker start: %v", err)
	}

	handler := NewHandler(Config{
		Service:       svc,
		Store:         store,
		SignedURLTTL:  time.Minute,
		ActorProvider: StaticActorProvider{Actor: export.Actor{ID: "user-1"}},
	})

	req := httptest.NewRequest(http.MethodGet, "/admin/exports/exp-1/download", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusFound {
		t.Fatalf("expected 302, got %d", rec.Code)
	}
	location := rec.Header().Get("Location")
	if location == "" || !strings.HasPrefix(location, store.BaseURL) {
		t.Fatalf("expected redirect to signed URL, got %q", location)
	}
}

func TestHandler_AsyncRequiresService(t *testing.T) {
	runner := newTestRunner(t)
	handler := NewHandler(Config{
		Runner:        runner,
		ActorProvider: StaticActorProvider{Actor: export.Actor{ID: "user-1"}},
	})

	body := `{"definition":"users","format":"csv","delivery":"async"}`
	req := httptest.NewRequest(http.MethodPost, "/admin/exports", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("expected 501, got %d", rec.Code)
	}
	var payload exportapi.ErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if payload.Error.Code != "not_implemented" {
		t.Fatalf("expected not_implemented code, got %q", payload.Error.Code)
	}
}
