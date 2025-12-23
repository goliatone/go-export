package exporthttp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/goliatone/go-export/export"
)

type stubSource struct {
	rows []export.Row
}

func (s *stubSource) Open(ctx context.Context, spec export.RowSourceSpec) (export.RowIterator, error) {
	_ = ctx
	_ = spec
	return &stubIterator{rows: s.rows}, nil
}

type stubIterator struct {
	rows []export.Row
	idx  int
}

func (it *stubIterator) Next(ctx context.Context) (export.Row, error) {
	_ = ctx
	if it.idx >= len(it.rows) {
		return nil, io.EOF
	}
	row := it.rows[it.idx]
	it.idx++
	return row, nil
}

func (it *stubIterator) Close() error { return nil }

type denyDownloadGuard struct{}

func (denyDownloadGuard) AuthorizeExport(ctx context.Context, actor export.Actor, req export.ExportRequest, def export.ResolvedDefinition) error {
	_ = ctx
	_ = actor
	_ = req
	_ = def
	return nil
}

func (denyDownloadGuard) AuthorizeDownload(ctx context.Context, actor export.Actor, exportID string) error {
	_ = ctx
	_ = actor
	_ = exportID
	return errors.New("denied")
}

type captureGuard struct {
	called     bool
	definition string
	resource   string
}

func (g *captureGuard) AuthorizeExport(ctx context.Context, actor export.Actor, req export.ExportRequest, def export.ResolvedDefinition) error {
	_ = ctx
	_ = actor
	_ = req
	g.called = true
	g.definition = def.Name
	g.resource = def.Resource
	return nil
}

func (g *captureGuard) AuthorizeDownload(ctx context.Context, actor export.Actor, exportID string) error {
	_ = ctx
	_ = actor
	_ = exportID
	return nil
}

func newTestRunner(t *testing.T) *export.Runner {
	t.Helper()
	runner := export.NewRunner()
	if err := runner.Definitions.Register(export.ExportDefinition{
		Name:         "users",
		Resource:     "users",
		RowSourceKey: "stub",
		Schema: export.Schema{Columns: []export.Column{
			{Name: "id"},
			{Name: "name"},
		}},
	}); err != nil {
		t.Fatalf("register definition: %v", err)
	}
	if err := runner.RowSources.Register("stub", func(req export.ExportRequest, def export.ResolvedDefinition) (export.RowSource, error) {
		_ = req
		_ = def
		return &stubSource{rows: []export.Row{{"1", "alice"}}}, nil
	}); err != nil {
		t.Fatalf("register source: %v", err)
	}
	return runner
}

func TestHandler_SyncExport(t *testing.T) {
	runner := newTestRunner(t)
	handler := NewHandler(Config{
		Runner:        runner,
		ActorProvider: StaticActorProvider{Actor: export.Actor{ID: "user-1"}},
	})

	body := `{"definition":"users","format":"csv","delivery":"sync"}`
	req := httptest.NewRequest(http.MethodPost, "/admin/exports", strings.NewReader(body))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if rec.Header().Get("X-Export-Id") == "" {
		t.Fatalf("expected X-Export-Id header")
	}
	if !strings.Contains(rec.Header().Get("Content-Disposition"), "attachment") {
		t.Fatalf("expected Content-Disposition attachment")
	}
	if !strings.Contains(rec.Body.String(), "id,name") {
		t.Fatalf("expected csv headers, got %q", rec.Body.String())
	}
}

func TestHandler_AsyncIdempotencyAndDownload(t *testing.T) {
	runner := newTestRunner(t)
	tracker := export.NewMemoryTracker()
	store := export.NewMemoryStore()
	svc := export.NewService(export.ServiceConfig{
		Runner:  runner,
		Tracker: tracker,
		Store:   store,
		DeliveryPolicy: export.DeliveryPolicy{
			Default: export.DeliveryAsync,
		},
	})

	idempotency := NewMemoryIdempotencyStore()
	handler := NewHandler(Config{
		Service:          svc,
		Runner:           runner,
		Store:            store,
		ActorProvider:    StaticActorProvider{Actor: export.Actor{ID: "user-1"}},
		IdempotencyStore: idempotency,
	})

	body := `{"definition":"users","format":"csv","delivery":"async"}`
	req := httptest.NewRequest(http.MethodPost, "/admin/exports", strings.NewReader(body))
	req.Header.Set("Idempotency-Key", "abc123")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", rec.Code)
	}
	var first asyncResponse
	if err := json.NewDecoder(bytes.NewReader(rec.Body.Bytes())).Decode(&first); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if first.ID == "" {
		t.Fatalf("expected export id")
	}

	req2 := httptest.NewRequest(http.MethodPost, "/admin/exports", strings.NewReader(body))
	req2.Header.Set("Idempotency-Key", "abc123")
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)

	var second asyncResponse
	if err := json.NewDecoder(bytes.NewReader(rec2.Body.Bytes())).Decode(&second); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if second.ID != first.ID {
		t.Fatalf("expected same export id, got %s vs %s", second.ID, first.ID)
	}

	_, err := svc.GenerateExport(context.Background(), export.Actor{ID: "user-1"}, first.ID, export.ExportRequest{
		Definition: "users",
		Format:     export.FormatCSV,
	})
	if err != nil {
		t.Fatalf("generate export: %v", err)
	}

	downloadReq := httptest.NewRequest(http.MethodGet, "/admin/exports/"+first.ID+"/download", nil)
	downloadRec := httptest.NewRecorder()
	handler.ServeHTTP(downloadRec, downloadReq)

	if downloadRec.Code != http.StatusOK {
		t.Fatalf("expected download 200, got %d", downloadRec.Code)
	}
	if !strings.Contains(downloadRec.Header().Get("Content-Disposition"), "attachment") {
		t.Fatalf("expected Content-Disposition attachment")
	}
	if !strings.Contains(downloadRec.Body.String(), "id,name") {
		t.Fatalf("expected csv content, got %q", downloadRec.Body.String())
	}
}

func TestHandler_DownloadGuardRejects(t *testing.T) {
	runner := newTestRunner(t)
	tracker := export.NewMemoryTracker()
	store := export.NewMemoryStore()
	svc := export.NewService(export.ServiceConfig{
		Runner:  runner,
		Tracker: tracker,
		Store:   store,
		Guard:   denyDownloadGuard{},
	})

	ref, err := store.Put(context.Background(), "exports/exp-guard.csv", bytes.NewBufferString("id,name\n1,alice\n"), export.ArtifactMeta{
		Filename:    "users.csv",
		ContentType: "text/csv",
	})
	if err != nil {
		t.Fatalf("store put: %v", err)
	}
	if _, err := tracker.Start(context.Background(), export.ExportRecord{
		ID:         "exp-guard",
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
		ActorProvider: StaticActorProvider{Actor: export.Actor{ID: "user-1"}},
	})

	req := httptest.NewRequest(http.MethodGet, "/admin/exports/exp-guard/download", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", rec.Code)
	}
}

func TestHandler_GetExportUsesQueryDecoder(t *testing.T) {
	runner := newTestRunner(t)
	handler := NewHandler(Config{
		Runner:        runner,
		ActorProvider: StaticActorProvider{Actor: export.Actor{ID: "user-1"}},
	})

	req := httptest.NewRequest(http.MethodGet, "/admin/exports?definition=users&format=csv", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "id,name") {
		t.Fatalf("expected csv content, got %q", rec.Body.String())
	}
}

func TestHandler_HistoryRoute(t *testing.T) {
	runner := newTestRunner(t)
	tracker := export.NewMemoryTracker()
	store := export.NewMemoryStore()
	svc := export.NewService(export.ServiceConfig{
		Runner:  runner,
		Tracker: tracker,
		Store:   store,
	})

	if _, err := tracker.Start(context.Background(), export.ExportRecord{
		ID:         "exp-history",
		Definition: "users",
		Format:     export.FormatCSV,
		State:      export.StateCompleted,
	}); err != nil {
		t.Fatalf("tracker start: %v", err)
	}

	handler := NewHandler(Config{
		Service:       svc,
		Runner:        runner,
		ActorProvider: StaticActorProvider{Actor: export.Actor{ID: "user-1"}},
	})

	req := httptest.NewRequest(http.MethodGet, "/admin/exports/history", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "exp-history") {
		t.Fatalf("expected history payload, got %q", rec.Body.String())
	}
}

func TestHandler_CustomHistoryRoute(t *testing.T) {
	runner := newTestRunner(t)
	tracker := export.NewMemoryTracker()
	store := export.NewMemoryStore()
	svc := export.NewService(export.ServiceConfig{
		Runner:  runner,
		Tracker: tracker,
		Store:   store,
	})

	if _, err := tracker.Start(context.Background(), export.ExportRecord{
		ID:         "exp-history-custom",
		Definition: "users",
		Format:     export.FormatCSV,
		State:      export.StateCompleted,
	}); err != nil {
		t.Fatalf("tracker start: %v", err)
	}

	handler := NewHandler(Config{
		Service:       svc,
		Runner:        runner,
		HistoryPath:   "/admin/exports/archive",
		ActorProvider: StaticActorProvider{Actor: export.Actor{ID: "user-1"}},
	})

	req := httptest.NewRequest(http.MethodGet, "/admin/exports/archive", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "exp-history-custom") {
		t.Fatalf("expected history payload, got %q", rec.Body.String())
	}
}

func TestHandler_ResourceResolver(t *testing.T) {
	runner := newTestRunner(t)
	guard := &captureGuard{}
	handler := NewHandler(Config{
		Runner:        runner,
		Guard:         guard,
		ActorProvider: StaticActorProvider{Actor: export.Actor{ID: "user-1"}},
	})

	body := `{"resource":"users","format":"csv","delivery":"sync"}`
	req := httptest.NewRequest(http.MethodPost, "/admin/exports", strings.NewReader(body))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !guard.called {
		t.Fatalf("expected guard to be called")
	}
	if guard.definition != "users" {
		t.Fatalf("expected resolved definition users, got %q", guard.definition)
	}
	if guard.resource != "users" {
		t.Fatalf("expected resolved resource users, got %q", guard.resource)
	}
}

func TestHandler_ResourceResolverMissing(t *testing.T) {
	runner := newTestRunner(t)
	handler := NewHandler(Config{
		Runner:        runner,
		ActorProvider: StaticActorProvider{Actor: export.Actor{ID: "user-1"}},
	})

	body := `{"resource":"missing","format":"csv"}`
	req := httptest.NewRequest(http.MethodPost, "/admin/exports", strings.NewReader(body))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}
