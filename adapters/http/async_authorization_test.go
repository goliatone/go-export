package exporthttp

import (
	"context"
	"errors"
	"github.com/goliatone/go-export/export"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

type independentGuard struct {
	deny   atomic.Bool
	checks atomic.Int64
}

func (g *independentGuard) AuthorizeExport(context.Context, export.Actor, export.ExportRequest, export.ResolvedDefinition) error {
	g.checks.Add(1)
	if g.deny.Load() {
		return errors.New("export permission revoked")
	}
	return nil
}
func (g *independentGuard) AuthorizeDownload(context.Context, export.Actor, string) error { return nil }
func TestIndependentHTTPConfiguredGuardBeforeAsyncSubmission(t *testing.T) {
	for _, delivery := range []string{"sync", "async"} {
		t.Run(delivery, func(t *testing.T) {
			r := newTestRunner(t)
			tr := export.NewMemoryTracker()
			svc := export.NewService(export.ServiceConfig{Runner: r, Tracker: tr, Store: export.NewMemoryStore()})
			g := &independentGuard{}
			g.deny.Store(true)
			h := NewHandler(Config{Service: svc, Runner: r, Guard: g, ActorProvider: StaticActorProvider{Actor: export.Actor{ID: "user-a"}}})
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest("POST", "/admin/exports", strings.NewReader(`{"definition":"users","format":"csv","delivery":"`+delivery+`"}`)))
			if w.Code != 403 || g.checks.Load() == 0 {
				t.Fatalf("configured API guard bypassed: status=%d guard_checks=%d body=%s", w.Code, g.checks.Load(), w.Body.String())
			}
		})
	}
}
func TestIndependentHTTPIdempotentReplayRechecksExportPermission(t *testing.T) {
	r := newTestRunner(t)
	tr := export.NewMemoryTracker()
	g := &independentGuard{}
	svc := export.NewService(export.ServiceConfig{Runner: r, Tracker: tr, Store: export.NewMemoryStore(), Guard: g})
	h := NewHandler(Config{Service: svc, Runner: r, Guard: g, ActorProvider: StaticActorProvider{Actor: export.Actor{ID: "user-a"}}, IdempotencyStore: NewMemoryIdempotencyStore()})
	call := func() *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		req := httptest.NewRequest("POST", "/admin/exports", strings.NewReader(`{"definition":"users","format":"csv","delivery":"async"}`))
		req.Header.Set("Idempotency-Key", "operation-a")
		h.ServeHTTP(w, req)
		return w
	}
	w := call()
	if w.Code != 202 {
		t.Fatalf("initial queue: status=%d body=%s", w.Code, w.Body.String())
	}
	g.deny.Store(true)
	before := g.checks.Load()
	w = call()
	if w.Code != 403 || g.checks.Load() == before {
		t.Fatalf("revoked export policy bypassed by replay: status=%d new_export_checks=%d body=%s", w.Code, g.checks.Load()-before, w.Body.String())
	}
}

type queueSpy struct{ calls atomic.Int64 }

func (q *queueSpy) RequestExport(context.Context, export.Actor, export.ExportRequest) (export.ExportRecord, error) {
	q.calls.Add(1)
	return export.ExportRecord{ID: "must-not-queue", State: export.StateQueued}, nil
}

type idempotencySpy struct {
	IdempotencyStore
	reads atomic.Int64
}

func (s *idempotencySpy) Get(ctx context.Context, k string) (string, bool, error) {
	s.reads.Add(1)
	return s.IdempotencyStore.Get(ctx, k)
}
func TestHandlerAsyncGuardBeforeCustomQueueAndIdempotency(t *testing.T) {
	for _, policy := range []string{"controller", "runner"} {
		t.Run(policy, func(t *testing.T) {
			r := newTestRunner(t)
			svc := export.NewService(export.ServiceConfig{Runner: r, Tracker: export.NewMemoryTracker(), Store: export.NewMemoryStore()})
			g := &independentGuard{}
			g.deny.Store(true)
			configured := g
			if policy == "runner" {
				r.Guard = g
				configured = &independentGuard{}
			}
			queue := &queueSpy{}
			idempotency := &idempotencySpy{IdempotencyStore: NewMemoryIdempotencyStore()}
			h := NewHandler(Config{Service: svc, Runner: r, Guard: configured, AsyncRequester: queue, IdempotencyStore: idempotency, ActorProvider: StaticActorProvider{Actor: export.Actor{ID: "actor-a"}}})
			req := httptest.NewRequest("POST", "/admin/exports", strings.NewReader(`{"definition":"users","format":"csv","delivery":"async"}`))
			req.Header.Set("Idempotency-Key", "protected-operation")
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)
			if w.Code != 403 || g.checks.Load() == 0 || queue.calls.Load() != 0 || idempotency.reads.Load() != 0 || w.Header().Get("X-Export-ID") != "" {
				t.Fatalf("async policy order: status=%d checks=%d queue=%d idempotency=%d body=%s", w.Code, g.checks.Load(), queue.calls.Load(), idempotency.reads.Load(), w.Body.String())
			}
		})
	}
}
