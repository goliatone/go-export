package export

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
)

type policyGuard struct{ deny *atomic.Bool }

func (g policyGuard) AuthorizeExport(context.Context, Actor, ExportRequest, ResolvedDefinition) error {
	if g.deny.Load() {
		return errors.New("private-policy-detail")
	}
	return nil
}
func (g policyGuard) AuthorizeDownload(context.Context, Actor, string) error { return nil }

type observedStore struct {
	*MemoryStore
	puts atomic.Int64
}

func (s *observedStore) Put(ctx context.Context, key string, r io.Reader, m ArtifactMeta) (ArtifactRef, error) {
	s.puts.Add(1)
	return s.MemoryStore.Put(ctx, key, r, m)
}

type observedTracker struct {
	*MemoryTracker
	starts, reads atomic.Int64
}

func (s *observedTracker) Start(ctx context.Context, r ExportRecord) (string, error) {
	s.starts.Add(1)
	return s.MemoryTracker.Start(ctx, r)
}
func (s *observedTracker) Status(ctx context.Context, id string) (ExportRecord, error) {
	s.reads.Add(1)
	return s.MemoryTracker.Status(ctx, id)
}
func guardFixture(t *testing.T) (*Runner, *observedStore, *observedTracker) {
	t.Helper()
	r := NewRunner()
	if err := r.Definitions.Register(ExportDefinition{Name: "resources", RowSourceKey: "rows", Schema: Schema{Columns: []Column{{Name: "id"}}}}); err != nil {
		t.Fatal(err)
	}
	if err := r.RowSources.Register("rows", func(ExportRequest, ResolvedDefinition) (RowSource, error) {
		return &stubSource{iter: &stubIterator{rows: []Row{{"a"}}}}, nil
	}); err != nil {
		t.Fatal(err)
	}
	return r, &observedStore{MemoryStore: NewMemoryStore()}, &observedTracker{MemoryTracker: NewMemoryTracker()}
}
func requireGuardDenial(t *testing.T, err error) {
	t.Helper()
	if err == nil || AsGoError(err).TextCode != "authz" {
		t.Fatalf("expected authz denial, got %v", err)
	}
	if strings.Contains(err.Error(), "private-policy-detail") {
		t.Fatalf("private denial exposed: %v", err)
	}
}
func TestServiceGenerateExportGuardBeforeSideEffects(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(map[bool]string{false: "unknown-id", true: "queued-id"}[existing], func(t *testing.T) {
			r, store, tracker := guardFixture(t)
			deny := new(atomic.Bool)
			deny.Store(true)
			if existing {
				_, err := tracker.MemoryTracker.Start(context.Background(), ExportRecord{ID: "export-1", State: StateQueued})
				if err != nil {
					t.Fatal(err)
				}
			}
			svc := NewService(ServiceConfig{Runner: r, Store: store, Tracker: tracker, Guard: policyGuard{deny}})
			out, err := svc.GenerateExport(context.Background(), Actor{ID: "actor-1"}, "export-1", ExportRequest{Definition: "resources", Format: FormatCSV})
			requireGuardDenial(t, err)
			if store.puts.Load() != 0 || tracker.starts.Load() != 0 || tracker.reads.Load() != 0 || out.ID != "" {
				t.Fatalf("denied generation touched protected dependencies: puts=%d starts=%d reads=%d result=%+v", store.puts.Load(), tracker.starts.Load(), tracker.reads.Load(), out)
			}
		})
	}
}
func TestServiceConfiguredGuardCoversEveryExecutionMode(t *testing.T) {
	for _, mode := range []string{"sync", "queue", "generate"} {
		t.Run(mode, func(t *testing.T) {
			r, store, tracker := guardFixture(t)
			allow := new(atomic.Bool)
			deny := new(atomic.Bool)
			deny.Store(true)
			r.Guard = policyGuard{allow}
			svc := NewService(ServiceConfig{Runner: r, Store: store, Tracker: tracker, Guard: policyGuard{deny}})
			var output bytes.Buffer
			req := ExportRequest{Definition: "resources", Format: FormatCSV, Output: &output}
			var err error
			if mode == "generate" {
				_, err = svc.GenerateExport(context.Background(), Actor{ID: "actor-1"}, "x", req)
			} else {
				if mode == "queue" {
					req.Delivery = DeliveryAsync
				}
				_, err = svc.RequestExport(context.Background(), Actor{ID: "actor-1"}, req)
			}
			requireGuardDenial(t, err)
			if output.Len() != 0 || store.puts.Load() != 0 || tracker.starts.Load() != 0 {
				t.Fatal("configured service guard was bypassed")
			}
		})
	}
}
func TestServiceGenerateExportRechecksQueuedAuthorization(t *testing.T) {
	r, store, tracker := guardFixture(t)
	deny := new(atomic.Bool)
	svc := NewService(ServiceConfig{Runner: r, Store: store, Tracker: tracker, Guard: policyGuard{deny}})
	actor := Actor{ID: "actor-1"}
	req := ExportRequest{Definition: "resources", Format: FormatCSV, Delivery: DeliveryAsync}
	record, err := svc.RequestExport(context.Background(), actor, req)
	if err != nil {
		t.Fatal(err)
	}
	deny.Store(true)
	tracker.starts.Store(0)
	tracker.reads.Store(0)
	_, err = svc.GenerateExport(context.Background(), actor, record.ID, req)
	requireGuardDenial(t, err)
	if store.puts.Load() != 0 || tracker.starts.Load() != 0 || tracker.reads.Load() != 0 {
		t.Fatal("revoked queued request touched protected state")
	}
	persisted, err := tracker.MemoryTracker.Status(context.Background(), record.ID)
	if err != nil || persisted.State != StateQueued {
		t.Fatalf("denied generation mutated queued record: %+v %v", persisted, err)
	}
}

func TestServicePreservesStricterRunnerGuard(t *testing.T) {
	for _, mode := range []string{"sync", "queue", "generate"} {
		t.Run(mode, func(t *testing.T) {
			r, store, tracker := guardFixture(t)
			deny := new(atomic.Bool)
			deny.Store(true)
			allow := new(atomic.Bool)
			r.Guard = policyGuard{deny}
			svc := NewService(ServiceConfig{Runner: r, Store: store, Tracker: tracker, Guard: policyGuard{allow}})
			var output bytes.Buffer
			req := ExportRequest{Definition: "resources", Format: FormatCSV, Output: &output}
			var err error
			if mode == "generate" {
				_, err = svc.GenerateExport(context.Background(), Actor{ID: "actor-1"}, "x", req)
			} else {
				if mode == "queue" {
					req.Delivery = DeliveryAsync
				}
				_, err = svc.RequestExport(context.Background(), Actor{ID: "actor-1"}, req)
			}
			requireGuardDenial(t, err)
			if output.Len() != 0 || store.puts.Load() != 0 || tracker.starts.Load() != 0 {
				t.Fatal("service weakened the runner policy")
			}
		})
	}
}

type recordDenyGuard struct{}

func (recordDenyGuard) AuthorizeExport(context.Context, Actor, ExportRequest, ResolvedDefinition) error {
	return nil
}
func (recordDenyGuard) AuthorizeDownload(context.Context, Actor, string) error {
	return errors.New("another-Group-private-record")
}
func TestServiceGenerateExportGuardsTargetRecord(t *testing.T) {
	r, store, tracker := guardFixture(t)
	ctx := context.Background()
	_, err := tracker.MemoryTracker.Start(ctx, ExportRecord{ID: "other-group-export", State: StateCompleted, RequestedBy: Actor{ID: "other-actor"}, Counts: ExportCounts{Processed: 9}})
	if err != nil {
		t.Fatal(err)
	}
	svc := NewService(ServiceConfig{Runner: r, Store: store, Tracker: tracker, Guard: recordDenyGuard{}})
	_, err = svc.GenerateExport(ctx, Actor{ID: "actor-1"}, "other-group-export", ExportRequest{Definition: "resources", Format: FormatCSV})
	requireGuardDenial(t, err)
	if tracker.reads.Load() != 0 || tracker.starts.Load() != 0 || store.puts.Load() != 0 {
		t.Fatal("unauthorized target record was read or overwritten")
	}
	got, err := tracker.MemoryTracker.Status(ctx, "other-group-export")
	if err != nil || got.State != StateCompleted || got.Counts.Processed != 9 {
		t.Fatalf("target metadata changed: %+v %v", got, err)
	}
}
