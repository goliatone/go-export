package exportjob

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/goliatone/go-command/dispatcher"
	exportcmd "github.com/goliatone/go-export/command"
	"github.com/goliatone/go-export/export"
	job "github.com/goliatone/go-job"
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

type blockingSource struct {
	started chan struct{}
}

func (s *blockingSource) Open(ctx context.Context, spec export.RowSourceSpec) (export.RowIterator, error) {
	_ = ctx
	_ = spec
	select {
	case <-s.started:
	default:
		close(s.started)
	}
	return &blockingIterator{}, nil
}

type blockingIterator struct{}

func (it *blockingIterator) Next(ctx context.Context) (export.Row, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (it *blockingIterator) Close() error { return nil }

type deleteTrackingStore struct {
	deletes int
	mu      sync.Mutex
}

func (s *deleteTrackingStore) Put(ctx context.Context, key string, r io.Reader, meta export.ArtifactMeta) (export.ArtifactRef, error) {
	_ = ctx
	_ = key
	_ = r
	_ = meta
	return export.ArtifactRef{}, export.NewError(export.KindNotImpl, "put not implemented", nil)
}

func (s *deleteTrackingStore) Open(ctx context.Context, key string) (io.ReadCloser, export.ArtifactMeta, error) {
	_ = ctx
	_ = key
	return nil, export.ArtifactMeta{}, export.NewError(export.KindNotImpl, "open not implemented", nil)
}

func (s *deleteTrackingStore) Delete(ctx context.Context, key string) error {
	_ = ctx
	_ = key
	s.mu.Lock()
	s.deletes++
	s.mu.Unlock()
	return nil
}

func (s *deleteTrackingStore) SignedURL(ctx context.Context, key string, ttl time.Duration) (string, error) {
	_ = ctx
	_ = key
	_ = ttl
	return "", export.NewError(export.KindNotImpl, "signed url not implemented", nil)
}

func setupRunner(t *testing.T, source export.RowSource) *export.Runner {
	t.Helper()
	runner := export.NewRunner()
	if err := runner.Definitions.Register(export.ExportDefinition{
		Name:         "users",
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
		return source, nil
	}); err != nil {
		t.Fatalf("register source: %v", err)
	}
	return runner
}

func TestScheduler_RequestExport_EnqueueAndDownload(t *testing.T) {
	runner := setupRunner(t, &stubSource{rows: []export.Row{{"1", "alice"}}})
	tracker := export.NewMemoryTracker()
	store := export.NewMemoryStore()

	svc := export.NewService(export.ServiceConfig{
		Runner:  runner,
		Tracker: tracker,
		Store:   store,
	})

	sub := dispatcher.SubscribeCommand(exportcmd.NewGenerateExportHandler(svc))
	defer sub.Unsubscribe()

	task := NewGenerateTask(TaskConfig{Store: store})
	cmd := job.NewTaskCommander(task)
	enqueuer := EnqueuerFunc(func(ctx context.Context, msg *job.ExecutionMessage) error {
		return cmd.Execute(ctx, msg)
	})

	scheduler := NewScheduler(Config{
		Service:  svc,
		Enqueuer: enqueuer,
		Tracker:  tracker,
	})

	record, err := scheduler.RequestExport(context.Background(), export.Actor{ID: "actor-1"}, export.ExportRequest{
		Definition: "users",
		Format:     export.FormatCSV,
		Delivery:   export.DeliveryAsync,
	})
	if err != nil {
		t.Fatalf("request export: %v", err)
	}
	if record.ID == "" {
		t.Fatalf("expected export id")
	}

	status, err := svc.Status(context.Background(), export.Actor{ID: "actor-1"}, record.ID)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.State != export.StateCompleted {
		t.Fatalf("expected completed state, got %s", status.State)
	}

	info, err := svc.DownloadMetadata(context.Background(), export.Actor{ID: "actor-1"}, record.ID)
	if err != nil {
		t.Fatalf("download metadata: %v", err)
	}
	reader, _, err := store.Open(context.Background(), info.Artifact.Key)
	if err != nil {
		t.Fatalf("open artifact: %v", err)
	}
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read artifact: %v", err)
	}
	if !bytes.Contains(data, []byte("id,name")) {
		t.Fatalf("expected csv headers, got %q", string(data))
	}
}

func TestScheduler_RequestExport_Idempotency(t *testing.T) {
	runner := setupRunner(t, &stubSource{rows: []export.Row{{"1", "alice"}}})
	tracker := export.NewMemoryTracker()
	store := export.NewMemoryStore()

	svc := export.NewService(export.ServiceConfig{
		Runner:  runner,
		Tracker: tracker,
		Store:   store,
	})

	idempotency := NewMemoryIdempotencyStore()
	var enqueueCalls int
	enqueuer := EnqueuerFunc(func(ctx context.Context, msg *job.ExecutionMessage) error {
		_ = ctx
		_ = msg
		enqueueCalls++
		return nil
	})

	scheduler := NewScheduler(Config{
		Service:          svc,
		Enqueuer:         enqueuer,
		Tracker:          tracker,
		IdempotencyStore: idempotency,
	})

	req := export.ExportRequest{
		Definition:     "users",
		Format:         export.FormatCSV,
		Delivery:       export.DeliveryAsync,
		IdempotencyKey: "abc123",
	}
	first, err := scheduler.RequestExport(context.Background(), export.Actor{ID: "actor-1"}, req)
	if err != nil {
		t.Fatalf("first request: %v", err)
	}
	second, err := scheduler.RequestExport(context.Background(), export.Actor{ID: "actor-1"}, req)
	if err != nil {
		t.Fatalf("second request: %v", err)
	}
	if second.ID != first.ID {
		t.Fatalf("expected same export id, got %s vs %s", second.ID, first.ID)
	}
	if enqueueCalls != 1 {
		t.Fatalf("expected 1 enqueue call, got %d", enqueueCalls)
	}
}

func TestScheduler_CancelExportStopsJob(t *testing.T) {
	started := make(chan struct{})
	runner := setupRunner(t, &blockingSource{started: started})
	tracker := export.NewMemoryTracker()
	store := export.NewMemoryStore()
	cancelRegistry := NewCancelRegistry()

	svc := export.NewService(export.ServiceConfig{
		Runner:     runner,
		Tracker:    tracker,
		Store:      store,
		CancelHook: cancelRegistry,
	})

	sub := dispatcher.SubscribeCommand(exportcmd.NewGenerateExportHandler(svc))
	defer sub.Unsubscribe()

	task := NewGenerateTask(TaskConfig{CancelRegistry: cancelRegistry})
	cmd := job.NewTaskCommander(task)
	done := make(chan error, 1)
	enqueuer := EnqueuerFunc(func(ctx context.Context, msg *job.ExecutionMessage) error {
		go func() {
			done <- cmd.Execute(ctx, msg)
		}()
		return nil
	})

	scheduler := NewScheduler(Config{
		Service:  svc,
		Enqueuer: enqueuer,
		Tracker:  tracker,
	})

	record, err := scheduler.RequestExport(context.Background(), export.Actor{ID: "actor-1"}, export.ExportRequest{
		Definition: "users",
		Format:     export.FormatCSV,
		Delivery:   export.DeliveryAsync,
	})
	if err != nil {
		t.Fatalf("request export: %v", err)
	}

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatalf("job did not start")
	}

	_, err = svc.CancelExport(context.Background(), export.Actor{ID: "actor-1"}, record.ID)
	if err != nil {
		t.Fatalf("cancel export: %v", err)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("job did not finish")
	}

	status, err := tracker.Status(context.Background(), record.ID)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.State != export.StateCanceled {
		t.Fatalf("expected canceled state, got %s", status.State)
	}
}

type tempNetError struct{}

func (tempNetError) Error() string   { return "temporary" }
func (tempNetError) Timeout() bool   { return false }
func (tempNetError) Temporary() bool { return true }

func TestGenerateTask_RetriesRetryableErrors(t *testing.T) {
	var attempts int
	store := &deleteTrackingStore{}
	policy := RetryPolicy{
		MaxRetries: 2,
		Backoff: job.BackoffConfig{
			Strategy: job.BackoffNone,
		},
	}
	task := NewGenerateTask(TaskConfig{
		RetryPolicy: policy,
		Store:       store,
		Dispatch: func(ctx context.Context, msg exportcmd.GenerateExport) error {
			_ = ctx
			_ = msg
			attempts++
			if attempts < 3 {
				return tempNetError{}
			}
			return nil
		},
	})

	payload := Payload{
		ExportID: "exp-1",
		Actor:    export.Actor{ID: "actor-1"},
		Request: export.ExportRequest{
			Definition: "users",
			Format:     export.FormatCSV,
		},
	}
	encoded, err := encodePayload(payload)
	if err != nil {
		t.Fatalf("encode payload: %v", err)
	}

	err = task.Execute(context.Background(), &job.ExecutionMessage{
		Parameters: map[string]any{"payload": encoded},
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}
	if store.deletes != 2 {
		t.Fatalf("expected 2 cleanup deletes, got %d", store.deletes)
	}
}
