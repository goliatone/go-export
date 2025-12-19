package command

import (
	"context"
	"errors"
	"testing"
	"time"

	gcmd "github.com/goliatone/go-command"
	"github.com/goliatone/go-export/export"
)

type stubService struct {
	request  func(ctx context.Context, actor export.Actor, req export.ExportRequest) (export.ExportRecord, error)
	generate func(ctx context.Context, actor export.Actor, exportID string, req export.ExportRequest) (export.ExportResult, error)
	cancel   func(ctx context.Context, actor export.Actor, exportID string) (export.ExportRecord, error)
	delete   func(ctx context.Context, actor export.Actor, exportID string) error
	status   func(ctx context.Context, actor export.Actor, exportID string) (export.ExportRecord, error)
	history  func(ctx context.Context, actor export.Actor, filter export.ProgressFilter) ([]export.ExportRecord, error)
	download func(ctx context.Context, actor export.Actor, exportID string) (export.DownloadInfo, error)
	cleanup  func(ctx context.Context, now time.Time) (int, error)
}

func (s *stubService) RequestExport(ctx context.Context, actor export.Actor, req export.ExportRequest) (export.ExportRecord, error) {
	if s.request != nil {
		return s.request(ctx, actor, req)
	}
	return export.ExportRecord{}, nil
}

func (s *stubService) GenerateExport(ctx context.Context, actor export.Actor, exportID string, req export.ExportRequest) (export.ExportResult, error) {
	if s.generate != nil {
		return s.generate(ctx, actor, exportID, req)
	}
	return export.ExportResult{}, nil
}

func (s *stubService) CancelExport(ctx context.Context, actor export.Actor, exportID string) (export.ExportRecord, error) {
	if s.cancel != nil {
		return s.cancel(ctx, actor, exportID)
	}
	return export.ExportRecord{}, nil
}

func (s *stubService) DeleteExport(ctx context.Context, actor export.Actor, exportID string) error {
	if s.delete != nil {
		return s.delete(ctx, actor, exportID)
	}
	return nil
}

func (s *stubService) Status(ctx context.Context, actor export.Actor, exportID string) (export.ExportRecord, error) {
	if s.status != nil {
		return s.status(ctx, actor, exportID)
	}
	return export.ExportRecord{}, nil
}

func (s *stubService) History(ctx context.Context, actor export.Actor, filter export.ProgressFilter) ([]export.ExportRecord, error) {
	if s.history != nil {
		return s.history(ctx, actor, filter)
	}
	return nil, nil
}

func (s *stubService) DownloadMetadata(ctx context.Context, actor export.Actor, exportID string) (export.DownloadInfo, error) {
	if s.download != nil {
		return s.download(ctx, actor, exportID)
	}
	return export.DownloadInfo{}, nil
}

func (s *stubService) Cleanup(ctx context.Context, now time.Time) (int, error) {
	if s.cleanup != nil {
		return s.cleanup(ctx, now)
	}
	return 0, nil
}

type denyGuard struct {
	exportCalls   int
	downloadCalls int
}

func (g *denyGuard) AuthorizeExport(ctx context.Context, actor export.Actor, req export.ExportRequest, def export.ResolvedDefinition) error {
	_ = ctx
	_ = actor
	_ = req
	_ = def
	g.exportCalls++
	return errors.New("deny")
}

func (g *denyGuard) AuthorizeDownload(ctx context.Context, actor export.Actor, exportID string) error {
	_ = ctx
	_ = actor
	_ = exportID
	g.downloadCalls++
	return errors.New("deny")
}

func TestRequestExportHandler_StoresResults(t *testing.T) {
	want := export.ExportRecord{ID: "exp-1"}
	svc := &stubService{
		request: func(ctx context.Context, actor export.Actor, req export.ExportRequest) (export.ExportRecord, error) {
			_ = ctx
			_ = actor
			_ = req
			return want, nil
		},
	}

	handler := NewRequestExportHandler(svc)
	var got export.ExportRecord
	result := gcmd.NewResult[export.ExportRecord]()
	ctx := gcmd.ContextWithResult(context.Background(), result)

	err := handler.Execute(ctx, RequestExport{
		Actor:   export.Actor{ID: "actor-1"},
		Request: export.ExportRequest{Definition: "users"},
		Result:  &got,
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if got.ID != want.ID {
		t.Fatalf("expected result pointer %q, got %q", want.ID, got.ID)
	}

	stored, ok := result.Load()
	if !ok {
		t.Fatalf("expected context result")
	}
	if stored.ID != want.ID {
		t.Fatalf("expected context result %q, got %q", want.ID, stored.ID)
	}
}

func TestCancelExportHandler_GuardBlocks(t *testing.T) {
	tracker := export.NewMemoryTracker()
	_, err := tracker.Start(context.Background(), export.ExportRecord{
		ID:         "exp-1",
		Definition: "users",
		Format:     export.FormatCSV,
	})
	if err != nil {
		t.Fatalf("tracker start: %v", err)
	}

	guard := &denyGuard{}
	service := export.NewService(export.ServiceConfig{
		Runner:  export.NewRunner(),
		Guard:   guard,
		Tracker: tracker,
	})

	handler := NewCancelExportHandler(service)
	err = handler.Execute(context.Background(), CancelExport{
		Actor:    export.Actor{ID: "actor-1"},
		ExportID: "exp-1",
	})
	if err == nil {
		t.Fatalf("expected guard error")
	}
	if guard.downloadCalls == 0 {
		t.Fatalf("expected download guard to be called")
	}
}
