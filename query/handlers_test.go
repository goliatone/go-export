package query

import (
	"context"
	"errors"
	"testing"

	"github.com/goliatone/go-export/export"
)

type denyGuard struct {
	downloadCalls int
}

func (g *denyGuard) AuthorizeExport(ctx context.Context, actor export.Actor, req export.ExportRequest, def export.ResolvedDefinition) error {
	_ = ctx
	_ = actor
	_ = req
	_ = def
	return errors.New("deny")
}

func (g *denyGuard) AuthorizeDownload(ctx context.Context, actor export.Actor, exportID string) error {
	_ = ctx
	_ = actor
	_ = exportID
	g.downloadCalls++
	return errors.New("deny")
}

func TestExportStatusHandler_GuardBlocks(t *testing.T) {
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

	handler := NewExportStatusHandler(service)
	_, err = handler.Query(context.Background(), ExportStatus{
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
