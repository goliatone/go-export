package command

import (
	"context"
	"testing"
	"time"

	"github.com/goliatone/go-export/export"
)

type captureBatchRequester struct {
	count int
}

func (c *captureBatchRequester) RequestExport(ctx context.Context, actor export.Actor, req export.ExportRequest) (export.ExportRecord, error) {
	c.count++
	return export.ExportRecord{ID: "exp-1"}, nil
}

func TestBuildPDFBatchRequests_DefaultsFormat(t *testing.T) {
	batch := DefinitionBatch{
		Actor:       export.Actor{ID: "actor-1"},
		Definitions: []string{"users", "teams"},
	}
	requests := BuildPDFBatchRequests(batch)
	if len(requests) != 2 {
		t.Fatalf("expected 2 requests, got %d", len(requests))
	}
	for _, req := range requests {
		if req.Request.Format != export.FormatPDF {
			t.Fatalf("expected pdf format")
		}
	}
}

func TestBatchCommand_RunHonorsLimits(t *testing.T) {
	requester := &captureBatchRequester{}
	loader := func(ctx context.Context) ([]BatchRequest, error) {
		return []BatchRequest{
			{Actor: export.Actor{ID: "actor-1"}, Request: export.ExportRequest{Definition: "users", Format: export.FormatPDF}},
			{Actor: export.Actor{ID: "actor-1"}, Request: export.ExportRequest{Definition: "teams", Format: export.FormatPDF}},
		}, nil
	}

	cmd := NewScheduledExportsCommand(requester, loader, WithBatchLimits(BatchLimits{MaxRequests: 1, MinInterval: time.Millisecond}))
	cmd.sleep = func(time.Duration) {}

	count, err := cmd.run(context.Background(), "")
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 request, got %d", count)
	}
	if requester.count != 1 {
		t.Fatalf("expected requester count 1, got %d", requester.count)
	}
}

func TestBatchCommand_RunUsesExecutorWhenProvided(t *testing.T) {
	var calls int
	executor := BatchExecutorFunc(func(ctx context.Context, actor export.Actor, req export.ExportRequest) (export.ExportRecord, error) {
		_ = ctx
		_ = actor
		_ = req
		calls++
		return export.ExportRecord{ID: "exp-1"}, nil
	})
	loader := func(ctx context.Context) ([]BatchRequest, error) {
		return []BatchRequest{
			{Actor: export.Actor{ID: "actor-1"}, Request: export.ExportRequest{Definition: "users", Format: export.FormatPDF}},
			{Actor: export.Actor{ID: "actor-2"}, Request: export.ExportRequest{Definition: "teams", Format: export.FormatPDF}},
		}, nil
	}

	cmd := NewScheduledExportsCommand(nil, loader, WithBatchExecutor(executor))

	count, err := cmd.run(context.Background(), "")
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 requests, got %d", count)
	}
	if calls != 2 {
		t.Fatalf("expected executor count 2, got %d", calls)
	}
}
