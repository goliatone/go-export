package exportdelivery

import (
	"context"
	"reflect"
	"testing"

	"github.com/goliatone/go-export/export"
	job "github.com/goliatone/go-job"
)

type captureDeliveryHandler struct {
	requests []Request
}

func (h *captureDeliveryHandler) Deliver(ctx context.Context, req Request) (Result, error) {
	_ = ctx
	h.requests = append(h.requests, req)
	return Result{}, nil
}

func TestTask_GetHandler_UsesBuilder(t *testing.T) {
	handler := &captureDeliveryHandler{}
	req := Request{
		Actor:  export.Actor{ID: "actor-1"},
		Export: export.ExportRequest{Definition: "users", Format: export.FormatPDF},
		Targets: []Target{{
			Kind:  TargetEmail,
			Email: EmailTarget{To: []string{"demo@example.com"}},
		}},
	}

	encoded, err := encodePayload(Payload{Request: req})
	if err != nil {
		t.Fatalf("encode payload: %v", err)
	}

	task := NewTask(TaskConfig{
		Handler: handler,
		MessageBuilder: func(ctx context.Context) (*job.ExecutionMessage, error) {
			_ = ctx
			return &job.ExecutionMessage{
				JobID:      "export:deliver",
				ScriptPath: "export:deliver",
				Parameters: map[string]any{"payload": encoded},
			}, nil
		},
	})

	if err := task.GetHandler()(); err != nil {
		t.Fatalf("handler: %v", err)
	}
	if len(handler.requests) != 1 {
		t.Fatalf("expected 1 request, got %d", len(handler.requests))
	}
	if !reflect.DeepEqual(handler.requests[0], req) {
		t.Fatalf("unexpected request payload")
	}
}

func TestTask_GetHandler_MissingBuilder(t *testing.T) {
	task := NewTask(TaskConfig{Handler: &captureDeliveryHandler{}})

	err := task.GetHandler()()
	if export.KindFromError(err) != export.KindNotImpl {
		t.Fatalf("expected not_implemented error, got %v", err)
	}
}
