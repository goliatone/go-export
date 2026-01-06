package exportdelivery

import (
	"context"
	"reflect"
	"testing"

	"github.com/goliatone/go-export/export"
	job "github.com/goliatone/go-job"
)

type enqueuerFunc func(ctx context.Context, msg *job.ExecutionMessage) error

func (f enqueuerFunc) Enqueue(ctx context.Context, msg *job.ExecutionMessage) error {
	if f == nil {
		return export.NewError(export.KindInternal, "enqueuer is nil", nil)
	}
	return f(ctx, msg)
}

func TestScheduler_RequestDelivery_DecodesPayload(t *testing.T) {
	handler := &captureDeliveryHandler{}
	task := NewTask(TaskConfig{Handler: handler})

	enqueuer := enqueuerFunc(func(ctx context.Context, msg *job.ExecutionMessage) error {
		return task.Execute(ctx, msg)
	})

	scheduler := NewScheduler(SchedulerConfig{Enqueuer: enqueuer})
	req := Request{
		Actor:  export.Actor{ID: "actor-1"},
		Export: export.ExportRequest{Definition: "users", Format: export.FormatPDF},
		Targets: []Target{{
			Kind:  TargetEmail,
			Email: EmailTarget{To: []string{"demo@example.com"}},
		}},
	}

	if err := scheduler.RequestDelivery(context.Background(), req); err != nil {
		t.Fatalf("request delivery: %v", err)
	}
	if len(handler.requests) != 1 {
		t.Fatalf("expected 1 request, got %d", len(handler.requests))
	}
	if !reflect.DeepEqual(handler.requests[0], req) {
		t.Fatalf("unexpected request payload")
	}
}
