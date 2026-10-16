package exportjob

import (
	"context"
	"encoding/json"
	cmd "github.com/goliatone/go-export/command"
	"github.com/goliatone/go-export/export"
	job "github.com/goliatone/go-job"
	"testing"
)

func TestWebhookTaskRequiresTrustedIdentity(t *testing.T) {
	if _, e := NewWebhookTask(WebhookTaskConfig{}); e == nil {
		t.Fatal("unauthenticated default")
	}
	payload, _ := json.Marshal(cmd.DeliverWebhook{Actor: export.Actor{ID: "forged"}, ExportID: "export", ReceiptID: "receipt"})
	m := &job.ExecutionMessage{Parameters: map[string]any{"payload": json.RawMessage(payload)}}
	called := false
	task, e := NewWebhookTask(WebhookTaskConfig{Resolve: func(ctx context.Context, _ *job.ExecutionMessage) (context.Context, export.Actor, error) {
		return ctx, export.Actor{ID: "trusted"}, nil
	}, Dispatch: func(_ context.Context, m cmd.DeliverWebhook) error {
		called = true
		if m.Actor.ID != "trusted" || m.ExportID != "export" || m.ReceiptID != "receipt" {
			t.Fatal("persisted claims used as authentication")
		}
		return nil
	}})
	if e != nil {
		t.Fatal(e)
	}
	if e = task.Execute(context.Background(), m); e != nil || !called {
		t.Fatal("typed command not dispatched", e)
	}
	task.cfg.Resolve = func(ctx context.Context, _ *job.ExecutionMessage) (context.Context, export.Actor, error) {
		return ctx, export.Actor{}, nil
	}
	called = false
	if e = task.Execute(context.Background(), m); e == nil || called {
		t.Fatal("missing worker identity accepted")
	}
}
