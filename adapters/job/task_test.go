package exportjob

import (
	"context"
	"testing"

	"github.com/goliatone/go-command/dispatcher"
	exportcmd "github.com/goliatone/go-export/command"
	"github.com/goliatone/go-export/export"
	job "github.com/goliatone/go-job"
)

func TestGenerateTask_GetHandler_BuildsMessageAndExecutes(t *testing.T) {
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

	builder := NewMessageBuilder(MessageBuilderConfig{
		Service: svc,
		Tracker: tracker,
	})

	actor := export.Actor{ID: "actor-1"}
	req := export.ExportRequest{
		Definition: "users",
		Format:     export.FormatCSV,
	}

	var exportID string
	task := NewGenerateTask(TaskConfig{
		Store: store,
		MessageBuilder: func(ctx context.Context) (*job.ExecutionMessage, error) {
			result, err := builder.Build(ctx, actor, req)
			if err != nil {
				return nil, err
			}
			exportID = result.Record.ID
			return result.Message, nil
		},
	})

	if err := task.GetHandler()(); err != nil {
		t.Fatalf("handler: %v", err)
	}
	if exportID == "" {
		t.Fatalf("expected export id to be set")
	}

	status, err := svc.Status(context.Background(), actor, exportID)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.State != export.StateCompleted {
		t.Fatalf("expected completed state, got %s", status.State)
	}
}
