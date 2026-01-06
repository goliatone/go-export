package exportjob

import (
	"context"

	exportcmd "github.com/goliatone/go-export/command"
	"github.com/goliatone/go-export/export"
)

// NewBatchExecutor builds a BatchExecutor that runs exports synchronously.
func NewBatchExecutor(task *GenerateTask, builder *MessageBuilder) exportcmd.BatchExecutor {
	return exportcmd.BatchExecutorFunc(func(ctx context.Context, actor export.Actor, req export.ExportRequest) (export.ExportRecord, error) {
		if task == nil {
			return export.ExportRecord{}, export.NewError(export.KindInternal, "generate task is nil", nil)
		}
		if builder == nil {
			return export.ExportRecord{}, export.NewError(export.KindNotImpl, "message builder not configured", nil)
		}

		result, err := builder.Build(ctx, actor, req)
		if err != nil {
			return result.Record, err
		}
		if result.Reused {
			return result.Record, nil
		}
		if result.Message == nil {
			return result.Record, export.NewError(export.KindValidation, "execution message is required", nil)
		}

		if err := task.Execute(ctx, result.Message); err != nil {
			return result.Record, err
		}
		if result.Signature != "" {
			_ = builder.StoreIdempotency(ctx, result.Signature, result.Record.ID)
		}
		return result.Record, nil
	})
}
