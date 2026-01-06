package exportdelivery

import (
	"context"

	"github.com/goliatone/go-export/export"
	job "github.com/goliatone/go-job"
)

// MessageBuilderConfig configures message building for scheduled deliveries.
type MessageBuilderConfig struct {
	TaskID          string
	TaskPath        string
	ExecutionConfig job.Config
	Logger          export.Logger
}

// MessageBuilder builds execution messages for delivery requests.
type MessageBuilder struct {
	taskID   string
	taskPath string
	config   job.Config
	logger   export.Logger
}

// NewMessageBuilder creates a new delivery message builder.
func NewMessageBuilder(cfg MessageBuilderConfig) *MessageBuilder {
	logger := cfg.Logger
	if logger == nil {
		logger = export.NopLogger{}
	}
	taskID := cfg.TaskID
	if taskID == "" {
		taskID = DefaultDeliveryTaskID
	}
	taskPath := cfg.TaskPath
	if taskPath == "" {
		taskPath = DefaultDeliveryTaskPath
	}

	return &MessageBuilder{
		taskID:   taskID,
		taskPath: taskPath,
		config:   cfg.ExecutionConfig,
		logger:   logger,
	}
}

// Build creates an execution message for a delivery request.
func (b *MessageBuilder) Build(ctx context.Context, req Request) (*job.ExecutionMessage, error) {
	if b == nil {
		return nil, export.NewError(export.KindInternal, "message builder is nil", nil)
	}
	_ = ctx

	encoded, err := encodePayload(Payload{Request: req})
	if err != nil {
		return nil, err
	}

	msg := &job.ExecutionMessage{
		JobID:      b.taskID,
		ScriptPath: b.taskPath,
		Config:     b.config,
		Parameters: map[string]any{"payload": encoded},
	}

	return msg, nil
}
