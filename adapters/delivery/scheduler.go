package exportdelivery

import (
	"context"

	"github.com/goliatone/go-export/export"
	job "github.com/goliatone/go-job"
)

const (
	DefaultDeliveryTaskID   = "export:deliver"
	DefaultDeliveryTaskPath = "export:deliver"
)

// Enqueuer delivers execution messages to go-job.
type Enqueuer interface {
	Enqueue(ctx context.Context, msg *job.ExecutionMessage) error
}

// SchedulerConfig configures the delivery scheduler.
type SchedulerConfig struct {
	Enqueuer Enqueuer
	TaskID   string
	TaskPath string
	Logger   export.Logger
}

// Scheduler enqueues scheduled delivery jobs.
type Scheduler struct {
	enqueuer Enqueuer
	taskID   string
	taskPath string
	logger   export.Logger
}

// NewScheduler creates a new delivery scheduler.
func NewScheduler(cfg SchedulerConfig) *Scheduler {
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

	return &Scheduler{
		enqueuer: cfg.Enqueuer,
		taskID:   taskID,
		taskPath: taskPath,
		logger:   logger,
	}
}

// RequestDelivery enqueues a scheduled delivery.
func (s *Scheduler) RequestDelivery(ctx context.Context, req Request) error {
	if s == nil {
		return export.NewError(export.KindInternal, "scheduler is nil", nil)
	}
	if s.enqueuer == nil {
		return export.NewError(export.KindNotImpl, "job enqueuer not configured", nil)
	}

	encoded, err := encodePayload(Payload{Request: req})
	if err != nil {
		return err
	}

	msg := &job.ExecutionMessage{
		JobID:      s.taskID,
		ScriptPath: s.taskPath,
		Parameters: map[string]any{"payload": encoded},
	}

	if err := s.enqueuer.Enqueue(ctx, msg); err != nil {
		return export.NewError(export.KindExternal, "enqueue delivery failed", err)
	}
	return nil
}
