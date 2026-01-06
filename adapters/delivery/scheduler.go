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
	Enqueuer        Enqueuer
	TaskID          string
	TaskPath        string
	ExecutionConfig job.Config
	Logger          export.Logger
}

// Scheduler enqueues scheduled delivery jobs.
type Scheduler struct {
	enqueuer Enqueuer
	builder  *MessageBuilder
	logger   export.Logger
}

// NewScheduler creates a new delivery scheduler.
func NewScheduler(cfg SchedulerConfig) *Scheduler {
	logger := cfg.Logger
	if logger == nil {
		logger = export.NopLogger{}
	}
	builder := NewMessageBuilder(MessageBuilderConfig{
		TaskID:          cfg.TaskID,
		TaskPath:        cfg.TaskPath,
		ExecutionConfig: cfg.ExecutionConfig,
		Logger:          logger,
	})

	return &Scheduler{
		enqueuer: cfg.Enqueuer,
		builder:  builder,
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
	if s.builder == nil {
		return export.NewError(export.KindInternal, "message builder is nil", nil)
	}

	msg, err := s.builder.Build(ctx, req)
	if err != nil {
		return err
	}
	if msg == nil {
		return export.NewError(export.KindValidation, "execution message is required", nil)
	}

	if err := s.enqueuer.Enqueue(ctx, msg); err != nil {
		return export.NewError(export.KindExternal, "enqueue delivery failed", err)
	}
	return nil
}
