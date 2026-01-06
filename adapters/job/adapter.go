package exportjob

import (
	"context"
	"time"

	"github.com/goliatone/go-export/export"
	job "github.com/goliatone/go-job"
)

// Enqueuer delivers execution messages to go-job.
type Enqueuer interface {
	Enqueue(ctx context.Context, msg *job.ExecutionMessage) error
}

// EnqueuerFunc adapts a function to an Enqueuer.
type EnqueuerFunc func(ctx context.Context, msg *job.ExecutionMessage) error

func (f EnqueuerFunc) Enqueue(ctx context.Context, msg *job.ExecutionMessage) error {
	if f == nil {
		return export.NewError(export.KindInternal, "enqueuer is nil", nil)
	}
	return f(ctx, msg)
}

// Config configures the go-job export scheduler.
type Config struct {
	Service          export.Service
	Enqueuer         Enqueuer
	Tracker          export.ProgressTracker
	IdempotencyStore IdempotencyStore
	IdempotencyTTL   time.Duration
	TaskID           string
	TaskPath         string
	ExecutionConfig  job.Config
	Logger           export.Logger
}

// Scheduler enqueues export generation jobs.
type Scheduler struct {
	enqueuer Enqueuer
	builder  *MessageBuilder
	tracker  export.ProgressTracker
	logger   export.Logger
}

// NewScheduler creates a new job scheduler adapter.
func NewScheduler(cfg Config) *Scheduler {
	logger := cfg.Logger
	if logger == nil {
		logger = export.NopLogger{}
	}
	builder := NewMessageBuilder(MessageBuilderConfig{
		Service:          cfg.Service,
		Tracker:          cfg.Tracker,
		IdempotencyStore: cfg.IdempotencyStore,
		IdempotencyTTL:   cfg.IdempotencyTTL,
		TaskID:           cfg.TaskID,
		TaskPath:         cfg.TaskPath,
		Config:           cfg.ExecutionConfig,
		Logger:           logger,
	})

	return &Scheduler{
		enqueuer: cfg.Enqueuer,
		builder:  builder,
		tracker:  cfg.Tracker,
		logger:   logger,
	}
}

// RequestExport creates an async export record and enqueues job execution.
func (s *Scheduler) RequestExport(ctx context.Context, actor export.Actor, req export.ExportRequest) (export.ExportRecord, error) {
	if s == nil {
		return export.ExportRecord{}, export.NewError(export.KindInternal, "scheduler is nil", nil)
	}
	if s.enqueuer == nil {
		return export.ExportRecord{}, export.NewError(export.KindNotImpl, "job enqueuer not configured", nil)
	}
	if s.builder == nil {
		return export.ExportRecord{}, export.NewError(export.KindInternal, "message builder is nil", nil)
	}

	result, err := s.builder.Build(ctx, actor, req)
	if err != nil {
		return result.Record, err
	}
	if result.Reused {
		return result.Record, nil
	}
	if result.Message == nil {
		return result.Record, export.NewError(export.KindValidation, "execution message is required", nil)
	}

	if err := s.enqueuer.Enqueue(ctx, result.Message); err != nil {
		if s.tracker != nil {
			if ferr := s.tracker.Fail(ctx, result.Record.ID, err, map[string]any{"stage": "enqueue"}); ferr != nil {
				s.logger.Errorf("enqueue failure tracking failed: %v", ferr)
			}
		}
		return result.Record, err
	}

	if result.Signature != "" {
		if err := s.builder.StoreIdempotency(ctx, result.Signature, result.Record.ID); err != nil {
			s.logger.Errorf("idempotency store set failed: %v", err)
		}
	}

	return result.Record, nil
}

func isReusableState(state export.ExportState) bool {
	switch state {
	case export.StateQueued, export.StateRunning, export.StatePublishing, export.StateCompleted:
		return true
	default:
		return false
	}
}
