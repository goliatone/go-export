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
	Logger           export.Logger
}

// Scheduler enqueues export generation jobs.
type Scheduler struct {
	service          export.Service
	enqueuer         Enqueuer
	tracker          export.ProgressTracker
	idempotencyStore IdempotencyStore
	idempotencyTTL   time.Duration
	taskID           string
	taskPath         string
	logger           export.Logger
}

// NewScheduler creates a new job scheduler adapter.
func NewScheduler(cfg Config) *Scheduler {
	logger := cfg.Logger
	if logger == nil {
		logger = export.NopLogger{}
	}
	taskID := cfg.TaskID
	if taskID == "" {
		taskID = DefaultGenerateTaskID
	}
	taskPath := cfg.TaskPath
	if taskPath == "" {
		taskPath = DefaultGenerateTaskPath
	}

	return &Scheduler{
		service:          cfg.Service,
		enqueuer:         cfg.Enqueuer,
		tracker:          cfg.Tracker,
		idempotencyStore: cfg.IdempotencyStore,
		idempotencyTTL:   cfg.IdempotencyTTL,
		taskID:           taskID,
		taskPath:         taskPath,
		logger:           logger,
	}
}

// RequestExport creates an async export record and enqueues job execution.
func (s *Scheduler) RequestExport(ctx context.Context, actor export.Actor, req export.ExportRequest) (export.ExportRecord, error) {
	if s == nil {
		return export.ExportRecord{}, export.NewError(export.KindInternal, "scheduler is nil", nil)
	}
	if s.service == nil {
		return export.ExportRecord{}, export.NewError(export.KindNotImpl, "export service not configured", nil)
	}
	if s.enqueuer == nil {
		return export.ExportRecord{}, export.NewError(export.KindNotImpl, "job enqueuer not configured", nil)
	}
	if actor.ID == "" {
		return export.ExportRecord{}, export.NewError(export.KindValidation, "actor ID is required", nil)
	}

	asyncReq := req
	asyncReq.Delivery = export.DeliveryAsync
	asyncReq.Output = nil

	signature := ""
	if asyncReq.IdempotencyKey != "" && s.idempotencyStore != nil {
		signature = buildIdempotencyKey(asyncReq.IdempotencyKey, actor, asyncReq)
		exportID, ok, err := s.idempotencyStore.Get(ctx, signature)
		if err != nil {
			return export.ExportRecord{}, err
		}
		if ok {
			record, err := s.service.Status(ctx, actor, exportID)
			if err == nil && isReusableState(record.State) {
				return record, nil
			}
		}
	}

	record, err := s.service.RequestExport(ctx, actor, asyncReq)
	if err != nil {
		return export.ExportRecord{}, err
	}

	payload := Payload{
		ExportID: record.ID,
		Actor:    actor,
		Request:  asyncReq,
	}
	encoded, err := encodePayload(payload)
	if err != nil {
		if s.tracker != nil {
			if ferr := s.tracker.Fail(ctx, record.ID, err, map[string]any{"stage": "payload"}); ferr != nil {
				s.logger.Errorf("payload failure tracking failed: %v", ferr)
			}
		}
		return record, err
	}

	msg := &job.ExecutionMessage{
		JobID:      s.taskID,
		ScriptPath: s.taskPath,
		Parameters: map[string]any{"payload": encoded},
	}

	if signature != "" {
		msg.IdempotencyKey = signature
		msg.DedupPolicy = job.DedupPolicyMerge
	}

	if err := s.enqueuer.Enqueue(ctx, msg); err != nil {
		if s.tracker != nil {
			if ferr := s.tracker.Fail(ctx, record.ID, err, map[string]any{"stage": "enqueue"}); ferr != nil {
				s.logger.Errorf("enqueue failure tracking failed: %v", ferr)
			}
		}
		return record, err
	}

	if signature != "" && s.idempotencyStore != nil {
		ttl := s.idempotencyTTL
		if ttl == 0 {
			ttl = 24 * time.Hour
		}
		if err := s.idempotencyStore.Set(ctx, signature, record.ID, ttl); err != nil {
			s.logger.Errorf("idempotency store set failed: %v", err)
		}
	}

	return record, nil
}

func isReusableState(state export.ExportState) bool {
	switch state {
	case export.StateQueued, export.StateRunning, export.StatePublishing, export.StateCompleted:
		return true
	default:
		return false
	}
}
