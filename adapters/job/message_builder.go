package exportjob

import (
	"context"
	"errors"
	"time"

	"github.com/goliatone/go-export/export"
	job "github.com/goliatone/go-job"
)

var errExecutionSkipped = errors.New("export execution skipped")

// MessageBuilderConfig configures message building for export generation.
type MessageBuilderConfig struct {
	Service          export.Service
	Tracker          export.ProgressTracker
	IdempotencyStore IdempotencyStore
	IdempotencyTTL   time.Duration
	TaskID           string
	TaskPath         string
	Config           job.Config
	Logger           export.Logger
}

// MessageBuilder builds execution messages for export generation.
type MessageBuilder struct {
	service          export.Service
	tracker          export.ProgressTracker
	idempotencyStore IdempotencyStore
	idempotencyTTL   time.Duration
	taskID           string
	taskPath         string
	config           job.Config
	logger           export.Logger
}

// BuildResult captures the outcome of message building.
type BuildResult struct {
	Record    export.ExportRecord
	Message   *job.ExecutionMessage
	Signature string
	Reused    bool
}

// NewMessageBuilder creates a new MessageBuilder.
func NewMessageBuilder(cfg MessageBuilderConfig) *MessageBuilder {
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

	return &MessageBuilder{
		service:          cfg.Service,
		tracker:          cfg.Tracker,
		idempotencyStore: cfg.IdempotencyStore,
		idempotencyTTL:   cfg.IdempotencyTTL,
		taskID:           taskID,
		taskPath:         taskPath,
		config:           cfg.Config,
		logger:           logger,
	}
}

// Build prepares an execution message for an export request.
func (b *MessageBuilder) Build(ctx context.Context, actor export.Actor, req export.ExportRequest) (BuildResult, error) {
	if b == nil {
		return BuildResult{}, export.NewError(export.KindInternal, "message builder is nil", nil)
	}
	if b.service == nil {
		return BuildResult{}, export.NewError(export.KindNotImpl, "export service not configured", nil)
	}
	if actor.ID == "" {
		return BuildResult{}, export.NewError(export.KindValidation, "actor ID is required", nil)
	}
	if ctx == nil {
		ctx = context.Background()
	}

	asyncReq := req
	// Force async semantics so ExportID is created before execution.
	asyncReq.Delivery = export.DeliveryAsync
	asyncReq.Output = nil

	signature := ""
	if asyncReq.IdempotencyKey != "" && b.idempotencyStore != nil {
		signature = buildIdempotencyKey(asyncReq.IdempotencyKey, actor, asyncReq)
		exportID, ok, err := b.idempotencyStore.Get(ctx, signature)
		if err != nil {
			return BuildResult{}, err
		}
		if ok {
			record, err := b.service.Status(ctx, actor, exportID)
			if err == nil && isReusableState(record.State) {
				return BuildResult{Record: record, Signature: signature, Reused: true}, nil
			}
		}
	}

	record, err := b.service.RequestExport(ctx, actor, asyncReq)
	if err != nil {
		return BuildResult{}, err
	}

	payload := Payload{
		ExportID: record.ID,
		Actor:    actor,
		Request:  asyncReq,
	}
	encoded, err := encodePayload(payload)
	if err != nil {
		if b.tracker != nil {
			if ferr := b.tracker.Fail(ctx, record.ID, err, map[string]any{"stage": "payload"}); ferr != nil {
				b.logger.Errorf("payload failure tracking failed: %v", ferr)
			}
		}
		return BuildResult{Record: record, Signature: signature}, err
	}

	msg := &job.ExecutionMessage{
		JobID:      b.taskID,
		ScriptPath: b.taskPath,
		Config:     b.config,
		Parameters: map[string]any{"payload": encoded},
	}

	if signature != "" {
		msg.IdempotencyKey = signature
		msg.DedupPolicy = job.DedupPolicyMerge
	}

	return BuildResult{Record: record, Message: msg, Signature: signature}, nil
}

// BuildMessage returns an execution message or signals a no-op when the request was reused.
func (b *MessageBuilder) BuildMessage(ctx context.Context, actor export.Actor, req export.ExportRequest) (*job.ExecutionMessage, error) {
	result, err := b.Build(ctx, actor, req)
	if err != nil {
		return nil, err
	}
	if result.Reused {
		return nil, errExecutionSkipped
	}
	if result.Message == nil {
		return nil, export.NewError(export.KindValidation, "execution message is required", nil)
	}
	return result.Message, nil
}

// StoreIdempotency records an idempotency signature after successful enqueue.
func (b *MessageBuilder) StoreIdempotency(ctx context.Context, signature, exportID string) error {
	if signature == "" || b == nil || b.idempotencyStore == nil {
		return nil
	}
	ttl := b.idempotencyTTL
	if ttl == 0 {
		ttl = 24 * time.Hour
	}
	return b.idempotencyStore.Set(ctx, signature, exportID, ttl)
}
