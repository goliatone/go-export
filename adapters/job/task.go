package exportjob

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"sync"
	"time"

	"github.com/goliatone/go-command/dispatcher"
	errorslib "github.com/goliatone/go-errors"
	exportcmd "github.com/goliatone/go-export/command"
	"github.com/goliatone/go-export/export"
	job "github.com/goliatone/go-job"
)

const (
	DefaultGenerateTaskID   = "export:generate"
	DefaultGenerateTaskPath = "export:generate"
)

var (
	backoffRand   = rand.New(rand.NewSource(time.Now().UnixNano()))
	backoffRandMu sync.Mutex
)

// Payload captures the job execution input.
type Payload struct {
	ExportID string               `json:"export_id"`
	Actor    export.Actor         `json:"actor"`
	Request  export.ExportRequest `json:"request"`
}

// MessageBuilderFunc builds an execution message for non-queue paths.
type MessageBuilderFunc func(ctx context.Context) (*job.ExecutionMessage, error)

// GenerateDispatch dispatches an export generation command.
type GenerateDispatch func(ctx context.Context, msg exportcmd.GenerateExport) error

// TaskConfig configures the export generation task.
type TaskConfig struct {
	ID             string
	Path           string
	Config         job.Config
	HandlerOptions job.HandlerOptions
	RetryPolicy    RetryPolicy
	CancelRegistry *CancelRegistry
	Store          export.ArtifactStore
	Logger         export.Logger
	Dispatch       GenerateDispatch
	MessageBuilder MessageBuilderFunc
}

// GenerateTask executes export generation jobs.
type GenerateTask struct {
	id             string
	path           string
	config         job.Config
	handlerOptions job.HandlerOptions
	retryPolicy    RetryPolicy
	cancelRegistry *CancelRegistry
	store          export.ArtifactStore
	logger         export.Logger
	dispatch       GenerateDispatch
	messageBuilder MessageBuilderFunc
}

// NewGenerateTask creates a new export generation task.
func NewGenerateTask(cfg TaskConfig) *GenerateTask {
	logger := cfg.Logger
	if logger == nil {
		logger = export.NopLogger{}
	}
	id := cfg.ID
	if id == "" {
		id = DefaultGenerateTaskID
	}
	path := cfg.Path
	if path == "" {
		path = DefaultGenerateTaskPath
	}
	dispatch := cfg.Dispatch
	if dispatch == nil {
		dispatch = func(ctx context.Context, msg exportcmd.GenerateExport) error {
			return dispatcher.Dispatch(ctx, msg)
		}
	}

	return &GenerateTask{
		id:             id,
		path:           path,
		config:         cfg.Config,
		handlerOptions: cfg.HandlerOptions,
		retryPolicy:    cfg.RetryPolicy,
		cancelRegistry: cfg.CancelRegistry,
		store:          cfg.Store,
		logger:         logger,
		dispatch:       dispatch,
		messageBuilder: cfg.MessageBuilder,
	}
}

// GetID returns the task identifier.
func (t *GenerateTask) GetID() string { return t.id }

// GetHandler returns a handler for non-queue execution paths.
func (t *GenerateTask) GetHandler() func() error {
	return func() error {
		if t == nil {
			return export.NewError(export.KindInternal, "task is nil", nil)
		}
		if t.messageBuilder == nil {
			return export.NewError(export.KindNotImpl, "job message builder not configured", nil)
		}

		ctx := context.Background()
		msg, err := t.messageBuilder(ctx)
		if err != nil {
			if errors.Is(err, errExecutionSkipped) {
				return nil
			}
			return err
		}
		if msg == nil {
			return export.NewError(export.KindValidation, "execution message is required", nil)
		}
		return t.Execute(ctx, msg)
	}
}

// GetHandlerConfig returns scheduler options for the task.
func (t *GenerateTask) GetHandlerConfig() job.HandlerOptions { return t.handlerOptions }

// GetConfig returns task config defaults.
func (t *GenerateTask) GetConfig() job.Config { return t.config }

// GetPath returns the task path.
func (t *GenerateTask) GetPath() string { return t.path }

// GetEngine returns nil because this task is code-driven.
func (t *GenerateTask) GetEngine() job.Engine { return nil }

// Execute runs export generation for the provided payload.
func (t *GenerateTask) Execute(ctx context.Context, msg *job.ExecutionMessage) error {
	if t == nil {
		return export.NewError(export.KindInternal, "task is nil", nil)
	}
	if ctx == nil {
		ctx = context.Background()
	}

	payload, err := decodePayload(msg)
	if err != nil {
		return err
	}
	if payload.ExportID == "" {
		return export.NewError(export.KindValidation, "export ID is required", nil)
	}

	execCtx := ctx
	if t.cancelRegistry != nil {
		var cancel context.CancelFunc
		execCtx, cancel = context.WithCancel(ctx)
		release := t.cancelRegistry.Register(payload.ExportID, cancel)
		defer release()
	}

	policy := t.retryPolicy
	attempt := 0
	for {
		if err := execCtx.Err(); err != nil {
			return err
		}

		cmd := exportcmd.GenerateExport{
			Actor:    payload.Actor,
			ExportID: payload.ExportID,
			Request:  payload.Request,
		}
		err := t.dispatch(execCtx, cmd)
		if err == nil {
			return nil
		}

		if !policy.shouldRetry(err) || attempt >= policy.MaxRetries {
			return err
		}

		if cerr := t.cleanupArtifact(execCtx, payload); cerr != nil {
			return cerr
		}

		attempt++
		delay := policy.backoffDelay(attempt)
		if delay > 0 {
			if serr := sleepWithContext(execCtx, delay); serr != nil {
				return serr
			}
		}
	}
}

func (t *GenerateTask) cleanupArtifact(ctx context.Context, payload Payload) error {
	_ = ctx
	if t.store == nil {
		return nil
	}
	key := artifactKey(payload.ExportID, payload.Request.Format)
	if key == "" {
		return nil
	}
	if err := t.store.Delete(context.Background(), key); err != nil {
		if isNotFoundError(err) {
			return nil
		}
		return err
	}
	return nil
}

func encodePayload(payload Payload) (json.RawMessage, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, export.NewError(export.KindValidation, "payload is not serializable", err)
	}
	return json.RawMessage(raw), nil
}

func decodePayload(msg *job.ExecutionMessage) (Payload, error) {
	if msg == nil || msg.Parameters == nil {
		return Payload{}, export.NewError(export.KindValidation, "job payload is required", nil)
	}

	raw, ok := msg.Parameters["payload"]
	if !ok {
		return Payload{}, export.NewError(export.KindValidation, "job payload missing", nil)
	}

	switch value := raw.(type) {
	case Payload:
		return value, nil
	case *Payload:
		if value == nil {
			return Payload{}, export.NewError(export.KindValidation, "job payload is nil", nil)
		}
		return *value, nil
	case json.RawMessage:
		return unmarshalPayload(value)
	case []byte:
		return unmarshalPayload(value)
	case string:
		return unmarshalPayload([]byte(value))
	default:
		data, err := json.Marshal(value)
		if err != nil {
			return Payload{}, export.NewError(export.KindValidation, "job payload is invalid", err)
		}
		return unmarshalPayload(data)
	}
}

func unmarshalPayload(data []byte) (Payload, error) {
	if len(data) == 0 {
		return Payload{}, export.NewError(export.KindValidation, "job payload is empty", nil)
	}
	var payload Payload
	if err := json.Unmarshal(data, &payload); err != nil {
		return Payload{}, export.NewError(export.KindValidation, "job payload is invalid", err)
	}
	return payload, nil
}

func artifactKey(exportID string, format export.Format) string {
	if exportID == "" {
		return ""
	}
	if format == "" {
		format = export.FormatCSV
	}
	return fmt.Sprintf("exports/%s.%s", exportID, format)
}

func isNotFoundError(err error) bool {
	var exportErr *export.ExportError
	if errors.As(err, &exportErr) && exportErr.Kind == export.KindNotFound {
		return true
	}
	if os.IsNotExist(err) {
		return true
	}
	var goErr *errorslib.Error
	if errors.As(err, &goErr) && goErr.Category == errorslib.CategoryNotFound {
		return true
	}
	return false
}

// RetryPolicy determines retry behavior for retryable errors.
type RetryPolicy struct {
	MaxRetries int
	Backoff    job.BackoffConfig
	Retryable  func(error) bool
}

func (p RetryPolicy) shouldRetry(err error) bool {
	if err == nil || p.MaxRetries <= 0 {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	if p.Retryable != nil {
		return p.Retryable(err)
	}
	return defaultRetryable(err)
}

func (p RetryPolicy) backoffDelay(attempt int) time.Duration {
	if attempt <= 0 {
		return 0
	}
	return computeBackoffDelay(attempt, p.Backoff)
}

func defaultRetryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if errorslib.IsRetryableError(err) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return netErr.Timeout() || netErr.Temporary()
	}
	var exportErr *export.ExportError
	if errors.As(err, &exportErr) {
		switch exportErr.Kind {
		case export.KindTimeout, export.KindInternal:
			return true
		}
	}
	return false
}

func computeBackoffDelay(attempt int, cfg job.BackoffConfig) time.Duration {
	if attempt <= 0 {
		return 0
	}

	interval := cfg.Interval
	if interval <= 0 {
		interval = 250 * time.Millisecond
	}

	maxInterval := cfg.MaxInterval
	if maxInterval <= 0 {
		maxInterval = 5 * time.Second
	}

	switch cfg.Strategy {
	case job.BackoffFixed:
		return applyJitter(interval, cfg.Jitter)
	case job.BackoffExponential:
		delay := interval
		for i := 1; i < attempt; i++ {
			delay *= 2
			if delay > maxInterval {
				delay = maxInterval
				break
			}
		}
		return applyJitter(delay, cfg.Jitter)
	default:
		return 0
	}
}

func applyJitter(delay time.Duration, jitter bool) time.Duration {
	if !jitter || delay <= 0 {
		return delay
	}
	// +/-50% jitter
	half := float64(delay) * 0.5
	backoffRandMu.Lock()
	offset := (backoffRand.Float64()*2 - 1) * half
	backoffRandMu.Unlock()
	jittered := float64(delay) + offset
	if jittered < 0 {
		return 0
	}
	return time.Duration(jittered)
}

func sleepWithContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
