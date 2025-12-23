package exportdelivery

import (
	"context"

	"github.com/goliatone/go-export/export"
	job "github.com/goliatone/go-job"
)

// DeliveryHandler executes delivery requests.
type DeliveryHandler interface {
	Deliver(ctx context.Context, req Request) (Result, error)
}

// TaskConfig configures the scheduled delivery task.
type TaskConfig struct {
	ID             string
	Path           string
	Config         job.Config
	HandlerOptions job.HandlerOptions
	Handler        DeliveryHandler
	Logger         export.Logger
}

// Task executes scheduled delivery jobs.
type Task struct {
	id             string
	path           string
	config         job.Config
	handlerOptions job.HandlerOptions
	handler        DeliveryHandler
	logger         export.Logger
}

// NewTask creates a new scheduled delivery task.
func NewTask(cfg TaskConfig) *Task {
	logger := cfg.Logger
	if logger == nil {
		logger = export.NopLogger{}
	}
	id := cfg.ID
	if id == "" {
		id = DefaultDeliveryTaskID
	}
	path := cfg.Path
	if path == "" {
		path = DefaultDeliveryTaskPath
	}

	return &Task{
		id:             id,
		path:           path,
		config:         cfg.Config,
		handlerOptions: cfg.HandlerOptions,
		handler:        cfg.Handler,
		logger:         logger,
	}
}

// GetID returns the task identifier.
func (t *Task) GetID() string { return t.id }

// GetHandler returns a handler for cron usage.
func (t *Task) GetHandler() func() error {
	return func() error {
		return t.Execute(context.Background(), &job.ExecutionMessage{})
	}
}

// GetHandlerConfig returns scheduler options for the task.
func (t *Task) GetHandlerConfig() job.HandlerOptions { return t.handlerOptions }

// GetConfig returns task config defaults.
func (t *Task) GetConfig() job.Config { return t.config }

// GetPath returns the task path.
func (t *Task) GetPath() string { return t.path }

// GetEngine returns nil because this task is code-driven.
func (t *Task) GetEngine() job.Engine { return nil }

// Execute runs a scheduled delivery payload.
func (t *Task) Execute(ctx context.Context, msg *job.ExecutionMessage) error {
	if t == nil {
		return export.NewError(export.KindInternal, "delivery task is nil", nil)
	}
	if t.handler == nil {
		return export.NewError(export.KindNotImpl, "delivery handler not configured", nil)
	}
	if ctx == nil {
		ctx = context.Background()
	}

	payload, err := decodePayload(msg)
	if err != nil {
		return err
	}
	_, err = t.handler.Deliver(ctx, payload.Request)
	return err
}
