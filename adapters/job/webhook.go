package exportjob

import (
	"context"
	"encoding/json"
	cmd "github.com/goliatone/go-export/command"
	"github.com/goliatone/go-export/export"
	job "github.com/goliatone/go-job"
)

const DefaultWebhookTaskID = "export:webhook"

type WebhookTaskConfig struct {
	// Resolve must authenticate current durable background identity and return
	// its trusted context. Persisted Actor is a hint, never authentication.
	Resolve  func(context.Context, *job.ExecutionMessage) (context.Context, export.Actor, error)
	Dispatch func(context.Context, cmd.DeliverWebhook) error
	Config   job.Config
}
type WebhookTask struct{ cfg WebhookTaskConfig }

func NewWebhookTask(cfg WebhookTaskConfig) (*WebhookTask, error) {
	if cfg.Resolve == nil || cfg.Dispatch == nil {
		return nil, export.NewError(export.KindValidation, "webhook task requires trusted identity resolver and command dispatch", nil)
	}
	return &WebhookTask{cfg: cfg}, nil
}
func (*WebhookTask) GetID() string                        { return DefaultWebhookTaskID }
func (*WebhookTask) GetPath() string                      { return DefaultWebhookTaskID }
func (t *WebhookTask) GetConfig() job.Config              { return t.cfg.Config }
func (*WebhookTask) GetHandlerConfig() job.HandlerOptions { return job.HandlerOptions{} }
func (*WebhookTask) GetEngine() job.Engine                { return nil }
func (*WebhookTask) GetHandler() func() error {
	return func() error {
		return export.NewError(export.KindValidation, "webhook task requires persisted message", nil)
	}
}
func (t *WebhookTask) Execute(ctx context.Context, m *job.ExecutionMessage) error {
	if t == nil || m == nil {
		return export.NewError(export.KindValidation, "webhook execution message required", nil)
	}
	var raw []byte
	var e error
	switch value := m.Parameters["payload"].(type) {
	case json.RawMessage:
		raw = value
	case []byte:
		raw = value
	case string:
		raw = []byte(value)
	default:
		raw, e = json.Marshal(value)
	}
	if e != nil {
		return export.NewError(export.KindValidation, "invalid webhook job payload", nil)
	}
	var msg cmd.DeliverWebhook
	if e = json.Unmarshal(raw, &msg); e != nil {
		return export.NewError(export.KindValidation, "invalid webhook job payload", nil)
	}
	ctx, a, e := t.cfg.Resolve(ctx, m)
	if e != nil {
		return e
	}
	if ctx == nil || a.ID == "" {
		return export.NewError(export.KindAuthz, "webhook worker identity unavailable", nil)
	}
	msg.Actor = a
	if e = msg.Validate(); e != nil {
		return e
	}
	return t.cfg.Dispatch(ctx, msg)
}
