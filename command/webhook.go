package command

import (
	"context"
	gcmd "github.com/goliatone/go-command"
	delivery "github.com/goliatone/go-export/adapters/delivery"
	"github.com/goliatone/go-export/export"
)

const DeliverWebhookCommand = "export.webhook.deliver"
const ScheduleWebhooksCommand = "export.webhook.schedule"

type ScheduleWebhooks struct {
	Actor   export.Actor                     `json:"actor"`
	Request delivery.ScheduleWebhooksRequest `json:"request"`
	Result  *[]delivery.WebhookReceipt       `json:"-"`
}

func (ScheduleWebhooks) Type() string { return ScheduleWebhooksCommand }
func (m ScheduleWebhooks) Validate() error {
	if m.Actor.ID == "" || m.Request.ExportID == "" || m.Request.Key == "" {
		return export.NewError(export.KindValidation, "webhook actor, export and key are required", nil)
	}
	return nil
}

type WebhookScheduleService interface {
	Schedule(context.Context, export.Actor, delivery.ScheduleWebhooksRequest) ([]delivery.WebhookReceipt, error)
}
type ScheduleWebhooksHandler struct{ Service WebhookScheduleService }

func (h *ScheduleWebhooksHandler) Run(ctx context.Context, m ScheduleWebhooks) ([]delivery.WebhookReceipt, error) {
	if e := m.Validate(); e != nil {
		return nil, e
	}
	if h == nil || h.Service == nil {
		return nil, export.NewError(export.KindInternal, "webhook schedule service required", nil)
	}
	return h.Service.Schedule(ctx, m.Actor, m.Request)
}
func (h *ScheduleWebhooksHandler) Execute(ctx context.Context, m ScheduleWebhooks) error {
	r, e := h.Run(ctx, m)
	if e != nil {
		return e
	}
	if m.Result != nil {
		*m.Result = r
	}
	if result := gcmd.ResultFromContext[[]delivery.WebhookReceipt](ctx); result != nil {
		result.Store(r)
	}
	return nil
}

// DeliverWebhook identifies persisted intent. Actor claims must be reconstructed
// from trusted host policy by the transport/worker before command dispatch.
type DeliverWebhook struct {
	Actor     export.Actor `json:"actor"`
	ExportID  string       `json:"export_id"`
	ReceiptID string       `json:"receipt_id"`
}

func (DeliverWebhook) Type() string { return DeliverWebhookCommand }
func (m DeliverWebhook) Validate() error {
	if m.Actor.ID == "" || m.ExportID == "" || m.ReceiptID == "" {
		return export.NewError(export.KindValidation, "webhook actor, export and receipt are required", nil)
	}
	return nil
}

type WebhookDeliveryService interface {
	DeliverTarget(context.Context, export.Actor, string, string) error
}
type DeliverWebhookHandler struct{ Service WebhookDeliveryService }

func (h *DeliverWebhookHandler) Execute(ctx context.Context, m DeliverWebhook) error {
	if err := m.Validate(); err != nil {
		return err
	}
	if h == nil || h.Service == nil {
		return export.NewError(export.KindInternal, "webhook delivery service required", nil)
	}
	return h.Service.DeliverTarget(ctx, m.Actor, m.ExportID, m.ReceiptID)
}
