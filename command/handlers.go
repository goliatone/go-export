package command

import (
	"context"
	"time"

	gcmd "github.com/goliatone/go-command"
	"github.com/goliatone/go-errors"
	"github.com/goliatone/go-export/export"
)

// RequestExportHandler handles export requests.
type RequestExportHandler struct {
	Service export.Service
}

func NewRequestExportHandler(svc export.Service) *RequestExportHandler {
	return &RequestExportHandler{Service: svc}
}

func (h *RequestExportHandler) Execute(ctx context.Context, msg RequestExport) error {
	if h == nil || h.Service == nil {
		return errors.New("export service is required", errors.CategoryInternal).
			WithTextCode("SERVICE_REQUIRED")
	}
	record, err := h.Service.RequestExport(ctx, msg.Actor, msg.Request)
	if err != nil {
		return err
	}
	if msg.Result != nil {
		*msg.Result = record
	}
	if res := gcmd.ResultFromContext[export.ExportRecord](ctx); res != nil {
		res.Store(record)
	}
	return nil
}

// CancelExportHandler cancels an export.
type CancelExportHandler struct {
	Service export.Service
}

func NewCancelExportHandler(svc export.Service) *CancelExportHandler {
	return &CancelExportHandler{Service: svc}
}

func (h *CancelExportHandler) Execute(ctx context.Context, msg CancelExport) error {
	if h == nil || h.Service == nil {
		return errors.New("export service is required", errors.CategoryInternal).
			WithTextCode("SERVICE_REQUIRED")
	}
	_, err := h.Service.CancelExport(ctx, msg.Actor, msg.ExportID)
	return err
}

// DeleteExportHandler deletes an export.
type DeleteExportHandler struct {
	Service export.Service
}

func NewDeleteExportHandler(svc export.Service) *DeleteExportHandler {
	return &DeleteExportHandler{Service: svc}
}

func (h *DeleteExportHandler) Execute(ctx context.Context, msg DeleteExport) error {
	if h == nil || h.Service == nil {
		return errors.New("export service is required", errors.CategoryInternal).
			WithTextCode("SERVICE_REQUIRED")
	}
	return h.Service.DeleteExport(ctx, msg.Actor, msg.ExportID)
}

// GenerateExportHandler runs export generation jobs.
type GenerateExportHandler struct {
	Service export.Service
}

func NewGenerateExportHandler(svc export.Service) *GenerateExportHandler {
	return &GenerateExportHandler{Service: svc}
}

func (h *GenerateExportHandler) Execute(ctx context.Context, msg GenerateExport) error {
	if h == nil || h.Service == nil {
		return errors.New("export service is required", errors.CategoryInternal).
			WithTextCode("SERVICE_REQUIRED")
	}
	result, err := h.Service.GenerateExport(ctx, msg.Actor, msg.ExportID, msg.Request)
	if err != nil {
		return err
	}
	if msg.Result != nil {
		*msg.Result = result
	}
	if res := gcmd.ResultFromContext[export.ExportResult](ctx); res != nil {
		res.Store(result)
	}
	return nil
}

// CleanupExportsHandler removes expired exports.
type CleanupExportsHandler struct {
	Service export.Service
	Config  gcmd.HandlerConfig
	Clock   func() time.Time
}

func NewCleanupExportsHandler(svc export.Service) *CleanupExportsHandler {
	return &CleanupExportsHandler{Service: svc}
}

func (h *CleanupExportsHandler) Execute(ctx context.Context, msg CleanupExports) error {
	if h == nil || h.Service == nil {
		return errors.New("export service is required", errors.CategoryInternal).
			WithTextCode("SERVICE_REQUIRED")
	}
	now := msg.Now
	if now.IsZero() && h.Clock != nil {
		now = h.Clock()
	}
	count, err := h.Service.Cleanup(ctx, now)
	if err != nil {
		return err
	}
	if msg.Result != nil {
		*msg.Result = count
	}
	if res := gcmd.ResultFromContext[int](ctx); res != nil {
		res.Store(count)
	}
	return nil
}

func (h *CleanupExportsHandler) CronHandler() func() error {
	return func() error {
		return h.Execute(context.Background(), CleanupExports{})
	}
}

func (h *CleanupExportsHandler) CronOptions() gcmd.HandlerConfig {
	return h.Config
}
