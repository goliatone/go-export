package query

import (
	"context"

	"github.com/goliatone/go-errors"
	"github.com/goliatone/go-export/export"
)

// ExportStatusHandler returns a single export record.
type ExportStatusHandler struct {
	Service export.Service
}

func NewExportStatusHandler(svc export.Service) *ExportStatusHandler {
	return &ExportStatusHandler{Service: svc}
}

func (h *ExportStatusHandler) Query(ctx context.Context, msg ExportStatus) (export.ExportRecord, error) {
	if h == nil || h.Service == nil {
		return export.ExportRecord{}, errors.New("export service is required", errors.CategoryInternal).
			WithTextCode("SERVICE_REQUIRED")
	}
	return h.Service.Status(ctx, msg.Actor, msg.ExportID)
}

// ExportHistoryHandler returns export history.
type ExportHistoryHandler struct {
	Service export.Service
}

func NewExportHistoryHandler(svc export.Service) *ExportHistoryHandler {
	return &ExportHistoryHandler{Service: svc}
}

func (h *ExportHistoryHandler) Query(ctx context.Context, msg ExportHistory) ([]export.ExportRecord, error) {
	if h == nil || h.Service == nil {
		return nil, errors.New("export service is required", errors.CategoryInternal).
			WithTextCode("SERVICE_REQUIRED")
	}
	return h.Service.History(ctx, msg.Actor, msg.Filter)
}

// DownloadMetadataHandler returns artifact metadata.
type DownloadMetadataHandler struct {
	Service export.Service
}

func NewDownloadMetadataHandler(svc export.Service) *DownloadMetadataHandler {
	return &DownloadMetadataHandler{Service: svc}
}

func (h *DownloadMetadataHandler) Query(ctx context.Context, msg DownloadMetadata) (export.DownloadInfo, error) {
	if h == nil || h.Service == nil {
		return export.DownloadInfo{}, errors.New("export service is required", errors.CategoryInternal).
			WithTextCode("SERVICE_REQUIRED")
	}
	return h.Service.DownloadMetadata(ctx, msg.Actor, msg.ExportID)
}
