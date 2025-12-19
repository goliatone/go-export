package query

import (
	"github.com/goliatone/go-errors"
	"github.com/goliatone/go-export/export"
)

// ExportStatus requests an export status record.
type ExportStatus struct {
	Actor    export.Actor
	ExportID string
}

func (ExportStatus) Type() string { return "export:status" }

func (msg ExportStatus) Validate() error {
	if msg.Actor.ID == "" {
		return errors.New("actor ID is required", errors.CategoryValidation).
			WithTextCode("ACTOR_REQUIRED")
	}
	if msg.ExportID == "" {
		return errors.New("export ID is required", errors.CategoryValidation).
			WithTextCode("EXPORT_ID_REQUIRED")
	}
	return nil
}

// ExportHistory requests export history.
type ExportHistory struct {
	Actor  export.Actor
	Filter export.ProgressFilter
}

func (ExportHistory) Type() string { return "export:history" }

func (msg ExportHistory) Validate() error {
	if msg.Actor.ID == "" {
		return errors.New("actor ID is required", errors.CategoryValidation).
			WithTextCode("ACTOR_REQUIRED")
	}
	return nil
}

// DownloadMetadata requests download metadata.
type DownloadMetadata struct {
	Actor    export.Actor
	ExportID string
}

func (DownloadMetadata) Type() string { return "export:download" }

func (msg DownloadMetadata) Validate() error {
	if msg.Actor.ID == "" {
		return errors.New("actor ID is required", errors.CategoryValidation).
			WithTextCode("ACTOR_REQUIRED")
	}
	if msg.ExportID == "" {
		return errors.New("export ID is required", errors.CategoryValidation).
			WithTextCode("EXPORT_ID_REQUIRED")
	}
	return nil
}
