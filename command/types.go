package command

import (
	"time"

	"github.com/goliatone/go-errors"
	"github.com/goliatone/go-export/export"
)

// RequestExport requests a sync or async export.
type RequestExport struct {
	Actor   export.Actor
	Request export.ExportRequest
	Result  *export.ExportRecord
}

func (RequestExport) Type() string { return "export:request" }

func (msg RequestExport) Validate() error {
	if msg.Actor.ID == "" {
		return errors.New("actor ID is required", errors.CategoryValidation).
			WithTextCode("ACTOR_REQUIRED")
	}
	if msg.Request.Definition == "" {
		return errors.New("definition is required", errors.CategoryValidation).
			WithTextCode("DEFINITION_REQUIRED")
	}
	return nil
}

// CancelExport cancels an existing export.
type CancelExport struct {
	Actor    export.Actor
	ExportID string
}

func (CancelExport) Type() string { return "export:cancel" }

func (msg CancelExport) Validate() error {
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

// DeleteExport deletes an export and its artifacts.
type DeleteExport struct {
	Actor    export.Actor
	ExportID string
}

func (DeleteExport) Type() string { return "export:delete" }

func (msg DeleteExport) Validate() error {
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

// GenerateExport runs an export generation job.
type GenerateExport struct {
	Actor    export.Actor
	ExportID string
	Request  export.ExportRequest
	Result   *export.ExportResult
}

func (GenerateExport) Type() string { return "export:generate" }

func (msg GenerateExport) Validate() error {
	if msg.Actor.ID == "" {
		return errors.New("actor ID is required", errors.CategoryValidation).
			WithTextCode("ACTOR_REQUIRED")
	}
	if msg.ExportID == "" {
		return errors.New("export ID is required", errors.CategoryValidation).
			WithTextCode("EXPORT_ID_REQUIRED")
	}
	if msg.Request.Definition == "" {
		return errors.New("definition is required", errors.CategoryValidation).
			WithTextCode("DEFINITION_REQUIRED")
	}
	return nil
}

// CleanupExports removes expired exports.
type CleanupExports struct {
	Now    time.Time
	Result *int
}

func (CleanupExports) Type() string { return "export:cleanup" }

func (CleanupExports) Validate() error { return nil }
