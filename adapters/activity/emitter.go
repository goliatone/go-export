package exportactivity

import (
	"context"
	"strings"

	"github.com/goliatone/go-export/export"
	"github.com/goliatone/go-users/activity"
	"github.com/goliatone/go-users/pkg/types"
	"github.com/google/uuid"
)

// Config configures the activity emitter adapter.
type Config struct {
	Sink       types.ActivitySink
	Channel    string
	ObjectType string
}

// Emitter adapts ChangeEmitter events into go-users activity records.
type Emitter struct {
	sink       types.ActivitySink
	channel    string
	objectType string
}

// NewEmitter creates a new activity emitter.
func NewEmitter(cfg Config) *Emitter {
	channel := strings.TrimSpace(cfg.Channel)
	if channel == "" {
		channel = "export"
	}
	objectType := strings.TrimSpace(cfg.ObjectType)
	if objectType == "" {
		objectType = "export"
	}
	return &Emitter{
		sink:       cfg.Sink,
		channel:    channel,
		objectType: objectType,
	}
}

// Emit logs export lifecycle events to the configured ActivitySink.
func (e *Emitter) Emit(ctx context.Context, evt export.ChangeEvent) error {
	if e == nil {
		return export.NewError(export.KindInternal, "activity emitter is nil", nil)
	}
	if e.sink == nil {
		return export.NewError(export.KindNotImpl, "activity sink not configured", nil)
	}
	verb := strings.TrimSpace(evt.Name)
	if verb == "" {
		return export.NewError(export.KindValidation, "activity verb is required", nil)
	}
	objectID := strings.TrimSpace(evt.ExportID)
	if objectID == "" {
		return export.NewError(export.KindValidation, "activity object ID is required", nil)
	}

	meta := buildMetadata(evt)
	record, err := activity.BuildRecordFromUUID(
		parseUUID(evt.Actor.ID),
		verb,
		e.objectType,
		objectID,
		meta,
		activity.WithChannel(e.channel),
		activity.WithOccurredAt(evt.Timestamp),
		activity.WithTenant(parseUUID(evt.Actor.Scope.TenantID)),
		activity.WithOrg(parseUUID(evt.Actor.Scope.WorkspaceID)),
	)
	if err != nil {
		return err
	}
	return e.sink.Log(ctx, record)
}

func buildMetadata(evt export.ChangeEvent) map[string]any {
	meta := make(map[string]any, 4)
	if evt.Definition != "" {
		meta["definition"] = evt.Definition
	}
	if evt.Format != "" {
		meta["format"] = evt.Format
	}
	if evt.Delivery != "" {
		meta["delivery"] = evt.Delivery
	}
	for k, v := range evt.Metadata {
		meta[k] = v
	}
	return meta
}

func parseUUID(value string) uuid.UUID {
	value = strings.TrimSpace(value)
	if value == "" {
		return uuid.Nil
	}
	parsed, err := uuid.Parse(value)
	if err != nil {
		return uuid.Nil
	}
	return parsed
}
