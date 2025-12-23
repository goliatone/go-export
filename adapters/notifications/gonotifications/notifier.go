package gonotifications

import (
	"context"

	"github.com/goliatone/go-export/export"
	"github.com/goliatone/go-export/export/notify"
	"github.com/goliatone/go-notifications/pkg/onready"
)

// Notifier adapts go-notifications OnReadyNotifier to go-export.
type Notifier struct {
	delegate onready.OnReadyNotifier
}

// NewNotifier wraps a go-notifications notifier.
func NewNotifier(delegate onready.OnReadyNotifier) *Notifier {
	return &Notifier{delegate: delegate}
}

// Send forwards the event to the underlying go-notifications notifier.
func (n *Notifier) Send(ctx context.Context, evt notify.ExportReadyEvent) error {
	if n == nil || n.delegate == nil {
		return export.NewError(export.KindNotImpl, "go-notifications notifier not configured", nil)
	}

	payload := onready.OnReadyEvent{
		Recipients:       evt.Recipients,
		Locale:           evt.Locale,
		TenantID:         evt.TenantID,
		ActorID:          evt.ActorID,
		Channels:         evt.Channels,
		FileName:         evt.FileName,
		Format:           evt.Format,
		URL:              evt.URL,
		ExpiresAt:        evt.ExpiresAt,
		Rows:             evt.Rows,
		Parts:            evt.Parts,
		ManifestURL:      evt.ManifestURL,
		Message:          evt.Message,
		ChannelOverrides: evt.ChannelOverrides,
	}

	return n.delegate.Send(ctx, payload)
}
