package gonotifications

import (
	"context"
	"testing"

	"github.com/goliatone/go-export/export/notify"
	"github.com/goliatone/go-notifications/pkg/onready"
)

type captureNotifier struct {
	event onready.OnReadyEvent
}

func (c *captureNotifier) Send(ctx context.Context, evt onready.OnReadyEvent) error {
	_ = ctx
	c.event = evt
	return nil
}

func TestNotifier_SendMapsFields(t *testing.T) {
	capture := &captureNotifier{}
	notifier := NewNotifier(capture)

	err := notifier.Send(context.Background(), notify.ExportReadyEvent{
		Recipients:  []string{"user-1"},
		Channels:    []string{"email"},
		Locale:      "en",
		TenantID:    "tenant-1",
		ActorID:     "actor-1",
		FileName:    "report.csv",
		Format:      "csv",
		URL:         "https://example.com/report.csv",
		ExpiresAt:   "2025-01-01T10:00:00Z",
		Rows:        120,
		Parts:       2,
		ManifestURL: "https://example.com/manifest.json",
		Message:     "ready",
		ChannelOverrides: map[string]map[string]any{
			"email": {"cta_label": "Download"},
		},
	})
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	if capture.event.FileName != "report.csv" {
		t.Fatalf("expected filename report.csv, got %s", capture.event.FileName)
	}
	if capture.event.TenantID != "tenant-1" {
		t.Fatalf("expected tenant tenant-1, got %s", capture.event.TenantID)
	}
}
