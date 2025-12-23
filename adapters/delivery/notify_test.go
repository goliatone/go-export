package exportdelivery

import (
	"testing"
	"time"

	"github.com/goliatone/go-export/export"
)

func TestDeriveExpiresAt_UsesArtifactExpiry(t *testing.T) {
	expiry := time.Date(2025, 2, 10, 12, 0, 0, 0, time.UTC)
	meta := export.ArtifactMeta{ExpiresAt: expiry}
	now := time.Date(2025, 2, 9, 10, 0, 0, 0, time.UTC)

	got := deriveExpiresAt(meta, 15*time.Minute, now)
	if got != expiry.Format(time.RFC3339) {
		t.Fatalf("expected expiry %s, got %s", expiry.Format(time.RFC3339), got)
	}
}

func TestDeriveExpiresAt_FallsBackToTTL(t *testing.T) {
	meta := export.ArtifactMeta{}
	now := time.Date(2025, 2, 9, 10, 0, 0, 0, time.UTC)
	ttl := 45 * time.Minute

	got := deriveExpiresAt(meta, ttl, now)
	want := now.Add(ttl).Format(time.RFC3339)
	if got != want {
		t.Fatalf("expected expiry %s, got %s", want, got)
	}
}

func TestResolveNotifyAttachments_FromDelivery(t *testing.T) {
	attachment := &Attachment{
		Filename:    "report.csv",
		ContentType: "text/csv",
		Data:        []byte("data"),
		Size:        4,
	}

	got := resolveNotifyAttachments(NotificationRequest{}, attachment, 10, nil)
	if len(got) != 1 {
		t.Fatalf("expected 1 attachment, got %d", len(got))
	}
	if got[0].Filename != "report.csv" {
		t.Fatalf("expected filename report.csv, got %s", got[0].Filename)
	}
	if got[0].Size != 4 {
		t.Fatalf("expected size 4, got %d", got[0].Size)
	}
}

func TestResolveNotifyAttachments_FallbackOnLimit(t *testing.T) {
	attachment := &Attachment{
		Filename:    "report.csv",
		ContentType: "text/csv",
		Data:        []byte("data"),
		Size:        4,
	}

	got := resolveNotifyAttachments(NotificationRequest{}, attachment, 3, nil)
	if got != nil {
		t.Fatalf("expected attachments to be skipped")
	}
}
