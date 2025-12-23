package notify

import "context"

// ExportReadyNotifier delivers export-ready notifications.
type ExportReadyNotifier interface {
	Send(ctx context.Context, evt ExportReadyEvent) error
}

// ExportReadyEvent mirrors go-notifications OnReadyEvent, but stays in go-export.
type ExportReadyEvent struct {
	Recipients       []string
	Channels         []string
	Locale           string
	TenantID         string
	ActorID          string
	FileName         string
	Format           string
	URL              string
	ExpiresAt        string
	Rows             int
	Parts            int
	ManifestURL      string
	Message          string
	ChannelOverrides map[string]map[string]any
	Attachments      []NotificationAttachment
}

// NotificationAttachment captures file payloads for notifications.
type NotificationAttachment struct {
	Filename    string
	ContentType string
	Data        []byte
	Size        int64
}
