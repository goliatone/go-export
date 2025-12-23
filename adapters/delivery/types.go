package exportdelivery

import (
	"time"

	"github.com/goliatone/go-export/export"
	"github.com/goliatone/go-export/export/notify"
)

// DeliveryMode controls how artifacts are delivered.
type DeliveryMode string

const (
	DeliveryLink       DeliveryMode = "link"
	DeliveryAttachment DeliveryMode = "attachment"
)

// TargetKind identifies delivery destination type.
type TargetKind string

const (
	TargetEmail   TargetKind = "email"
	TargetWebhook TargetKind = "webhook"
)

// Message describes optional subject/body overrides for notifications.
type Message struct {
	Subject string `json:"subject,omitempty"`
	Body    string `json:"body,omitempty"`
}

// EmailTarget configures email delivery.
type EmailTarget struct {
	To      []string `json:"to"`
	Cc      []string `json:"cc,omitempty"`
	Bcc     []string `json:"bcc,omitempty"`
	ReplyTo string   `json:"reply_to,omitempty"`
}

// WebhookTarget configures webhook delivery.
type WebhookTarget struct {
	URL     string            `json:"url"`
	Method  string            `json:"method,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
}

// Target defines a destination for export delivery.
type Target struct {
	Kind    TargetKind    `json:"kind"`
	Email   EmailTarget   `json:"email,omitempty"`
	Webhook WebhookTarget `json:"webhook,omitempty"`
}

// Request describes a scheduled delivery request.
type Request struct {
	Actor    export.Actor         `json:"actor"`
	Export   export.ExportRequest `json:"export"`
	Targets  []Target             `json:"targets"`
	Mode     DeliveryMode         `json:"mode"`
	LinkTTL  time.Duration        `json:"link_ttl,omitempty"`
	Message  Message              `json:"message,omitempty"`
	Notify   NotificationRequest  `json:"notify,omitempty"`
	Metadata map[string]any       `json:"metadata,omitempty"`
}

// NotificationRequest configures export-ready notifications.
type NotificationRequest struct {
	Recipients       []string                        `json:"recipients,omitempty"`
	Channels         []string                        `json:"channels,omitempty"`
	Message          string                          `json:"message,omitempty"`
	Parts            int                             `json:"parts,omitempty"`
	ManifestURL      string                          `json:"manifest_url,omitempty"`
	ChannelOverrides map[string]map[string]any       `json:"channel_overrides,omitempty"`
	Attachments      []notify.NotificationAttachment `json:"attachments,omitempty"`
}

// Attachment captures file data for delivery.
type Attachment struct {
	Filename    string
	ContentType string
	Data        []byte
	Size        int64
}

// Result describes the outcome of a delivery request.
type Result struct {
	ExportID   string
	Definition string
	Format     export.Format
	Filename   string
	Link       string
	Attachment *Attachment
	Targets    int
	SentAt     time.Time
}
