package exportdelivery

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/goliatone/go-export/export"
)

// WebhookMessage describes an outbound webhook call.
type WebhookMessage struct {
	URL     string
	Method  string
	Headers map[string]string
	Payload any
}

// WebhookSender delivers webhook messages.
type WebhookSender interface {
	Send(ctx context.Context, msg WebhookMessage) error
}

// HTTPWebhookSender posts JSON payloads via HTTP.
type HTTPWebhookSender struct {
	Client *http.Client
}

// Send posts the webhook payload.
func (s *HTTPWebhookSender) Send(ctx context.Context, msg WebhookMessage) error {
	if s == nil {
		return export.NewError(export.KindInternal, "webhook sender is nil", nil)
	}
	if strings.TrimSpace(msg.URL) == "" {
		return export.NewError(export.KindValidation, "webhook URL is required", nil)
	}
	method := msg.Method
	if method == "" {
		method = http.MethodPost
	}

	payload, err := json.Marshal(msg.Payload)
	if err != nil {
		return export.NewError(export.KindValidation, "webhook payload invalid", err)
	}

	req, err := http.NewRequestWithContext(ctx, method, msg.URL, bytes.NewReader(payload))
	if err != nil {
		return export.NewError(export.KindInternal, "webhook request failed", err)
	}
	if msg.Headers != nil {
		for key, value := range msg.Headers {
			if strings.TrimSpace(key) == "" {
				continue
			}
			req.Header.Set(key, value)
		}
	}
	if req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}

	client := s.Client
	if client == nil {
		client = http.DefaultClient
	}

	resp, err := client.Do(req)
	if err != nil {
		return export.NewError(export.KindExternal, "webhook request failed", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return export.NewError(export.KindExternal, "webhook response error", nil)
	}
	return nil
}

// WebhookPayload describes the webhook event body.
type WebhookPayload struct {
	ExportID   string             `json:"export_id"`
	Definition string             `json:"definition"`
	Format     export.Format      `json:"format"`
	Filename   string             `json:"filename"`
	Mode       DeliveryMode       `json:"mode"`
	Link       string             `json:"link,omitempty"`
	Attachment *WebhookAttachment `json:"attachment,omitempty"`
	Metadata   map[string]any     `json:"metadata,omitempty"`
	Actor      export.Actor       `json:"actor"`
	SentAt     time.Time          `json:"sent_at"`
}

// WebhookAttachment describes attachment payloads for webhooks.
type WebhookAttachment struct {
	Filename    string `json:"filename"`
	ContentType string `json:"content_type"`
	Size        int64  `json:"size"`
	Data        string `json:"data"`
}
