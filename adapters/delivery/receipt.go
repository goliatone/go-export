package exportdelivery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/goliatone/go-export/export"
)

const (
	WebhookKeyHeader    = "Idempotency-Key"
	WebhookDigestHeader = "X-Export-Payload-SHA256"
)

// WebhookReceipt describes a durable target, without exposing its stored body or credentials.
type WebhookReceipt struct {
	ID          string    `json:"id"`
	ExportID    string    `json:"export_id"`
	TargetID    string    `json:"target_id"`
	DeliveredAt time.Time `json:"delivered_at"`
}

// WebhookTargetSpec is a stable application target identity and its authorized endpoint.
type WebhookTargetSpec struct {
	ID      string            `json:"id"`
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers,omitempty"`
}
type ScheduleWebhooksRequest struct {
	ExportID string              `json:"export_id"`
	Key      string              `json:"key"`
	Targets  []WebhookTargetSpec `json:"targets"`
	Metadata map[string]any      `json:"metadata,omitempty"`
}

// WebhookDelivery contains immutable persisted bytes. A retry MUST reuse all fields.
type WebhookDelivery struct {
	ID      string
	URL     string
	Headers map[string]string
	Body    json.RawMessage
	SHA256  string
}

// ReceiptWebhookSender succeeds only after the receiver commits a matching key/body receipt.
type ReceiptWebhookSender interface {
	SendReceipt(context.Context, WebhookDelivery) error
}

// HTTPReceiptWebhookSender implements the opt-in receipt protocol. The receiving
// application must atomically deduplicate the key and commit its effect. A 2xx
// response alone is insufficient. Redirects are disabled, including 307/308.
type HTTPReceiptWebhookSender struct{ Client *http.Client }

func PayloadDigest(body []byte) string {
	h := sha256.Sum256(body)
	return hex.EncodeToString(h[:])
}

// ValidateWebhookDelivery rejects ambiguous identities and reserved transport headers.
// Destination allowlists and credential provisioning remain host policy.
func ValidateWebhookDelivery(m WebhookDelivery) error {
	u, err := url.Parse(m.URL)
	if err != nil || u == nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" || u.User != nil || u.Fragment != "" {
		return export.NewError(export.KindValidation, "invalid webhook endpoint", nil)
	}
	if len(m.ID) != 64 || len(m.SHA256) != 64 || PayloadDigest(m.Body) != m.SHA256 || !json.Valid(m.Body) {
		return export.NewError(export.KindValidation, "invalid webhook identity or payload", nil)
	}
	if _, err := hex.DecodeString(m.ID); err != nil {
		return export.NewError(export.KindValidation, "invalid webhook identity", nil)
	}
	seen := make(map[string]bool, len(m.Headers))
	for k, v := range m.Headers {
		lower := strings.ToLower(k)
		if !validHeader(k, v) || seen[lower] {
			return export.NewError(export.KindValidation, "invalid webhook header", nil)
		}
		seen[lower] = true
		switch lower {
		case "idempotency-key", "x-export-payload-sha256", "content-type", "content-length", "host", "transfer-encoding", "connection", "trailer", "te", "upgrade":
			return export.NewError(export.KindValidation, "reserved webhook header", nil)
		}
	}
	return nil
}

func validHeader(k, v string) bool {
	if k == "" {
		return false
	}
	for _, c := range []byte(k) {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || strings.ContainsRune("!#$%&'*+-.^_`|~", rune(c))) {
			return false
		}
	}
	for _, c := range []byte(v) {
		if (c < 32 && c != '\t') || c == 127 {
			return false
		}
	}
	return true
}

func (s *HTTPReceiptWebhookSender) SendReceipt(ctx context.Context, m WebhookDelivery) error {
	if s == nil {
		return export.NewError(export.KindInternal, "webhook sender is nil", nil)
	}
	if err := ValidateWebhookDelivery(m); err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, m.URL, bytes.NewReader(m.Body))
	if err != nil {
		return export.NewError(export.KindValidation, "invalid webhook request", nil)
	}
	for k, v := range m.Headers {
		req.Header.Set(k, v)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(WebhookKeyHeader, m.ID)
	req.Header.Set(WebhookDigestHeader, m.SHA256)
	client := http.Client{Timeout: 30 * time.Second}
	if s.Client != nil {
		client = *s.Client
		if client.Timeout <= 0 {
			client.Timeout = 30 * time.Second
		}
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := client.Do(req)
	if err != nil {
		return export.NewError(export.KindExternal, "webhook acknowledgment unavailable", nil)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 || resp.Header.Get(WebhookKeyHeader) != m.ID || resp.Header.Get(WebhookDigestHeader) != m.SHA256 {
		return export.NewError(export.KindExternal, "webhook receipt not acknowledged", nil)
	}
	return nil
}
