package exportdelivery

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestReceiptWebhookProtocol(t *testing.T) {
	body := json.RawMessage(`{"export_id":"export"}`)
	id := strings.Repeat("a", 64)
	for _, mode := range []string{"matching", "missing", "wrong-key", "wrong-body", "server-error", "redirect"} {
		t.Run(mode, func(t *testing.T) {
			var redirected atomic.Int64
			destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { redirected.Add(1); w.WriteHeader(204) }))
			defer destination.Close()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "POST" || r.Header.Get(WebhookKeyHeader) != id || r.Header.Get(WebhookDigestHeader) != PayloadDigest(body) || r.Header.Get("Content-Type") != "application/json" {
					t.Error("protocol headers missing")
				}
				key, digest := id, PayloadDigest(body)
				switch mode {
				case "missing":
					w.WriteHeader(204)
					return
				case "wrong-key":
					key = strings.Repeat("b", 64)
				case "wrong-body":
					digest = strings.Repeat("c", 64)
				case "server-error":
					http.Error(w, "secret provider diagnostics", 503)
					return
				case "redirect":
					http.Redirect(w, r, destination.URL, 307)
					return
				}
				w.Header().Set(WebhookKeyHeader, key)
				w.Header().Set(WebhookDigestHeader, digest)
				w.WriteHeader(204)
			}))
			defer server.Close()
			sender := &HTTPReceiptWebhookSender{Client: server.Client()}
			e := sender.SendReceipt(context.Background(), WebhookDelivery{ID: id, URL: server.URL, Body: body, SHA256: PayloadDigest(body)})
			if (e == nil) != (mode == "matching") {
				t.Fatalf("ack mode %s: %v", mode, e)
			}
			if e != nil && strings.Contains(e.Error(), "secret") {
				t.Fatal("provider diagnostics exposed")
			}
			if redirected.Load() != 0 {
				t.Fatal("redirect followed")
			}
		})
	}
}
func TestReceiptWebhookRejectsUnsafeIntent(t *testing.T) {
	valid := WebhookDelivery{ID: strings.Repeat("a", 64), URL: "https://example.com/webhook", Body: json.RawMessage(`{}`), SHA256: PayloadDigest([]byte(`{}`))}
	for _, headers := range []map[string]string{{"idempotency-key": "override"}, {"Host": "other"}, {"Content-Type": "text/plain"}, {"bad header": "x"}, {"X-Test": "a\x00b"}, {"Authorization": "a", "authorization": "b"}} {
		m := valid
		m.Headers = headers
		if e := ValidateWebhookDelivery(m); e == nil {
			t.Fatal("invalid headers accepted", headers)
		}
	}
	for _, endpoint := range []string{"file:///etc/passwd", "https://user:pass@example.com/", "https://example.com/#fragment", "/relative"} {
		m := valid
		m.URL = endpoint
		if e := ValidateWebhookDelivery(m); e == nil {
			t.Fatal("unsafe endpoint", endpoint)
		}
	}
	m := valid
	m.Body = json.RawMessage(`{"changed":true}`)
	if e := ValidateWebhookDelivery(m); e == nil {
		t.Fatal("corrupt payload accepted")
	}
}
