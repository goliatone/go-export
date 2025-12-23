package exportdelivery

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHTTPWebhookSender_Send(t *testing.T) {
	var gotMethod string
	var gotContentType string
	var gotPayload map[string]any

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotContentType = r.Header.Get("Content-Type")
		if err := json.NewDecoder(r.Body).Decode(&gotPayload); err != nil {
			t.Fatalf("decode: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sender := &HTTPWebhookSender{Client: srv.Client()}
	err := sender.Send(context.Background(), WebhookMessage{
		URL:     srv.URL,
		Payload: map[string]any{"event": "export"},
	})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if gotMethod != http.MethodPost {
		t.Fatalf("expected POST, got %q", gotMethod)
	}
	if gotContentType == "" {
		t.Fatalf("expected content-type")
	}
	if gotPayload["event"] != "export" {
		t.Fatalf("expected payload")
	}
}
