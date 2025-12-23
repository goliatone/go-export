package exportdelivery

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/goliatone/go-export/export"
)

type stubExportService struct {
	request  func(ctx context.Context, actor export.Actor, req export.ExportRequest) (export.ExportRecord, error)
	generate func(ctx context.Context, actor export.Actor, exportID string, req export.ExportRequest) (export.ExportResult, error)
	download func(ctx context.Context, actor export.Actor, exportID string) (export.DownloadInfo, error)
}

func (s *stubExportService) RequestExport(ctx context.Context, actor export.Actor, req export.ExportRequest) (export.ExportRecord, error) {
	if s.request != nil {
		return s.request(ctx, actor, req)
	}
	return export.ExportRecord{}, nil
}

func (s *stubExportService) GenerateExport(ctx context.Context, actor export.Actor, exportID string, req export.ExportRequest) (export.ExportResult, error) {
	if s.generate != nil {
		return s.generate(ctx, actor, exportID, req)
	}
	return export.ExportResult{}, nil
}

func (s *stubExportService) CancelExport(ctx context.Context, actor export.Actor, exportID string) (export.ExportRecord, error) {
	return export.ExportRecord{}, nil
}

func (s *stubExportService) DeleteExport(ctx context.Context, actor export.Actor, exportID string) error {
	return nil
}

func (s *stubExportService) Status(ctx context.Context, actor export.Actor, exportID string) (export.ExportRecord, error) {
	return export.ExportRecord{}, nil
}

func (s *stubExportService) History(ctx context.Context, actor export.Actor, filter export.ProgressFilter) ([]export.ExportRecord, error) {
	return nil, nil
}

func (s *stubExportService) DownloadMetadata(ctx context.Context, actor export.Actor, exportID string) (export.DownloadInfo, error) {
	if s.download != nil {
		return s.download(ctx, actor, exportID)
	}
	return export.DownloadInfo{}, nil
}

func (s *stubExportService) Cleanup(ctx context.Context, now time.Time) (int, error) {
	return 0, nil
}

type stubStore struct {
	objects   map[string][]byte
	meta      export.ArtifactMeta
	signedURL string
	openErr   error
}

func (s *stubStore) Put(ctx context.Context, key string, r io.Reader, meta export.ArtifactMeta) (export.ArtifactRef, error) {
	if s.objects == nil {
		s.objects = make(map[string][]byte)
	}
	buf, err := io.ReadAll(r)
	if err != nil {
		return export.ArtifactRef{}, err
	}
	s.objects[key] = buf
	s.meta = meta
	s.meta.Size = int64(len(buf))
	return export.ArtifactRef{Key: key, Meta: s.meta}, nil
}

func (s *stubStore) Open(ctx context.Context, key string) (io.ReadCloser, export.ArtifactMeta, error) {
	if s.openErr != nil {
		return nil, export.ArtifactMeta{}, s.openErr
	}
	if s.objects == nil {
		return io.NopCloser(bytes.NewReader(nil)), s.meta, nil
	}
	data := s.objects[key]
	return io.NopCloser(bytes.NewReader(data)), s.meta, nil
}

func (s *stubStore) Delete(ctx context.Context, key string) error {
	delete(s.objects, key)
	return nil
}

func (s *stubStore) SignedURL(ctx context.Context, key string, ttl time.Duration) (string, error) {
	if s.signedURL == "" {
		return "", errors.New("no url")
	}
	return s.signedURL, nil
}

type captureEmailSender struct {
	messages []EmailMessage
}

func (c *captureEmailSender) Send(ctx context.Context, msg EmailMessage) error {
	c.messages = append(c.messages, msg)
	return nil
}

type captureWebhookSender struct {
	messages []WebhookMessage
}

func (c *captureWebhookSender) Send(ctx context.Context, msg WebhookMessage) error {
	c.messages = append(c.messages, msg)
	return nil
}

func TestService_Deliver_Link(t *testing.T) {
	store := &stubStore{
		objects: map[string][]byte{
			"exports/exp-1.pdf": []byte("pdf"),
		},
		meta: export.ArtifactMeta{
			Filename:    "report.pdf",
			ContentType: "application/pdf",
			Size:        3,
		},
		signedURL: "https://download.test/exp-1.pdf",
	}

	svc := &stubExportService{
		request: func(ctx context.Context, actor export.Actor, req export.ExportRequest) (export.ExportRecord, error) {
			return export.ExportRecord{ID: "exp-1"}, nil
		},
		generate: func(ctx context.Context, actor export.Actor, exportID string, req export.ExportRequest) (export.ExportResult, error) {
			ref := export.ArtifactRef{Key: "exports/exp-1.pdf", Meta: store.meta}
			return export.ExportResult{ID: exportID, Format: req.Format, Filename: "report.pdf", Artifact: &ref}, nil
		},
	}

	email := &captureEmailSender{}
	webhook := &captureWebhookSender{}
	delivery := NewService(Config{Service: svc, Store: store, EmailSender: email, WebhookSender: webhook})

	req := Request{
		Actor: export.Actor{ID: "actor-1"},
		Export: export.ExportRequest{
			Definition: "users",
			Format:     export.FormatPDF,
		},
		Mode: DeliveryLink,
		Targets: []Target{
			{Kind: TargetEmail, Email: EmailTarget{To: []string{"demo@example.com"}}},
			{Kind: TargetWebhook, Webhook: WebhookTarget{URL: "https://hooks.test/exports"}},
		},
	}

	result, err := delivery.Deliver(context.Background(), req)
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if result.Link == "" {
		t.Fatalf("expected link")
	}
	if len(email.messages) != 1 {
		t.Fatalf("expected email message")
	}
	if !strings.Contains(email.messages[0].Body, result.Link) {
		t.Fatalf("expected link in email body")
	}
	if len(webhook.messages) != 1 {
		t.Fatalf("expected webhook message")
	}
	payload, ok := webhook.messages[0].Payload.(WebhookPayload)
	if !ok {
		t.Fatalf("expected webhook payload")
	}
	if payload.Link == "" {
		t.Fatalf("expected webhook link")
	}
	if payload.Attachment != nil {
		t.Fatalf("expected no webhook attachment")
	}
}

func TestService_Deliver_Attachment(t *testing.T) {
	store := &stubStore{
		objects: map[string][]byte{
			"exports/exp-2.pdf": []byte("pdf-data"),
		},
		meta: export.ArtifactMeta{
			Filename:    "report.pdf",
			ContentType: "application/pdf",
			Size:        8,
		},
	}

	svc := &stubExportService{
		request: func(ctx context.Context, actor export.Actor, req export.ExportRequest) (export.ExportRecord, error) {
			return export.ExportRecord{ID: "exp-2"}, nil
		},
		generate: func(ctx context.Context, actor export.Actor, exportID string, req export.ExportRequest) (export.ExportResult, error) {
			ref := export.ArtifactRef{Key: "exports/exp-2.pdf", Meta: store.meta}
			return export.ExportResult{ID: exportID, Format: req.Format, Filename: "report.pdf", Artifact: &ref}, nil
		},
	}

	email := &captureEmailSender{}
	webhook := &captureWebhookSender{}
	delivery := NewService(Config{Service: svc, Store: store, EmailSender: email, WebhookSender: webhook})

	req := Request{
		Actor: export.Actor{ID: "actor-1"},
		Export: export.ExportRequest{
			Definition: "users",
			Format:     export.FormatPDF,
		},
		Mode: DeliveryAttachment,
		Targets: []Target{
			{Kind: TargetEmail, Email: EmailTarget{To: []string{"demo@example.com"}}},
			{Kind: TargetWebhook, Webhook: WebhookTarget{URL: "https://hooks.test/exports"}},
		},
	}

	result, err := delivery.Deliver(context.Background(), req)
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if result.Attachment == nil {
		t.Fatalf("expected attachment")
	}
	if len(email.messages) != 1 {
		t.Fatalf("expected email message")
	}
	if email.messages[0].Attachment == nil {
		t.Fatalf("expected email attachment")
	}
	payload, ok := webhook.messages[0].Payload.(WebhookPayload)
	if !ok {
		t.Fatalf("expected webhook payload")
	}
	if payload.Attachment == nil || payload.Attachment.Data == "" {
		t.Fatalf("expected webhook attachment")
	}
}
