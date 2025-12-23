package exportdelivery

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/goliatone/go-export/export"
	"github.com/goliatone/go-export/export/notify"
)

const (
	DefaultLinkTTL           = 30 * time.Minute
	DefaultMaxAttachmentSize = 10 * 1024 * 1024
	DefaultMaxTargets        = 20
	DefaultMaxRecipients     = 50
)

// Limits define operational bounds for delivery.
type Limits struct {
	MaxTargets        int
	MaxRecipients     int
	MaxAttachmentSize int64
}

// Config configures delivery service behavior.
type Config struct {
	Service        export.Service
	Store          export.ArtifactStore
	EmailSender    EmailSender
	WebhookSender  WebhookSender
	Logger         export.Logger
	LinkTTL        time.Duration
	Limits         Limits
	Notifier       notify.ExportReadyNotifier
	NotifyFailHard bool
}

// Service orchestrates scheduled export generation + delivery.
type Service struct {
	service        export.Service
	store          export.ArtifactStore
	emailSender    EmailSender
	webhookSender  WebhookSender
	logger         export.Logger
	linkTTL        time.Duration
	limits         Limits
	notifier       notify.ExportReadyNotifier
	notifyFailHard bool
	now            func() time.Time
}

// NewService creates a delivery service.
func NewService(cfg Config) *Service {
	logger := cfg.Logger
	if logger == nil {
		logger = export.NopLogger{}
	}

	linkTTL := cfg.LinkTTL
	if linkTTL == 0 {
		linkTTL = DefaultLinkTTL
	}
	limits := cfg.Limits
	if limits.MaxTargets == 0 {
		limits.MaxTargets = DefaultMaxTargets
	}
	if limits.MaxRecipients == 0 {
		limits.MaxRecipients = DefaultMaxRecipients
	}
	if limits.MaxAttachmentSize == 0 {
		limits.MaxAttachmentSize = DefaultMaxAttachmentSize
	}

	return &Service{
		service:        cfg.Service,
		store:          cfg.Store,
		emailSender:    cfg.EmailSender,
		webhookSender:  cfg.WebhookSender,
		logger:         logger,
		linkTTL:        linkTTL,
		limits:         limits,
		notifier:       cfg.Notifier,
		notifyFailHard: cfg.NotifyFailHard,
		now:            time.Now,
	}
}

// Deliver generates the export and notifies delivery targets.
func (s *Service) Deliver(ctx context.Context, req Request) (Result, error) {
	if s == nil {
		return Result{}, export.NewError(export.KindInternal, "delivery service is nil", nil)
	}
	if s.service == nil {
		return Result{}, export.NewError(export.KindNotImpl, "export service not configured", nil)
	}
	if s.store == nil {
		return Result{}, export.NewError(export.KindNotImpl, "artifact store not configured", nil)
	}
	if err := s.validateRequest(req); err != nil {
		return Result{}, err
	}

	mode := req.Mode
	if mode == "" {
		mode = DeliveryLink
	}
	req.Mode = mode
	notifyRequested := s.shouldNotify(req)

	exportReq := req.Export
	exportReq.Delivery = export.DeliveryAsync
	exportReq.Output = nil

	record, err := s.service.RequestExport(ctx, req.Actor, exportReq)
	if err != nil {
		return Result{}, err
	}

	result, err := s.service.GenerateExport(ctx, req.Actor, record.ID, exportReq)
	if err != nil {
		return Result{}, err
	}

	ref, err := s.resolveArtifact(ctx, req, record.ID, result.Artifact)
	if err != nil {
		return Result{}, err
	}

	link := ""
	if mode == DeliveryLink || notifyRequested {
		link, err = s.signedURL(ctx, ref, req.LinkTTL)
		if err != nil {
			if mode == DeliveryLink || s.notifyFailHard {
				return Result{}, err
			}
			notifyRequested = false
			if s.logger != nil {
				s.logger.Errorf("export ready notification skipped: signed URL failed: %v", err)
			}
		}
	}

	var attachment *Attachment
	if mode == DeliveryAttachment {
		attachment, err = s.loadAttachment(ctx, ref)
		if err != nil {
			return Result{}, err
		}
	}

	body := buildBody(req, link, attachment)
	subject := buildSubject(req)
	if err := s.dispatchTargets(ctx, req, subject, body, link, attachment, record, ref); err != nil {
		return Result{}, err
	}
	if notifyRequested {
		if err := s.notify(ctx, req, record, result, ref, link, attachment); err != nil {
			if s.notifyFailHard {
				return Result{}, err
			}
			if s.logger != nil {
				s.logger.Errorf("export ready notification failed: %v", err)
			}
		}
	}

	return Result{
		ExportID:   record.ID,
		Definition: exportReq.Definition,
		Format:     exportReq.Format,
		Filename:   ref.Meta.Filename,
		Link:       link,
		Attachment: attachment,
		Targets:    len(req.Targets),
		SentAt:     time.Now(),
	}, nil
}

func (s *Service) validateRequest(req Request) error {
	if req.Actor.ID == "" {
		return export.NewError(export.KindValidation, "actor ID is required", nil)
	}
	if strings.TrimSpace(req.Export.Definition) == "" {
		return export.NewError(export.KindValidation, "definition is required", nil)
	}
	if req.Export.Format == "" {
		return export.NewError(export.KindValidation, "format is required", nil)
	}
	if len(req.Targets) == 0 {
		return export.NewError(export.KindValidation, "delivery targets are required", nil)
	}
	if s.limits.MaxTargets > 0 && len(req.Targets) > s.limits.MaxTargets {
		return export.NewError(export.KindValidation, "delivery targets limit exceeded", nil)
	}

	for _, target := range req.Targets {
		switch target.Kind {
		case TargetEmail:
			recipients := countRecipients(target.Email)
			if recipients == 0 {
				return export.NewError(export.KindValidation, "email recipients are required", nil)
			}
			if s.limits.MaxRecipients > 0 && recipients > s.limits.MaxRecipients {
				return export.NewError(export.KindValidation, "email recipients limit exceeded", nil)
			}
			if s.emailSender == nil {
				return export.NewError(export.KindNotImpl, "email sender not configured", nil)
			}
		case TargetWebhook:
			if strings.TrimSpace(target.Webhook.URL) == "" {
				return export.NewError(export.KindValidation, "webhook URL is required", nil)
			}
			if s.webhookSender == nil {
				return export.NewError(export.KindNotImpl, "webhook sender not configured", nil)
			}
		default:
			return export.NewError(export.KindValidation, "delivery target kind is invalid", nil)
		}
	}

	if hasNotifyRequest(req.Notify) {
		if len(req.Notify.Recipients) == 0 {
			return export.NewError(export.KindValidation, "notification recipients are required", nil)
		}
		if s.notifier == nil {
			return export.NewError(export.KindNotImpl, "notification notifier not configured", nil)
		}
	}

	return nil
}

func (s *Service) resolveArtifact(ctx context.Context, req Request, exportID string, ref *export.ArtifactRef) (export.ArtifactRef, error) {
	if ref != nil && ref.Key != "" {
		return *ref, nil
	}
	info, err := s.service.DownloadMetadata(ctx, req.Actor, exportID)
	if err != nil {
		return export.ArtifactRef{}, err
	}
	return info.Artifact, nil
}

func (s *Service) signedURL(ctx context.Context, ref export.ArtifactRef, ttl time.Duration) (string, error) {
	ttl = s.resolveLinkTTL(ttl)
	return s.store.SignedURL(ctx, ref.Key, ttl)
}

func (s *Service) loadAttachment(ctx context.Context, ref export.ArtifactRef) (*Attachment, error) {
	limit := s.limits.MaxAttachmentSize
	if limit > 0 && ref.Meta.Size > limit {
		return nil, export.NewError(export.KindValidation, "attachment size exceeds limit", nil)
	}

	reader, meta, err := s.store.Open(ctx, ref.Key)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	data, err := readWithLimit(reader, limit)
	if err != nil {
		return nil, err
	}

	return &Attachment{
		Filename:    meta.Filename,
		ContentType: meta.ContentType,
		Data:        data,
		Size:        int64(len(data)),
	}, nil
}

func readWithLimit(r io.Reader, limit int64) ([]byte, error) {
	if limit <= 0 {
		return io.ReadAll(r)
	}
	buf, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(buf)) > limit {
		return nil, export.NewError(export.KindValidation, "attachment size exceeds limit", nil)
	}
	return buf, nil
}

func countRecipients(target EmailTarget) int {
	return len(target.To) + len(target.Cc) + len(target.Bcc)
}

func buildSubject(req Request) string {
	if strings.TrimSpace(req.Message.Subject) != "" {
		return req.Message.Subject
	}
	return fmt.Sprintf("Export ready: %s", req.Export.Definition)
}

func buildBody(req Request, link string, attachment *Attachment) string {
	body := strings.TrimSpace(req.Message.Body)
	if body == "" {
		body = fmt.Sprintf("Your %s export is ready.", req.Export.Definition)
	}
	if link != "" {
		body = strings.TrimSpace(body + "\n\nDownload: " + link)
	}
	if attachment != nil {
		body = strings.TrimSpace(body + "\n\nAttachment: " + attachment.Filename)
	}
	return body
}

func (s *Service) dispatchTargets(ctx context.Context, req Request, subject, body, link string, attachment *Attachment, record export.ExportRecord, ref export.ArtifactRef) error {
	var errs []error
	for _, target := range req.Targets {
		switch target.Kind {
		case TargetEmail:
			if err := s.sendEmail(ctx, req, target, subject, body, attachment); err != nil {
				errs = append(errs, err)
			}
		case TargetWebhook:
			if err := s.sendWebhook(ctx, req, target, link, attachment, record, ref); err != nil {
				errs = append(errs, err)
			}
		}
	}
	if len(errs) == 1 {
		return errs[0]
	}
	if len(errs) > 1 {
		return errors.Join(errs...)
	}
	return nil
}

func (s *Service) sendEmail(ctx context.Context, req Request, target Target, subject, body string, attachment *Attachment) error {
	if s.emailSender == nil {
		return export.NewError(export.KindNotImpl, "email sender not configured", nil)
	}
	msg := EmailMessage{
		To:         target.Email.To,
		Cc:         target.Email.Cc,
		Bcc:        target.Email.Bcc,
		ReplyTo:    target.Email.ReplyTo,
		Subject:    subject,
		Body:       body,
		Attachment: attachment,
	}
	return s.emailSender.Send(ctx, msg)
}

func (s *Service) sendWebhook(ctx context.Context, req Request, target Target, link string, attachment *Attachment, record export.ExportRecord, ref export.ArtifactRef) error {
	if s.webhookSender == nil {
		return export.NewError(export.KindNotImpl, "webhook sender not configured", nil)
	}

	payload := WebhookPayload{
		ExportID:   record.ID,
		Definition: req.Export.Definition,
		Format:     req.Export.Format,
		Filename:   ref.Meta.Filename,
		Mode:       req.Mode,
		Link:       link,
		Metadata:   req.Metadata,
		Actor:      req.Actor,
		SentAt:     time.Now(),
	}
	if attachment != nil {
		payload.Attachment = &WebhookAttachment{
			Filename:    attachment.Filename,
			ContentType: attachment.ContentType,
			Size:        attachment.Size,
			Data:        base64.StdEncoding.EncodeToString(attachment.Data),
		}
	}

	return s.webhookSender.Send(ctx, WebhookMessage{
		URL:     target.Webhook.URL,
		Method:  target.Webhook.Method,
		Headers: target.Webhook.Headers,
		Payload: payload,
	})
}
