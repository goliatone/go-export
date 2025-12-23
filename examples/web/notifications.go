package main

import (
	"context"
	"fmt"
	"html"
	"strings"
	"time"

	exportdelivery "github.com/goliatone/go-export/adapters/delivery"
	"github.com/goliatone/go-export/adapters/notifications/gonotifications"
	"github.com/goliatone/go-export/examples/web/config"
	"github.com/goliatone/go-export/export"
	"github.com/goliatone/go-export/export/notify"
	i18n "github.com/goliatone/go-i18n"
	"github.com/goliatone/go-notifications/pkg/adapters"
	"github.com/goliatone/go-notifications/pkg/adapters/console"
	notifsmtp "github.com/goliatone/go-notifications/pkg/adapters/smtp"
	notifconfig "github.com/goliatone/go-notifications/pkg/config"
	"github.com/goliatone/go-notifications/pkg/inbox"
	"github.com/goliatone/go-notifications/pkg/interfaces/broadcaster"
	"github.com/goliatone/go-notifications/pkg/interfaces/cache"
	notiflogger "github.com/goliatone/go-notifications/pkg/interfaces/logger"
	"github.com/goliatone/go-notifications/pkg/notifier"
	"github.com/goliatone/go-notifications/pkg/onready"
	"github.com/goliatone/go-notifications/pkg/storage"
	"github.com/goliatone/go-notifications/pkg/templates"
)

const notifyLinkTTL = 30 * time.Minute
const defaultNotifyFrom = "no-reply@example.com"

func setupExportReadyNotifier(ctx context.Context, logger *SimpleLogger, cfg config.NotificationConfig) (notify.ExportReadyNotifier, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	logNotificationConfig(logger, cfg)

	store := i18n.NewStaticStore(onready.Translations())
	translator, err := i18n.NewSimpleTranslator(store, i18n.WithTranslatorDefaultLocale("en"))
	if err != nil {
		return nil, err
	}

	providers := storage.NewMemoryProviders()
	logSink := notificationsLogger{base: logger}
	tplSvc, err := templates.New(templates.Dependencies{
		Repository:    providers.Templates,
		Cache:         &cache.Nop{},
		Logger:        logSink,
		Translator:    translator,
		Fallbacks:     i18n.NewStaticFallbackResolver(),
		DefaultLocale: "en",
	})
	if err != nil {
		return nil, err
	}

	inboxSvc, err := inbox.New(inbox.Dependencies{
		Repository:  providers.Inbox,
		Broadcaster: &broadcaster.Nop{},
		Logger:      logSink,
	})
	if err != nil {
		return nil, err
	}

	regResult, err := onready.Register(ctx, onready.Dependencies{
		Definitions: providers.Definitions,
		Templates:   tplSvc,
	}, onready.Options{})
	if err != nil {
		return nil, err
	}

	registry := adapters.NewRegistry(buildNotificationAdapters(logSink, cfg)...)
	manager, err := notifier.New(notifier.Dependencies{
		Definitions: providers.Definitions,
		Events:      providers.Events,
		Messages:    providers.Messages,
		Attempts:    providers.DeliveryAttempts,
		Templates:   tplSvc,
		Adapters:    registry,
		Logger:      logSink,
		Config: notifconfig.DispatcherConfig{
			EnvFallbackAllowlist: cfg.Recipients,
		},
		Inbox: inboxSvc,
	})
	if err != nil {
		return nil, err
	}

	ready, err := onready.NewNotifier(manager, regResult.DefinitionCode)
	if err != nil {
		return nil, err
	}

	return gonotifications.NewNotifier(ready), nil
}

func logNotificationConfig(logger *SimpleLogger, cfg config.NotificationConfig) {
	if logger == nil {
		return
	}
	logger.Infof("notifications enabled recipients=%v channels=%v smtp_host=%s smtp_port=%d smtp_from=%s smtp_user=%s smtp_tls=%t smtp_starttls=%t smtp_skip_tls_verify=%t smtp_auth_disabled=%t smtp_plain_only=%t",
		cfg.Recipients,
		cfg.Channels,
		cfg.SMTP.Host,
		cfg.SMTP.Port,
		cfg.SMTP.From,
		cfg.SMTP.Username,
		cfg.SMTP.UseTLS,
		cfg.SMTP.UseStartTLS,
		cfg.SMTP.SkipTLSVerify,
		cfg.SMTP.AuthDisabled,
		cfg.SMTP.PlainOnly,
	)
}

type notifyingService struct {
	base     export.Service
	store    export.ArtifactStore
	notifier notify.ExportReadyNotifier
	cfg      config.NotificationConfig
	logger   *SimpleLogger
	linkTTL  time.Duration
	now      func() time.Time
	baseURL  string
}

func newNotifyingService(base export.Service, store export.ArtifactStore, notifier notify.ExportReadyNotifier, cfg config.NotificationConfig, logger *SimpleLogger, baseURL string) export.Service {
	if base == nil {
		return nil
	}
	return &notifyingService{
		base:     base,
		store:    store,
		notifier: notifier,
		cfg:      cfg,
		logger:   logger,
		linkTTL:  notifyLinkTTL,
		now:      time.Now,
		baseURL:  strings.TrimRight(strings.TrimSpace(baseURL), "/"),
	}
}

func (s *notifyingService) RequestExport(ctx context.Context, actor export.Actor, req export.ExportRequest) (export.ExportRecord, error) {
	record, err := s.base.RequestExport(ctx, actor, req)
	if err != nil {
		return record, err
	}
	if record.State == export.StateCompleted {
		s.notifyFromRecord(ctx, actor, record, req)
	}
	return record, nil
}

func (s *notifyingService) GenerateExport(ctx context.Context, actor export.Actor, exportID string, req export.ExportRequest) (export.ExportResult, error) {
	result, err := s.base.GenerateExport(ctx, actor, exportID, req)
	if err != nil {
		return result, err
	}
	if req.Delivery == export.DeliveryAsync {
		s.notifyFromResult(ctx, actor, req, result, exportID)
	}
	return result, nil
}

func (s *notifyingService) CancelExport(ctx context.Context, actor export.Actor, exportID string) (export.ExportRecord, error) {
	return s.base.CancelExport(ctx, actor, exportID)
}

func (s *notifyingService) DeleteExport(ctx context.Context, actor export.Actor, exportID string) error {
	return s.base.DeleteExport(ctx, actor, exportID)
}

func (s *notifyingService) Status(ctx context.Context, actor export.Actor, exportID string) (export.ExportRecord, error) {
	return s.base.Status(ctx, actor, exportID)
}

func (s *notifyingService) History(ctx context.Context, actor export.Actor, filter export.ProgressFilter) ([]export.ExportRecord, error) {
	return s.base.History(ctx, actor, filter)
}

func (s *notifyingService) DownloadMetadata(ctx context.Context, actor export.Actor, exportID string) (export.DownloadInfo, error) {
	return s.base.DownloadMetadata(ctx, actor, exportID)
}

func (s *notifyingService) Cleanup(ctx context.Context, now time.Time) (int, error) {
	return s.base.Cleanup(ctx, now)
}

func (s *notifyingService) notifyFromResult(ctx context.Context, actor export.Actor, req export.ExportRequest, result export.ExportResult, exportID string) {
	if s == nil || s.notifier == nil || s.store == nil {
		return
	}
	if len(s.cfg.Recipients) == 0 {
		return
	}
	if result.Artifact == nil || result.Artifact.Key == "" {
		s.logNotifySkip("export ready notification skipped: missing artifact")
		return
	}
	if req.Format == "" {
		req.Format = result.Format
	}
	s.sendNotification(ctx, actor, req, *result.Artifact, result.Filename, result.Rows, exportID)
}

func (s *notifyingService) notifyFromRecord(ctx context.Context, actor export.Actor, record export.ExportRecord, req export.ExportRequest) {
	if s == nil || s.notifier == nil || s.store == nil {
		return
	}
	if len(s.cfg.Recipients) == 0 {
		return
	}
	if req.Definition == "" {
		req.Definition = record.Definition
	}
	if req.Format == "" {
		req.Format = record.Format
	}
	ref := record.Artifact
	if ref.Key == "" {
		info, err := s.base.DownloadMetadata(ctx, actor, record.ID)
		if err == nil {
			ref = info.Artifact
		}
	}
	if ref.Key == "" {
		s.logNotifySkip("export ready notification skipped: no artifact metadata")
		return
	}
	s.sendNotification(ctx, actor, req, ref, record.Artifact.Meta.Filename, record.Counts.Processed, record.ID)
}

func (s *notifyingService) sendNotification(ctx context.Context, actor export.Actor, req export.ExportRequest, ref export.ArtifactRef, fallbackFilename string, rows int64, exportID string) {
	link := s.resolveDownloadLink(ctx, ref.Key, exportID)
	if link == "" {
		s.logNotifySkip("export ready notification skipped: missing download link")
		return
	}
	now := time.Now()
	if s.now != nil {
		now = s.now()
	}
	expiresAt := ref.Meta.ExpiresAt
	if expiresAt.IsZero() {
		expiresAt = now.Add(s.linkTTL)
	}
	format := req.Format
	if format == "" {
		format = export.FormatCSV
	}
	filename := resolveNotifyFilename(ref.Meta, fallbackFilename, req.Definition, format)

	evt := notify.ExportReadyEvent{
		Recipients: s.cfg.Recipients,
		Channels:   notifyChannels(s.cfg.Channels),
		Locale:     req.Locale,
		TenantID:   actor.Scope.TenantID,
		ActorID:    actor.ID,
		FileName:   filename,
		Format:     string(export.NormalizeFormat(format)),
		URL:        link,
		ExpiresAt:  expiresAt.UTC().Format(time.RFC3339),
		Rows:       notifyRows(rows),
	}
	if err := s.notifier.Send(ctx, evt); err != nil {
		s.logNotifySkip(fmt.Sprintf("export ready notification failed: %v", err))
	}
}

func (s *notifyingService) resolveDownloadLink(ctx context.Context, key, exportID string) string {
	link := ""
	if s.store != nil && strings.TrimSpace(key) != "" {
		if signed, err := s.store.SignedURL(ctx, key, s.linkTTL); err == nil {
			link = strings.TrimSpace(signed)
		}
	}
	if s.baseURL == "" {
		return link
	}
	if strings.HasPrefix(link, "http://") || strings.HasPrefix(link, "https://") {
		return link
	}
	if exportID == "" {
		return link
	}
	return fmt.Sprintf("%s/admin/exports/%s/download", s.baseURL, exportID)
}

func (s *notifyingService) logNotifySkip(message string) {
	if s.logger == nil {
		return
	}
	s.logger.Infof("%s", message)
}

func notifyChannels(channels []string) []string {
	if len(channels) == 0 {
		return []string{"email"}
	}
	return channels
}

func notifyRows(rows int64) int {
	if rows <= 0 {
		return 0
	}
	return int(rows)
}

func resolveNotifyFilename(meta export.ArtifactMeta, resultFilename, definition string, format export.Format) string {
	filename := strings.TrimSpace(meta.Filename)
	if filename == "" {
		filename = strings.TrimSpace(resultFilename)
	}
	if filename != "" {
		return filename
	}
	base := strings.TrimSpace(definition)
	if base == "" {
		base = "export"
	}
	ext := string(export.NormalizeFormat(format))
	if ext == "" {
		ext = string(export.FormatCSV)
	}
	return fmt.Sprintf("%s.%s", base, ext)
}

func buildNotificationAdapters(logSink notiflogger.Logger, cfg config.NotificationConfig) []adapters.Messenger {
	adaptersList := make([]adapters.Messenger, 0, 2)
	if strings.TrimSpace(cfg.SMTP.Host) != "" {
		from := strings.TrimSpace(cfg.SMTP.From)
		if from == "" {
			from = defaultNotifyFrom
		}
		smtpCfg := notifsmtp.Config{
			Host:          cfg.SMTP.Host,
			Port:          cfg.SMTP.Port,
			From:          from,
			Username:      cfg.SMTP.Username,
			Password:      cfg.SMTP.Password,
			UseTLS:        cfg.SMTP.UseTLS,
			UseStartTLS:   cfg.SMTP.UseStartTLS,
			SkipTLSVerify: cfg.SMTP.SkipTLSVerify,
			AuthDisabled:  cfg.SMTP.AuthDisabled,
			PlainOnly:     cfg.SMTP.PlainOnly,
		}
		smtpAdapter := notifsmtp.New(logSink, notifsmtp.WithConfig(smtpCfg))
		adaptersList = append(adaptersList, smtpDefaultsAdapter{base: smtpAdapter, from: from})
	}
	adaptersList = append(adaptersList, console.New(logSink))
	return adaptersList
}

type smtpDefaultsAdapter struct {
	base adapters.Messenger
	from string
}

func (a smtpDefaultsAdapter) Name() string { return a.base.Name() }

func (a smtpDefaultsAdapter) Capabilities() adapters.Capability { return a.base.Capabilities() }

func (a smtpDefaultsAdapter) Send(ctx context.Context, msg adapters.Message) error {
	msg.Subject = html.UnescapeString(msg.Subject)
	msg.Metadata = ensureMetaString(msg.Metadata, "from", a.from)
	if strings.TrimSpace(msg.Body) != "" {
		msg.Metadata = ensureMetaString(msg.Metadata, "html_body", msg.Body)
	}
	return a.base.Send(ctx, msg)
}

func ensureMetaString(meta map[string]any, key, value string) map[string]any {
	if strings.TrimSpace(value) == "" {
		return meta
	}
	if meta == nil {
		meta = make(map[string]any)
	}
	if metaString(meta, key) == "" {
		meta[key] = value
	}
	return meta
}

func metaString(meta map[string]any, key string) string {
	if meta == nil {
		return ""
	}
	raw, ok := meta[key]
	if !ok || raw == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(raw))
}

func (a *App) maybeSendDemoNotification(ctx context.Context) {
	if a == nil || a.Delivery == nil {
		return
	}
	if !a.Config.Export.Notifications.Enabled {
		return
	}
	recipients := a.Config.Export.Notifications.Recipients
	if len(recipients) == 0 {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	format := export.NormalizeFormat(export.Format(a.Config.Export.DefaultFormat))
	go func() {
		demoCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()

		req := exportdelivery.Request{
			Actor: a.getActor(),
			Export: export.ExportRequest{
				Definition: "users",
				Format:     format,
			},
			Mode: exportdelivery.DeliveryLink,
			Targets: []exportdelivery.Target{
				{
					Kind: exportdelivery.TargetEmail,
					Email: exportdelivery.EmailTarget{
						To: recipients,
					},
				},
			},
			Notify: exportdelivery.NotificationRequest{
				Recipients: recipients,
				Channels:   a.Config.Export.Notifications.Channels,
				Message:    "Your export is ready.",
			},
		}

		if _, err := a.Delivery.Deliver(demoCtx, req); err != nil {
			a.Logger.Errorf("demo export notification failed: %v", err)
			return
		}
		a.Logger.Infof("demo export notification sent via go-notifications")
	}()
}

type logEmailSender struct {
	logger *SimpleLogger
}

func (l logEmailSender) Send(ctx context.Context, msg exportdelivery.EmailMessage) error {
	_ = ctx
	if l.logger == nil {
		return nil
	}
	hasAttachment := msg.Attachment != nil
	l.logger.Infof("delivery email: to=%v subject=%s attachment=%t", msg.To, msg.Subject, hasAttachment)
	return nil
}

type notificationsLogger struct {
	base *SimpleLogger
}

func (l notificationsLogger) With(fields ...notiflogger.Field) notiflogger.Logger {
	_ = fields
	return l
}

func (l notificationsLogger) Debug(msg string, fields ...notiflogger.Field) {
	l.log("DEBUG", msg, fields...)
}

func (l notificationsLogger) Info(msg string, fields ...notiflogger.Field) {
	l.log("INFO", msg, fields...)
}

func (l notificationsLogger) Warn(msg string, fields ...notiflogger.Field) {
	l.log("WARN", msg, fields...)
}

func (l notificationsLogger) Error(msg string, fields ...notiflogger.Field) {
	l.log("ERROR", msg, fields...)
}

func (l notificationsLogger) log(level, msg string, fields ...notiflogger.Field) {
	if l.base == nil {
		return
	}
	l.base.Infof("[go-notifications][%s] %s%s", level, msg, formatNotifFields(fields))
}

func formatNotifFields(fields []notiflogger.Field) string {
	if len(fields) == 0 {
		return ""
	}
	parts := make([]string, 0, len(fields))
	for _, field := range fields {
		parts = append(parts, fmt.Sprintf("%s=%v", field.Key, field.Value))
	}
	return " " + strings.Join(parts, " ")
}
