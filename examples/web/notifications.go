package main

import (
	"context"
	"errors"
	"fmt"
	"html"
	"os"
	"sort"
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
	"github.com/goliatone/go-notifications/pkg/domain"
	"github.com/goliatone/go-notifications/pkg/inbox"
	"github.com/goliatone/go-notifications/pkg/interfaces/broadcaster"
	"github.com/goliatone/go-notifications/pkg/interfaces/cache"
	notiflogger "github.com/goliatone/go-notifications/pkg/interfaces/logger"
	"github.com/goliatone/go-notifications/pkg/interfaces/store"
	"github.com/goliatone/go-notifications/pkg/notifier"
	"github.com/goliatone/go-notifications/pkg/onready"
	"github.com/goliatone/go-notifications/pkg/storage"
	"github.com/goliatone/go-notifications/pkg/templates"
)

const notifyLinkTTL = 30 * time.Minute
const defaultNotifyFrom = "no-reply@example.com"

type notificationSetup struct {
	Notifier notify.ExportReadyNotifier
	Inbox    *inbox.Service
}

func setupExportReadyNotifier(ctx context.Context, logger *SimpleLogger, cfg config.NotificationConfig, realTime broadcaster.Broadcaster) (notificationSetup, error) {
	if !cfg.Enabled {
		return notificationSetup{}, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	logNotificationConfig(logger, cfg)

	store := i18n.NewStaticStore(onready.Translations())
	translator, err := i18n.NewSimpleTranslator(store, i18n.WithTranslatorDefaultLocale("en"))
	if err != nil {
		return notificationSetup{}, err
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
		return notificationSetup{}, err
	}

	inboxBroadcaster := realTime
	if inboxBroadcaster == nil {
		inboxBroadcaster = &broadcaster.Nop{}
	}
	inboxSvc, err := inbox.New(inbox.Dependencies{
		Repository:  providers.Inbox,
		Broadcaster: inboxBroadcaster,
		Logger:      logSink,
	})
	if err != nil {
		return notificationSetup{}, err
	}

	regResult, err := onready.Register(ctx, onready.Dependencies{
		Definitions: providers.Definitions,
		Templates:   tplSvc,
	}, onready.Options{})
	if err != nil {
		return notificationSetup{}, err
	}

	if err := ensureInboxAssets(ctx, providers.Definitions, tplSvc, regResult); err != nil {
		return notificationSetup{}, err
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
		return notificationSetup{}, err
	}

	ready, err := onready.NewNotifier(manager, regResult.DefinitionCode)
	if err != nil {
		return notificationSetup{}, err
	}

	return notificationSetup{
		Notifier: gonotifications.NewNotifier(ready),
		Inbox:    inboxSvc,
	}, nil
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

func ensureInboxAssets(ctx context.Context, defs store.NotificationDefinitionRepository, tplSvc *templates.Service, reg onready.Result) error {
	if defs == nil || tplSvc == nil {
		return nil
	}
	def, err := defs.GetByCode(ctx, reg.DefinitionCode)
	if err != nil {
		return err
	}
	if def == nil {
		return errors.New("notifications: missing definition for inbox")
	}
	inboxCode := strings.TrimSpace(reg.DefinitionCode) + ".inbox"
	if strings.TrimSpace(inboxCode) == "" {
		return errors.New("notifications: invalid inbox template code")
	}

	baseTpl, err := tplSvc.Get(ctx, reg.InAppCode, "in-app", "en")
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			return err
		}
		baseTpl = findInAppTemplate(onready.Templates())
	}
	if baseTpl == nil {
		return errors.New("notifications: missing in-app template for inbox")
	}
	if err := upsertInboxTemplate(ctx, tplSvc, inboxCode, baseTpl); err != nil {
		return err
	}

	updated := false
	if next, changed := appendUniqueString(def.Channels, "inbox"); changed {
		def.Channels = next
		updated = true
	}
	templateKey := "inbox:" + inboxCode
	if next, changed := appendUniqueString(def.TemplateKeys, templateKey); changed {
		def.TemplateKeys = next
		updated = true
	}
	if !updated {
		return nil
	}
	return defs.Update(ctx, def)
}

func upsertInboxTemplate(ctx context.Context, tplSvc *templates.Service, code string, base *domain.NotificationTemplate) error {
	if tplSvc == nil || base == nil {
		return nil
	}
	locale := strings.TrimSpace(base.Locale)
	if locale == "" {
		locale = "en"
	}
	input := templates.TemplateInput{
		Code:        code,
		Channel:     "inbox",
		Locale:      locale,
		Subject:     base.Subject,
		Body:        base.Body,
		Description: "Inbox template for export-ready notifications",
		Format:      base.Format,
		Schema:      base.Schema,
		Metadata:    cloneJSONMap(base.Metadata),
	}
	if _, err := tplSvc.Get(ctx, code, "inbox", locale); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			_, err = tplSvc.Create(ctx, input)
			return err
		}
		return err
	}
	_, err := tplSvc.Update(ctx, input)
	return err
}

func findInAppTemplate(tpls []domain.NotificationTemplate) *domain.NotificationTemplate {
	for _, tpl := range tpls {
		if strings.EqualFold(tpl.Channel, "in-app") {
			copyTpl := tpl
			return &copyTpl
		}
	}
	return nil
}

func appendUniqueString(values []string, value string) ([]string, bool) {
	candidate := strings.TrimSpace(value)
	if candidate == "" {
		return values, false
	}
	for _, entry := range values {
		if strings.EqualFold(strings.TrimSpace(entry), candidate) {
			return values, false
		}
	}
	return append(values, candidate), true
}

func cloneJSONMap(src domain.JSONMap) domain.JSONMap {
	if len(src) == 0 {
		return nil
	}
	out := make(domain.JSONMap, len(src))
	for key, value := range src {
		out[key] = value
	}
	return out
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
	recipients := s.resolveRecipients(actor)
	if len(recipients) == 0 {
		s.logNotifySkip("export ready notification skipped: missing recipients")
		return
	}
	if result.Artifact == nil || result.Artifact.Key == "" {
		s.logNotifySkip("export ready notification skipped: missing artifact")
		return
	}
	if req.Format == "" {
		req.Format = result.Format
	}
	s.sendNotification(ctx, actor, recipients, req, *result.Artifact, result.Filename, result.Rows, exportID)
}

func (s *notifyingService) notifyFromRecord(ctx context.Context, actor export.Actor, record export.ExportRecord, req export.ExportRequest) {
	if s == nil || s.notifier == nil || s.store == nil {
		return
	}
	recipients := s.resolveRecipients(actor)
	if len(recipients) == 0 {
		s.logNotifySkip("export ready notification skipped: missing recipients")
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
	s.sendNotification(ctx, actor, recipients, req, ref, record.Artifact.Meta.Filename, record.Counts.Processed, record.ID)
}

func (s *notifyingService) sendNotification(ctx context.Context, actor export.Actor, recipients []string, req export.ExportRequest, ref export.ArtifactRef, fallbackFilename string, rows int64, exportID string) {
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
	formatLabel := string(export.NormalizeFormat(format))
	expiresLabel := expiresAt.UTC().Format(time.RFC3339)
	rowCount := notifyRows(rows)

	evt := notify.ExportReadyEvent{
		Recipients: recipients,
		Channels:   notifyChannels(s.cfg.Channels),
		Locale:     req.Locale,
		TenantID:   actor.Scope.TenantID,
		ActorID:    actor.ID,
		FileName:   filename,
		Format:     formatLabel,
		URL:        link,
		ExpiresAt:  expiresLabel,
		Rows:       rowCount,
	}
	htmlBody := buildExportReadyHTML(filename, formatLabel, link, expiresLabel, rowCount)
	textBody := buildExportReadyText(filename, formatLabel, link, expiresLabel, rowCount)
	if htmlBody != "" || textBody != "" {
		evt.ChannelOverrides = map[string]map[string]any{
			"email": {
				"html_body": htmlBody,
				"text_body": textBody,
			},
		}
	}
	if err := s.notifier.Send(ctx, evt); err != nil {
		s.logNotifySkip(fmt.Sprintf("export ready notification failed: %v", err))
	}
}

func (s *notifyingService) resolveRecipients(actor export.Actor) []string {
	if s == nil {
		return nil
	}
	if len(s.cfg.Recipients) > 0 {
		return s.cfg.Recipients
	}
	if strings.TrimSpace(actor.ID) == "" {
		return nil
	}
	actorID := strings.TrimSpace(actor.ID)
	channels := notifyChannels(s.cfg.Channels)
	if hasNotifyChannel(channels, "email") && !strings.Contains(actorID, "@") {
		return []string{actorID + "@example.com"}
	}
	return []string{actorID}
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
		return []string{"email", "inbox"}
	}
	return channels
}

func hasNotifyChannel(channels []string, target string) bool {
	for _, channel := range channels {
		if strings.EqualFold(strings.TrimSpace(channel), target) {
			return true
		}
	}
	return false
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

func buildExportReadyHTML(filename, format, url, expires string, rows int) string {
	if filename == "" && url == "" && expires == "" && rows == 0 {
		return ""
	}
	var sb strings.Builder
	sb.WriteString("<p>")
	sb.WriteString(fmt.Sprintf(
		"Your export &quot;%s&quot; (%s) is ready to download.",
		html.EscapeString(filename),
		html.EscapeString(format),
	))
	sb.WriteString("</p>")
	if url != "" {
		sb.WriteString(fmt.Sprintf(
			"<p><a href=\"%s\">Download</a></p>",
			html.EscapeString(url),
		))
	}
	if expires != "" {
		sb.WriteString(fmt.Sprintf(
			"<p>Link expires at %s</p>",
			html.EscapeString(expires),
		))
	}
	if rows > 0 {
		sb.WriteString(fmt.Sprintf("<p>Rows: %d</p>", rows))
	}
	return sb.String()
}

func buildExportReadyText(filename, format, url, expires string, rows int) string {
	if filename == "" && url == "" && expires == "" && rows == 0 {
		return ""
	}
	lines := []string{
		fmt.Sprintf("Your export %q (%s) is ready to download.", filename, format),
	}
	if url != "" {
		lines = append(lines, "Download: "+url)
	}
	if expires != "" {
		lines = append(lines, "Link expires at "+expires)
	}
	if rows > 0 {
		lines = append(lines, fmt.Sprintf("Rows: %d", rows))
	}
	return strings.Join(lines, "\n")
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
	base   *SimpleLogger
	fields map[string]any
}

func (l notificationsLogger) WithFields(fields map[string]any) notiflogger.Logger {
	if len(fields) == 0 {
		return l
	}
	next := notificationsLogger{base: l.base, fields: make(map[string]any, len(l.fields)+len(fields))}
	for k, v := range l.fields {
		next.fields[k] = v
	}
	for k, v := range fields {
		next.fields[k] = v
	}
	return next
}

func (l notificationsLogger) WithContext(ctx context.Context) notiflogger.Logger {
	_ = ctx
	return l
}

func (l notificationsLogger) Trace(msg string, args ...any) { l.log("TRACE", msg, args...) }
func (l notificationsLogger) Debug(msg string, args ...any) { l.log("DEBUG", msg, args...) }
func (l notificationsLogger) Info(msg string, args ...any)  { l.log("INFO", msg, args...) }
func (l notificationsLogger) Warn(msg string, args ...any)  { l.log("WARN", msg, args...) }
func (l notificationsLogger) Error(msg string, args ...any) { l.log("ERROR", msg, args...) }

func (l notificationsLogger) Fatal(msg string, args ...any) {
	l.log("FATAL", msg, args...)
	os.Exit(1)
}

func (l notificationsLogger) log(level, msg string, args ...any) {
	if l.base == nil {
		return
	}
	allArgs := append(fieldArgs(l.fields), args...)
	l.base.Infof("[go-notifications][%s] %s%s", level, msg, formatArgs(allArgs))
}

func fieldArgs(fields map[string]any) []any {
	if len(fields) == 0 {
		return nil
	}
	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	args := make([]any, 0, len(keys)*2)
	for _, k := range keys {
		args = append(args, k, fields[k])
	}
	return args
}

func formatArgs(args []any) string {
	if len(args) == 0 {
		return ""
	}
	parts := make([]string, 0, len(args))
	for i := 0; i < len(args); {
		if key, ok := args[i].(string); ok && i+1 < len(args) {
			parts = append(parts, fmt.Sprintf("%s=%v", key, args[i+1]))
			i += 2
			continue
		}
		parts = append(parts, fmt.Sprint(args[i]))
		i++
	}
	return " " + strings.Join(parts, " ")
}
