package exportdelivery

import (
	"context"
	"fmt"
	"html"
	"strings"
	"time"

	"github.com/goliatone/go-export/export"
	"github.com/goliatone/go-export/export/notify"
)

func (s *Service) resolveLinkTTL(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		ttl = s.linkTTL
	}
	if ttl <= 0 {
		ttl = DefaultLinkTTL
	}
	return ttl
}

func (s *Service) shouldNotify(req Request) bool {
	if s == nil || s.notifier == nil {
		return false
	}
	return hasNotifyRequest(req.Notify)
}

func hasNotifyRequest(req NotificationRequest) bool {
	if len(req.Recipients) > 0 {
		return true
	}
	if len(req.Channels) > 0 {
		return true
	}
	if strings.TrimSpace(req.Message) != "" {
		return true
	}
	if req.Parts > 0 || strings.TrimSpace(req.ManifestURL) != "" {
		return true
	}
	if len(req.ChannelOverrides) > 0 {
		return true
	}
	if len(req.Attachments) > 0 {
		return true
	}
	return false
}

func (s *Service) notify(ctx context.Context, req Request, record export.ExportRecord, result export.ExportResult, ref export.ArtifactRef, link string, attachment *Attachment) error {
	if s == nil || s.notifier == nil {
		return nil
	}
	if !hasNotifyRequest(req.Notify) {
		return nil
	}
	if strings.TrimSpace(link) == "" {
		return export.NewError(export.KindValidation, "notification link is required", nil)
	}
	ttl := s.resolveLinkTTL(req.LinkTTL)
	now := time.Now()
	if s.now != nil {
		now = s.now()
	}

	filename := resolveNotifyFilename(ref.Meta, result.Filename, req.Export.Definition, req.Export.Format)
	format := resolveNotifyFormat(req.Export.Format, result.Format)
	expiresAt := deriveExpiresAt(ref.Meta, ttl, now)
	rows := notifyRowCount(result.Rows)
	channelOverrides := ensureNotifyEmailOverrides(
		req.Notify.ChannelOverrides,
		buildNotifyHTML(filename, format, link, expiresAt, rows, req.Notify.Message),
		buildNotifyText(filename, format, link, expiresAt, rows, req.Notify.Message),
	)

	evt := notify.ExportReadyEvent{
		Recipients:       req.Notify.Recipients,
		Channels:         normalizeNotifyChannels(req.Notify.Channels),
		Locale:           req.Export.Locale,
		TenantID:         req.Actor.Scope.TenantID,
		ActorID:          req.Actor.ID,
		FileName:         filename,
		Format:           format,
		URL:              link,
		ExpiresAt:        expiresAt,
		Rows:             rows,
		Parts:            req.Notify.Parts,
		ManifestURL:      req.Notify.ManifestURL,
		Message:          req.Notify.Message,
		ChannelOverrides: channelOverrides,
		Attachments:      resolveNotifyAttachments(req.Notify, attachment, s.limits.MaxAttachmentSize, s.logger),
	}
	return s.notifier.Send(ctx, evt)
}

func normalizeNotifyChannels(channels []string) []string {
	if len(channels) == 0 {
		return []string{"email"}
	}
	return channels
}

func resolveNotifyFormat(reqFormat export.Format, resultFormat export.Format) string {
	format := reqFormat
	if format == "" {
		format = resultFormat
	}
	if format == "" {
		format = export.FormatCSV
	}
	return string(format)
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

func deriveExpiresAt(meta export.ArtifactMeta, ttl time.Duration, now time.Time) string {
	if !meta.ExpiresAt.IsZero() {
		return meta.ExpiresAt.Format(time.RFC3339)
	}
	return now.Add(ttl).Format(time.RFC3339)
}

func notifyRowCount(rows int64) int {
	if rows <= 0 {
		return 0
	}
	return int(rows)
}

func ensureNotifyEmailOverrides(overrides map[string]map[string]any, htmlBody, textBody string) map[string]map[string]any {
	if htmlBody == "" && textBody == "" {
		return overrides
	}
	if overrides == nil {
		overrides = make(map[string]map[string]any)
	}
	emailOverrides := overrides["email"]
	if emailOverrides == nil {
		emailOverrides = make(map[string]any)
		overrides["email"] = emailOverrides
	}
	if htmlBody != "" {
		if _, ok := emailOverrides["html_body"]; !ok {
			emailOverrides["html_body"] = htmlBody
		}
	}
	if textBody != "" {
		if _, ok := emailOverrides["text_body"]; !ok {
			emailOverrides["text_body"] = textBody
		}
	}
	return overrides
}

func buildNotifyHTML(filename, format, url, expires string, rows int, note string) string {
	if filename == "" && url == "" && expires == "" && rows == 0 && strings.TrimSpace(note) == "" {
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
	if strings.TrimSpace(note) != "" {
		sb.WriteString(fmt.Sprintf(
			"<p>Note: %s</p>",
			html.EscapeString(note),
		))
	}
	return sb.String()
}

func buildNotifyText(filename, format, url, expires string, rows int, note string) string {
	if filename == "" && url == "" && expires == "" && rows == 0 && strings.TrimSpace(note) == "" {
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
	if strings.TrimSpace(note) != "" {
		lines = append(lines, "", "Note: "+strings.TrimSpace(note))
	}
	return strings.Join(lines, "\n")
}

func resolveNotifyAttachments(req NotificationRequest, attachment *Attachment, maxSize int64, logger export.Logger) []notify.NotificationAttachment {
	if len(req.Attachments) > 0 {
		return req.Attachments
	}
	if attachment == nil {
		return nil
	}
	size := attachment.Size
	if size == 0 {
		size = int64(len(attachment.Data))
	}
	if maxSize > 0 && size > maxSize {
		if logger != nil {
			logger.Infof("notification attachment skipped: size %d exceeds limit %d", size, maxSize)
		}
		return nil
	}
	return []notify.NotificationAttachment{{
		Filename:    attachment.Filename,
		ContentType: attachment.ContentType,
		Data:        attachment.Data,
		Size:        size,
	}}
}
