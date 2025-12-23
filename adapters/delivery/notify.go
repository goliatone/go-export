package exportdelivery

import (
	"context"
	"fmt"
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

	evt := notify.ExportReadyEvent{
		Recipients:       req.Notify.Recipients,
		Channels:         normalizeNotifyChannels(req.Notify.Channels),
		Locale:           req.Export.Locale,
		TenantID:         req.Actor.Scope.TenantID,
		ActorID:          req.Actor.ID,
		FileName:         resolveNotifyFilename(ref.Meta, result.Filename, req.Export.Definition, req.Export.Format),
		Format:           resolveNotifyFormat(req.Export.Format, result.Format),
		URL:              link,
		ExpiresAt:        deriveExpiresAt(ref.Meta, ttl, now),
		Rows:             notifyRowCount(result.Rows),
		Parts:            req.Notify.Parts,
		ManifestURL:      req.Notify.ManifestURL,
		Message:          req.Notify.Message,
		ChannelOverrides: req.Notify.ChannelOverrides,
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
