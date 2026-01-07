package main

import (
	"os"
	"strings"
	"time"

	exportdelivery "github.com/goliatone/go-export/adapters/delivery"
	"github.com/goliatone/go-export/export"
	"github.com/goliatone/go-router"
)

type scheduleDeliveryInput struct {
	Definition   string   `json:"definition"`
	Format       string   `json:"format"`
	DeliveryMode string   `json:"delivery_mode"`
	ScheduleMode string   `json:"schedule_mode"`
	Recipients   []string `json:"recipients"`
	Notify       bool     `json:"notify"`
}

func (a *App) RunScheduledDeliveries(c router.Context) error {
	if a == nil || a.Delivery == nil {
		return c.JSON(500, map[string]any{"error": "delivery service not configured"})
	}

	var input scheduleDeliveryInput
	if err := c.Bind(&input); err != nil {
		return c.JSON(400, map[string]any{"error": "invalid request body"})
	}

	definition := strings.TrimSpace(input.Definition)
	if definition == "" {
		definition = "users"
	}

	formatValue := strings.TrimSpace(input.Format)
	if formatValue == "" {
		formatValue = a.Config.Export.DefaultFormat
	}
	format := export.NormalizeFormat(export.Format(formatValue))

	deliveryMode, err := resolveDeliveryMode(input.DeliveryMode)
	if err != nil {
		return c.JSON(400, map[string]any{"error": err.Error()})
	}

	scheduleMode, err := resolveScheduleMode(input.ScheduleMode)
	if err != nil {
		return c.JSON(400, map[string]any{"error": err.Error()})
	}

	recipients := normalizeRecipients(input.Recipients)
	if len(recipients) == 0 {
		recipients = normalizeRecipients(a.Config.Export.Notifications.Recipients)
	}
	if len(recipients) == 0 {
		recipients = []string{defaultDemoEmail()}
	}

	req := exportdelivery.Request{
		Actor: a.getActor(),
		Export: export.ExportRequest{
			Definition: definition,
			Format:     format,
		},
		Mode: deliveryMode,
		Targets: []exportdelivery.Target{
			{
				Kind: exportdelivery.TargetEmail,
				Email: exportdelivery.EmailTarget{
					To: recipients,
				},
			},
		},
	}
	notifyEnabled := input.Notify && a.Config.Export.Notifications.Enabled
	if notifyEnabled {
		req.Notify = exportdelivery.NotificationRequest{
			Recipients: recipients,
			Channels:   a.Config.Export.Notifications.Channels,
			Message:    "Your export is ready.",
		}
	}

	start := time.Now()
	switch scheduleMode {
	case exportdelivery.ScheduleModeEnqueue:
		if a.DeliveryScheduler == nil {
			return c.JSON(500, map[string]any{"error": "delivery scheduler not configured"})
		}
		if err := a.DeliveryScheduler.RequestDelivery(c.Context(), req); err != nil {
			return c.JSON(500, map[string]any{"error": err.Error()})
		}
	case exportdelivery.ScheduleModeExecuteSync:
		if a.DeliveryBuilder == nil || a.DeliveryTask == nil {
			return c.JSON(500, map[string]any{"error": "delivery executor not configured"})
		}
		msg, err := a.DeliveryBuilder.Build(c.Context(), req)
		if err != nil {
			return c.JSON(500, map[string]any{"error": err.Error()})
		}
		if err := a.DeliveryTask.Execute(c.Context(), msg); err != nil {
			return c.JSON(500, map[string]any{"error": err.Error()})
		}
	default:
		return c.JSON(400, map[string]any{"error": "invalid schedule mode"})
	}

	return c.JSON(200, map[string]any{
		"ok":             true,
		"mode":           scheduleMode,
		"delivery_mode":  deliveryMode,
		"definition":     definition,
		"format":         format,
		"recipients":     recipients,
		"notify_enabled": notifyEnabled,
		"duration_ms":    time.Since(start).Milliseconds(),
	})
}

func resolveScheduleMode(value string) (exportdelivery.ScheduleMode, error) {
	mode := exportdelivery.ScheduleModeExecuteSync
	if envValue := strings.TrimSpace(os.Getenv("EXPORT_DELIVERY_SCHEDULE_MODE")); envValue != "" {
		parsed, ok := parseScheduleMode(envValue)
		if !ok {
			return "", export.NewError(export.KindValidation, "schedule mode is invalid", nil)
		}
		mode = parsed
	}
	if value = strings.TrimSpace(value); value != "" {
		parsed, ok := parseScheduleMode(value)
		if !ok {
			return "", export.NewError(export.KindValidation, "schedule mode is invalid", nil)
		}
		mode = parsed
	}
	return mode, nil
}

func parseScheduleMode(value string) (exportdelivery.ScheduleMode, bool) {
	switch normalizeToken(value) {
	case string(exportdelivery.ScheduleModeEnqueue):
		return exportdelivery.ScheduleModeEnqueue, true
	case "sync", "execute", string(exportdelivery.ScheduleModeExecuteSync):
		return exportdelivery.ScheduleModeExecuteSync, true
	default:
		return "", false
	}
}

func resolveDeliveryMode(value string) (exportdelivery.DeliveryMode, error) {
	normalized := normalizeToken(value)
	if normalized == "" || normalized == string(exportdelivery.DeliveryLink) {
		return exportdelivery.DeliveryLink, nil
	}
	if normalized == string(exportdelivery.DeliveryAttachment) {
		return exportdelivery.DeliveryAttachment, nil
	}
	return "", export.NewError(export.KindValidation, "delivery mode is invalid", nil)
}

func normalizeToken(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	value = strings.ReplaceAll(value, "-", "_")
	value = strings.ReplaceAll(value, " ", "_")
	return value
}

func normalizeRecipients(recipients []string) []string {
	if len(recipients) == 0 {
		return nil
	}
	out := make([]string, 0, len(recipients))
	for _, recipient := range recipients {
		recipient = strings.TrimSpace(recipient)
		if recipient == "" {
			continue
		}
		out = append(out, recipient)
	}
	return out
}
