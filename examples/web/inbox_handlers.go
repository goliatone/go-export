package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/goliatone/go-notifications/pkg/inbox"
	"github.com/goliatone/go-notifications/pkg/interfaces/store"
	"github.com/goliatone/go-router"
)

type inboxMarkPayload struct {
	ID   string   `json:"id"`
	IDs  []string `json:"ids"`
	Read *bool    `json:"read"`
}

func (a *App) ListInbox(c router.Context) error {
	if a == nil || a.Inbox == nil {
		return inboxUnavailable(c)
	}
	userID := a.inboxUserID(c)
	if userID == "" {
		return c.JSON(400, map[string]any{"error": "missing user_id"})
	}

	limit := c.QueryInt("limit", 50)
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	offset := c.QueryInt("offset", 0)
	if offset < 0 {
		offset = 0
	}

	filters := inbox.ListFilters{
		UnreadOnly:       parseInboxBool(c.Query("unread")),
		IncludeDismissed: parseInboxBool(c.Query("include_dismissed")),
		PinnedOnly:       parseInboxBool(c.Query("pinned")),
		SnoozedOnly:      parseInboxBool(c.Query("snoozed")),
	}
	if before, ok := parseInboxTime(c.Query("before")); ok {
		filters.Before = before
	}

	result, err := a.Inbox.List(c.Context(), userID, store.ListOptions{
		Limit:  limit,
		Offset: offset,
	}, filters)
	if err != nil {
		return c.JSON(500, map[string]any{"error": err.Error()})
	}
	return c.JSON(200, map[string]any{
		"items": result.Items,
		"total": result.Total,
	})
}

func (a *App) InboxBadge(c router.Context) error {
	if a == nil || a.Inbox == nil {
		return inboxUnavailable(c)
	}
	userID := a.inboxUserID(c)
	if userID == "" {
		return c.JSON(400, map[string]any{"error": "missing user_id"})
	}
	count, err := a.Inbox.BadgeCount(c.Context(), userID)
	if err != nil {
		return c.JSON(500, map[string]any{"error": err.Error()})
	}
	return c.JSON(200, map[string]any{"unread": count})
}

func (a *App) InboxMarkRead(c router.Context) error {
	if a == nil || a.Inbox == nil {
		return inboxUnavailable(c)
	}
	userID := a.inboxUserID(c)
	if userID == "" {
		return c.JSON(400, map[string]any{"error": "missing user_id"})
	}

	var payload inboxMarkPayload
	if err := c.Bind(&payload); err != nil {
		return c.JSON(400, map[string]any{"error": "invalid payload"})
	}
	ids := payload.IDs
	if len(ids) == 0 && strings.TrimSpace(payload.ID) != "" {
		ids = []string{strings.TrimSpace(payload.ID)}
	}
	if len(ids) == 0 {
		return c.JSON(400, map[string]any{"error": "missing ids"})
	}
	read := true
	if payload.Read != nil {
		read = *payload.Read
	}

	if err := a.Inbox.MarkRead(c.Context(), userID, ids, read); err != nil {
		return c.JSON(500, map[string]any{"error": err.Error()})
	}
	return c.JSON(200, map[string]any{
		"ok":   true,
		"read": read,
	})
}

func (a *App) InboxDismiss(c router.Context) error {
	if a == nil || a.Inbox == nil {
		return inboxUnavailable(c)
	}
	userID := a.inboxUserID(c)
	if userID == "" {
		return c.JSON(400, map[string]any{"error": "missing user_id"})
	}
	id := strings.TrimSpace(c.Param("id"))
	if id == "" {
		return c.JSON(400, map[string]any{"error": "missing id"})
	}
	if err := a.Inbox.Dismiss(c.Context(), userID, id); err != nil {
		return c.JSON(500, map[string]any{"error": err.Error()})
	}
	return c.JSON(200, map[string]any{"ok": true})
}

func (a *App) InboxSocket() func(router.WebSocketContext) error {
	return func(ws router.WebSocketContext) error {
		if a == nil || a.InboxHub == nil {
			return ws.CloseWithStatus(router.CloseServiceRestart, "inbox not configured")
		}
		userID := a.inboxUserIDFromWS(ws)
		if userID == "" {
			return ws.CloseWithStatus(router.ClosePolicyViolation, "missing user_id")
		}

		client := a.InboxHub.Add(userID, ws)
		if client == nil {
			return nil
		}
		defer a.InboxHub.Remove(userID, ws)

		if a.Inbox != nil {
			if count, err := a.Inbox.BadgeCount(ws.Context(), userID); err == nil {
				msg := inboxRealtimeMessage{
					Topic:   "inbox.badge",
					Payload: map[string]any{"user_id": userID},
					Badge:   &count,
				}
				_ = client.writeJSON(msg)
			}
		}

		for {
			if _, _, err := ws.ReadMessage(); err != nil {
				break
			}
		}
		return nil
	}
}

func (a *App) inboxUserID(c router.Context) string {
	if c != nil {
		if userID := strings.TrimSpace(c.Query("user_id")); userID != "" {
			return userID
		}
		if userID := strings.TrimSpace(c.Header("X-User-ID")); userID != "" {
			return userID
		}
	}
	if a == nil {
		return ""
	}
	return strings.TrimSpace(a.getActor().ID)
}

func (a *App) inboxUserIDFromWS(ws router.WebSocketContext) string {
	if ws == nil {
		return ""
	}
	if raw, ok := ws.UpgradeData("user_id"); ok {
		return strings.TrimSpace(fmt.Sprint(raw))
	}
	return a.inboxUserID(ws)
}

func inboxUnavailable(c router.Context) error {
	return c.JSON(503, map[string]any{"error": "inbox not configured"})
}

func parseInboxBool(raw string) bool {
	value := strings.ToLower(strings.TrimSpace(raw))
	switch value {
	case "1", "true", "yes", "y", "on":
		return true
	default:
		return false
	}
}

func parseInboxTime(raw string) (time.Time, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, false
	}
	if ts, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return time.Unix(ts, 0).UTC(), true
	}
	if t, err := time.Parse(time.RFC3339, raw); err == nil {
		return t, true
	}
	return time.Time{}, false
}
