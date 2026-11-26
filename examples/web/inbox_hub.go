package main

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/goliatone/go-notifications/pkg/inbox"
	"github.com/goliatone/go-notifications/pkg/interfaces/broadcaster"
	"github.com/goliatone/go-notifications/pkg/privacy"
	"github.com/goliatone/go-router"
)

type inboxHub struct {
	mu           sync.RWMutex
	clients      map[string]map[string]*inboxClient
	inbox        *inbox.Service
	logger       *SimpleLogger
	stopping     bool
	readers      sync.WaitGroup
	shutdownOnce sync.Once
	shutdownDone chan struct{}
}

type inboxClient struct {
	conn   router.WebSocketContext
	mu     sync.Mutex
	closed bool
}

type inboxRealtimeMessage struct {
	Topic   string `json:"topic"`
	Payload any    `json:"payload,omitempty"`
	Badge   *int   `json:"badge,omitempty"`
}

func newInboxHub(logger *SimpleLogger) *inboxHub {
	return &inboxHub{
		clients:      make(map[string]map[string]*inboxClient),
		shutdownDone: make(chan struct{}),
		logger:       logger,
	}
}

func (h *inboxHub) SetInbox(svc *inbox.Service) {
	if h == nil {
		return
	}
	h.mu.Lock()
	h.inbox = svc
	h.mu.Unlock()
}

func (h *inboxHub) Add(userID string, conn router.WebSocketContext) *inboxClient {
	if h == nil || conn == nil {
		return nil
	}
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return nil
	}
	if _, ok := conn.(router.WebSocketReadInterrupter); !ok {
		return nil
	}
	client := &inboxClient{conn: conn}
	connID := conn.ConnectionID()

	h.mu.Lock()
	if h.stopping {
		h.mu.Unlock()
		return nil
	}
	h.readers.Add(1)
	if h.clients == nil {
		h.clients = make(map[string]map[string]*inboxClient)
	}
	if h.clients[userID] == nil {
		h.clients[userID] = make(map[string]*inboxClient)
	}
	h.clients[userID][connID] = client
	h.mu.Unlock()

	return client
}

func (h *inboxHub) Remove(userID string, conn router.WebSocketContext) {
	if h == nil || conn == nil {
		return
	}
	h.removeConn(userID, conn.ConnectionID())
}

func (h *inboxHub) Broadcast(ctx context.Context, event broadcaster.Event) error {
	if h == nil {
		return nil
	}
	userID := inboxUserIDFromPayload(event.Payload)
	if userID == "" {
		if payload, ok := event.Payload.(map[string]any); ok {
			if subject, ok := payload["subject_id"].(string); ok && subject != "" {
				h.mu.RLock()
				for connectedUser := range h.clients {
					if (privacy.DefaultPolicy{}).SafeSubjectID(connectedUser) == subject {
						userID = connectedUser
						break
					}
				}
				h.mu.RUnlock()
			}
		}
	}
	if userID == "" {
		h.logf("inbox broadcast skipped: no connected recipient topic=%s", event.Topic)
		return nil
	}

	msg := inboxRealtimeMessage{
		Topic:   event.Topic,
		Payload: event.Payload,
	}
	if badge, ok := h.badgeCount(ctx, userID); ok {
		msg.Badge = &badge
	}
	return h.broadcastToUser(userID, msg)
}

func (h *inboxHub) broadcastToUser(userID string, msg inboxRealtimeMessage) error {
	clients := h.snapshot(userID)
	if len(clients) == 0 {
		return nil
	}
	var firstErr error
	for _, client := range clients {
		if err := client.writeJSON(msg); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			client.interrupt()
		}
	}
	return firstErr
}

func (h *inboxHub) snapshot(userID string) []*inboxClient {
	h.mu.RLock()
	defer h.mu.RUnlock()
	conns := h.clients[userID]
	if len(conns) == 0 {
		return nil
	}
	result := make([]*inboxClient, 0, len(conns))
	for _, client := range conns {
		result = append(result, client)
	}
	return result
}

func (h *inboxHub) removeConn(userID, connID string) {
	if h == nil {
		return
	}
	userID = strings.TrimSpace(userID)
	if userID == "" || connID == "" {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	userConns, ok := h.clients[userID]
	if !ok {
		return
	}
	if client := userConns[connID]; client != nil {
		client.mu.Lock()
		client.closed = true
		client.mu.Unlock()
	}
	delete(userConns, connID)
	if len(userConns) == 0 {
		delete(h.clients, userID)
	}
}

func (h *inboxHub) badgeCount(ctx context.Context, userID string) (int, bool) {
	h.mu.RLock()
	inboxSvc := h.inbox
	h.mu.RUnlock()
	if inboxSvc == nil {
		return 0, false
	}
	if ctx == nil {
		ctx = context.Background()
	}
	count, err := inboxSvc.BadgeCount(ctx, userID)
	if err != nil {
		h.logf("inbox badge count failed: %v", err)
		return 0, false
	}
	return count, true
}

func (c *inboxClient) writeJSON(v any) error {
	if c == nil || c.conn == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	return c.conn.WriteJSON(v)
}

func (h *inboxHub) logf(format string, args ...any) {
	if h == nil || h.logger == nil {
		return
	}
	h.logger.Info(fmt.Sprintf(format, args...))
}

func inboxUserIDFromPayload(payload any) string {
	switch v := payload.(type) {
	case map[string]any:
		if raw, ok := v["user_id"]; ok {
			return strings.TrimSpace(fmt.Sprint(raw))
		}
	case map[string]string:
		if raw, ok := v["user_id"]; ok {
			return strings.TrimSpace(raw)
		}
	default:
		return ""
	}
	return ""
}

var _ broadcaster.Broadcaster = (*inboxHub)(nil)

// Shutdown prevents new readers and joins existing readers before HTTP shutdown.
func (h *inboxHub) Shutdown(ctx context.Context) error {
	if h == nil {
		return nil
	}
	h.shutdownOnce.Do(func() {
		h.mu.Lock()
		h.stopping = true
		var clients []*inboxClient
		for _, connections := range h.clients {
			for _, client := range connections {
				clients = append(clients, client)
			}
		}
		h.mu.Unlock()
		go func() {
			for _, client := range clients {
				client.interrupt()
			}
			h.readers.Wait()
			close(h.shutdownDone)
		}()
	})
	select {
	case <-h.shutdownDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Serialize with retirement so snapshots never touch a recycled native context.
func (c *inboxClient) interrupt() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	if reader, ok := c.conn.(router.WebSocketReadInterrupter); ok {
		_ = reader.InterruptRead()
	}
}
