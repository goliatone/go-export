package exportapi

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/goliatone/go-export/export"
)

// IdempotencyStore stores idempotency keys.
type IdempotencyStore interface {
	Get(ctx context.Context, key string) (string, bool, error)
	Set(ctx context.Context, key, exportID string, ttl time.Duration) error
}

// MemoryIdempotencyStore stores idempotency keys in memory.
type MemoryIdempotencyStore struct {
	mu      sync.RWMutex
	entries map[string]idempotencyEntry
	clock   func() time.Time
}

type idempotencyEntry struct {
	exportID  string
	expiresAt time.Time
}

// NewMemoryIdempotencyStore creates an in-memory store.
func NewMemoryIdempotencyStore() *MemoryIdempotencyStore {
	return &MemoryIdempotencyStore{
		entries: make(map[string]idempotencyEntry),
		clock:   time.Now,
	}
}

// Get returns the export ID for an idempotency key.
func (s *MemoryIdempotencyStore) Get(ctx context.Context, key string) (string, bool, error) {
	_ = ctx
	if s == nil {
		return "", false, export.NewError(export.KindInternal, "idempotency store is nil", nil)
	}
	s.mu.RLock()
	entry, ok := s.entries[key]
	s.mu.RUnlock()
	if !ok {
		return "", false, nil
	}
	if !entry.expiresAt.IsZero() && s.now().After(entry.expiresAt) {
		s.mu.Lock()
		delete(s.entries, key)
		s.mu.Unlock()
		return "", false, nil
	}
	return entry.exportID, true, nil
}

// Set stores the export ID for an idempotency key.
func (s *MemoryIdempotencyStore) Set(ctx context.Context, key, exportID string, ttl time.Duration) error {
	_ = ctx
	if s == nil {
		return export.NewError(export.KindInternal, "idempotency store is nil", nil)
	}
	if key == "" {
		return export.NewError(export.KindValidation, "idempotency key is required", nil)
	}
	if exportID == "" {
		return export.NewError(export.KindValidation, "export ID is required", nil)
	}
	var expires time.Time
	if ttl > 0 {
		expires = s.now().Add(ttl)
	}
	s.mu.Lock()
	s.entries[key] = idempotencyEntry{exportID: exportID, expiresAt: expires}
	s.mu.Unlock()
	return nil
}

func (s *MemoryIdempotencyStore) now() time.Time {
	if s.clock == nil {
		return time.Now()
	}
	return s.clock()
}

func buildIdempotencyKey(key string, actor export.Actor, req export.ExportRequest) string {
	ids := append([]string(nil), req.Selection.IDs...)
	sort.Strings(ids)

	payload := idempotencyPayload{
		Key:          key,
		ActorID:      actor.ID,
		Scope:        actor.Scope,
		Definition:   req.Definition,
		Variant:      req.SourceVariant,
		Format:       req.Format,
		Columns:      req.Columns,
		Selection:    export.Selection{Mode: req.Selection.Mode, IDs: ids, Query: req.Selection.Query},
		QueryPayload: marshalQuery(req.Query),
	}

	raw, _ := json.Marshal(payload)
	sum := sha256.Sum256(raw)
	return fmt.Sprintf("export:%x", sum[:])
}

type idempotencyPayload struct {
	Key          string           `json:"key"`
	ActorID      string           `json:"actor_id,omitempty"`
	Scope        export.Scope     `json:"scope"`
	Definition   string           `json:"definition"`
	Variant      string           `json:"variant,omitempty"`
	Format       export.Format    `json:"format"`
	Columns      []string         `json:"columns,omitempty"`
	Selection    export.Selection `json:"selection,omitempty"`
	QueryPayload json.RawMessage  `json:"query,omitempty"`
}

func marshalQuery(query any) json.RawMessage {
	if query == nil {
		return nil
	}
	switch value := query.(type) {
	case json.RawMessage:
		return value
	case []byte:
		return json.RawMessage(value)
	default:
		raw, err := json.Marshal(value)
		if err != nil {
			return nil
		}
		return raw
	}
}
