package export

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// MemoryStore stores artifacts in memory (test/dev only).
type MemoryStore struct {
	mu      sync.RWMutex
	objects map[string]memoryObject
}

type memoryObject struct {
	data []byte
	meta ArtifactMeta
}

// NewMemoryStore creates an in-memory artifact store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{objects: make(map[string]memoryObject)}
}

// Put stores an artifact.
func (s *MemoryStore) Put(ctx context.Context, key string, r io.Reader, meta ArtifactMeta) (ArtifactRef, error) {
	_ = ctx
	if key == "" {
		return ArtifactRef{}, NewError(KindValidation, "artifact key is required", nil)
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return ArtifactRef{}, err
	}
	meta.Size = int64(len(data))
	if meta.CreatedAt.IsZero() {
		meta.CreatedAt = time.Now()
	}

	s.mu.Lock()
	s.objects[key] = memoryObject{data: data, meta: meta}
	s.mu.Unlock()

	return ArtifactRef{Key: key, Meta: meta}, nil
}

// Open reads an artifact.
func (s *MemoryStore) Open(ctx context.Context, key string) (io.ReadCloser, ArtifactMeta, error) {
	_ = ctx
	s.mu.RLock()
	obj, ok := s.objects[key]
	s.mu.RUnlock()
	if !ok {
		return nil, ArtifactMeta{}, NewError(KindNotFound, fmt.Sprintf("artifact %q not found", key), nil)
	}
	return io.NopCloser(bytes.NewReader(obj.data)), obj.meta, nil
}

// Delete removes an artifact.
func (s *MemoryStore) Delete(ctx context.Context, key string) error {
	_ = ctx
	s.mu.Lock()
	delete(s.objects, key)
	s.mu.Unlock()
	return nil
}

// SignedURL returns a static error for memory store.
func (s *MemoryStore) SignedURL(ctx context.Context, key string, ttl time.Duration) (string, error) {
	_ = ctx
	_ = key
	_ = ttl
	return "", NewError(KindNotImpl, "signed URLs not supported by memory store", nil)
}

// MemoryTracker stores progress in memory (test/dev only).
type MemoryTracker struct {
	mu      sync.RWMutex
	records map[string]ExportRecord
	counter uint64
}

// NewMemoryTracker creates an in-memory tracker.
func NewMemoryTracker() *MemoryTracker {
	return &MemoryTracker{records: make(map[string]ExportRecord)}
}

// Start creates a new record.
func (t *MemoryTracker) Start(ctx context.Context, record ExportRecord) (string, error) {
	_ = ctx
	if record.ID == "" {
		record.ID = t.nextID()
	}
	if record.State == "" {
		record.State = StateQueued
	}
	if record.CreatedAt.IsZero() {
		record.CreatedAt = time.Now()
	}

	t.mu.Lock()
	t.records[record.ID] = record
	t.mu.Unlock()
	return record.ID, nil
}

// Advance updates counts.
func (t *MemoryTracker) Advance(ctx context.Context, id string, delta ProgressDelta, meta map[string]any) error {
	_ = ctx
	_ = meta

	t.mu.Lock()
	record, ok := t.records[id]
	if !ok {
		t.mu.Unlock()
		return NewError(KindNotFound, fmt.Sprintf("export %q not found", id), nil)
	}
	record.Counts.Processed += delta.Rows
	record.BytesWritten += delta.Bytes
	t.records[id] = record
	t.mu.Unlock()
	return nil
}

// SetState updates the record state.
func (t *MemoryTracker) SetState(ctx context.Context, id string, state ExportState, meta map[string]any) error {
	_ = ctx
	_ = meta

	t.mu.Lock()
	record, ok := t.records[id]
	if !ok {
		t.mu.Unlock()
		return NewError(KindNotFound, fmt.Sprintf("export %q not found", id), nil)
	}
	record.State = state
	if state == StateRunning && record.StartedAt.IsZero() {
		record.StartedAt = time.Now()
	}
	if state == StateCompleted && record.CompletedAt.IsZero() {
		record.CompletedAt = time.Now()
	}
	t.records[id] = record
	t.mu.Unlock()
	return nil
}

// Fail records failure state.
func (t *MemoryTracker) Fail(ctx context.Context, id string, err error, meta map[string]any) error {
	_ = ctx
	_ = meta

	t.mu.Lock()
	record, ok := t.records[id]
	if !ok {
		t.mu.Unlock()
		return NewError(KindNotFound, fmt.Sprintf("export %q not found", id), nil)
	}
	record.State = StateFailed
	record.CompletedAt = time.Now()
	t.records[id] = record
	t.mu.Unlock()
	_ = err
	return nil
}

// Complete marks the export as completed.
func (t *MemoryTracker) Complete(ctx context.Context, id string, meta map[string]any) error {
	_ = ctx
	_ = meta

	t.mu.Lock()
	record, ok := t.records[id]
	if !ok {
		t.mu.Unlock()
		return NewError(KindNotFound, fmt.Sprintf("export %q not found", id), nil)
	}
	record.State = StateCompleted
	record.CompletedAt = time.Now()
	t.records[id] = record
	t.mu.Unlock()
	return nil
}

// Status returns a record by ID.
func (t *MemoryTracker) Status(ctx context.Context, id string) (ExportRecord, error) {
	_ = ctx
	t.mu.RLock()
	record, ok := t.records[id]
	t.mu.RUnlock()
	if !ok {
		return ExportRecord{}, NewError(KindNotFound, fmt.Sprintf("export %q not found", id), nil)
	}
	return record, nil
}

// List returns records matching a filter.
func (t *MemoryTracker) List(ctx context.Context, filter ProgressFilter) ([]ExportRecord, error) {
	_ = ctx
	result := []ExportRecord{}
	
	t.mu.RLock()
	for _, record := range t.records {
		if filter.Definition != "" && record.Definition != filter.Definition {
			continue
		}
		if filter.State != "" && record.State != filter.State {
			continue
		}
		if !filter.Since.IsZero() && record.CreatedAt.Before(filter.Since) {
			continue
		}
		if !filter.Until.IsZero() && record.CreatedAt.After(filter.Until) {
			continue
		}
		result = append(result, record)
	}
	t.mu.RUnlock()
	return result, nil
}

func (t *MemoryTracker) nextID() string {
	id := atomic.AddUint64(&t.counter, 1)
	return fmt.Sprintf("exp-%d", id)
}
