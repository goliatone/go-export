package exporthttp

import "github.com/goliatone/go-export/adapters/exportapi"

// IdempotencyStore stores idempotency keys.
type IdempotencyStore = exportapi.IdempotencyStore

// MemoryIdempotencyStore stores idempotency keys in memory.
type MemoryIdempotencyStore = exportapi.MemoryIdempotencyStore

// NewMemoryIdempotencyStore creates an in-memory store.
func NewMemoryIdempotencyStore() *MemoryIdempotencyStore {
	return exportapi.NewMemoryIdempotencyStore()
}
