package exportjob

import (
	"context"
	"sync"

	"github.com/goliatone/go-export/export"
)

// CancelRegistry tracks running export jobs for cancellation.
type CancelRegistry struct {
	mu      sync.Mutex
	cancels map[string]context.CancelFunc
}

// NewCancelRegistry creates a new registry for job cancellation.
func NewCancelRegistry() *CancelRegistry {
	return &CancelRegistry{cancels: make(map[string]context.CancelFunc)}
}

// Register associates a cancel func with an export ID.
func (r *CancelRegistry) Register(exportID string, cancel context.CancelFunc) func() {
	if r == nil || exportID == "" || cancel == nil {
		return func() {}
	}
	r.mu.Lock()
	r.cancels[exportID] = cancel
	r.mu.Unlock()

	return func() {
		r.mu.Lock()
		delete(r.cancels, exportID)
		r.mu.Unlock()
	}
}

// Cancel triggers context cancellation for a running export.
func (r *CancelRegistry) Cancel(ctx context.Context, exportID string) error {
	_ = ctx
	if r == nil {
		return export.NewError(export.KindInternal, "cancel registry is nil", nil)
	}
	if exportID == "" {
		return export.NewError(export.KindValidation, "export ID is required", nil)
	}

	r.mu.Lock()
	cancel, ok := r.cancels[exportID]
	r.mu.Unlock()
	if !ok {
		return export.NewError(export.KindNotFound, "export not running", nil)
	}
	cancel()
	return nil
}
