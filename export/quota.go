package export

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// RateLimiter enforces per-actor rate limits in memory.
type RateLimiter struct {
	Max     int
	Window  time.Duration
	KeyFunc func(actor Actor, req ExportRequest, def ResolvedDefinition) string
	Now     func() time.Time

	mu      sync.Mutex
	buckets map[string]*rateBucket
}

type rateBucket struct {
	count   int
	resetAt time.Time
}

// Allow enforces the configured rate limit, keyed by actor/scope unless overridden.
func (l *RateLimiter) Allow(ctx context.Context, actor Actor, req ExportRequest, def ResolvedDefinition) error {
	_ = ctx
	if l == nil {
		return NewError(KindInternal, "rate limiter is nil", nil)
	}
	if l.Max <= 0 || l.Window <= 0 {
		return nil
	}

	key := ""
	if l.KeyFunc != nil {
		key = l.KeyFunc(actor, req, def)
	} else {
		key = defaultRateKey(actor)
	}
	if key == "" {
		return nil
	}

	now := time.Now
	if l.Now != nil {
		now = l.Now
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.buckets == nil {
		l.buckets = make(map[string]*rateBucket)
	}

	current := now()
	bucket := l.buckets[key]
	if bucket == nil || current.After(bucket.resetAt) {
		bucket = &rateBucket{resetAt: current.Add(l.Window)}
		l.buckets[key] = bucket
	}

	bucket.count++
	if bucket.count > l.Max {
		return NewError(KindValidation, "rate limit exceeded", nil)
	}
	return nil
}

func defaultRateKey(actor Actor) string {
	if actor.ID == "" {
		return ""
	}
	return fmt.Sprintf("%s:%s:%s", actor.ID, actor.Scope.TenantID, actor.Scope.WorkspaceID)
}
