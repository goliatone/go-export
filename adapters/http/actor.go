package exporthttp

import (
	"context"

	"github.com/goliatone/go-export/export"
)

type actorContextKey struct{}

// WithActor stores an actor in context for HTTP handlers.
func WithActor(ctx context.Context, actor export.Actor) context.Context {
	return context.WithValue(ctx, actorContextKey{}, actor)
}

// ContextActorProvider reads actors from request contexts.
type ContextActorProvider struct {
	Key any
}

// FromContext returns the actor stored in context.
func (p ContextActorProvider) FromContext(ctx context.Context) (export.Actor, error) {
	key := p.Key
	if key == nil {
		key = actorContextKey{}
	}
	actor, ok := ctx.Value(key).(export.Actor)
	if !ok {
		return export.Actor{}, export.NewError(export.KindAuthz, "actor not found in context", nil)
	}
	return actor, nil
}

// StaticActorProvider always returns the configured actor.
type StaticActorProvider struct {
	Actor export.Actor
}

// FromContext returns the configured actor.
func (p StaticActorProvider) FromContext(ctx context.Context) (export.Actor, error) {
	_ = ctx
	return p.Actor, nil
}
