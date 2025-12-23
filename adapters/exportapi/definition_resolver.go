package exportapi

import (
	"context"
	"strings"

	"github.com/goliatone/go-export/export"
)

// DefinitionResolver resolves a definition name for requests that omit it.
type DefinitionResolver interface {
	ResolveDefinition(ctx context.Context, req export.ExportRequest) (string, error)
}

// DefinitionResolverFunc adapts a function to a DefinitionResolver.
type DefinitionResolverFunc func(ctx context.Context, req export.ExportRequest) (string, error)

// ResolveDefinition resolves the definition name.
func (f DefinitionResolverFunc) ResolveDefinition(ctx context.Context, req export.ExportRequest) (string, error) {
	if f == nil {
		return "", export.NewError(export.KindInternal, "definition resolver is nil", nil)
	}
	return f(ctx, req)
}

// NewDefinitionResolver returns a resolver that maps request resources to definitions.
func NewDefinitionResolver(registry *export.DefinitionRegistry) DefinitionResolver {
	return DefinitionResolverFunc(func(ctx context.Context, req export.ExportRequest) (string, error) {
		_ = ctx
		if registry == nil {
			return "", export.NewError(export.KindInternal, "definition registry not configured", nil)
		}
		resource := strings.TrimSpace(req.Resource)
		if resource == "" {
			return "", export.NewError(export.KindValidation, "resource is required", nil)
		}
		def, err := registry.ResolveByResource(resource)
		if err != nil {
			return "", err
		}
		return def.Name, nil
	})
}
