package export

import (
	"fmt"
	"sync"
)

// DefinitionRegistry stores export definitions.
type DefinitionRegistry struct {
	mu   sync.RWMutex
	defs map[string]ExportDefinition
}

// NewDefinitionRegistry creates an empty registry.
func NewDefinitionRegistry() *DefinitionRegistry {
	return &DefinitionRegistry{defs: make(map[string]ExportDefinition)}
}

// Register adds a definition.
func (r *DefinitionRegistry) Register(def ExportDefinition) error {
	if def.Name == "" {
		return NewError(KindValidation, "definition name is required", nil)
	}
	if def.RowSourceKey == "" {
		return NewError(KindValidation, "row source key is required", nil)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.defs[def.Name]; exists {
		return NewError(KindValidation, fmt.Sprintf("definition %q already registered", def.Name), nil)
	}
	r.defs[def.Name] = def
	return nil
}

// Resolve returns a resolved definition for the request.
func (r *DefinitionRegistry) Resolve(req ExportRequest) (ResolvedDefinition, error) {
	r.mu.RLock()
	def, ok := r.defs[req.Definition]
	r.mu.RUnlock()
	if !ok {
		return ResolvedDefinition{}, NewError(KindNotFound, fmt.Sprintf("definition %q not found", req.Definition), nil)
	}

	resolved := ResolvedDefinition{
		ExportDefinition: def,
		Variant:          req.SourceVariant,
	}

	if req.SourceVariant != "" {
		variant, ok := def.SourceVariants[req.SourceVariant]
		if !ok {
			return ResolvedDefinition{}, NewError(KindValidation, fmt.Sprintf("source variant %q not defined", req.SourceVariant), nil)
		}

		if variant.RowSourceKey != "" {
			resolved.RowSourceKey = variant.RowSourceKey
		}
		if len(variant.AllowedFormats) > 0 {
			resolved.AllowedFormats = variant.AllowedFormats
		}
		if variant.DefaultFilename != "" {
			resolved.DefaultFilename = variant.DefaultFilename
		}
		if variant.Policy != nil {
			resolved.Policy = mergePolicy(def.Policy, *variant.Policy)
		}
	}

	if len(resolved.AllowedFormats) == 0 {
		resolved.AllowedFormats = []Format{FormatCSV, FormatJSON, FormatNDJSON, FormatXLSX}
	}

	return resolved, nil
}

// RowSourceFactory creates a RowSource for a request.
type RowSourceFactory func(req ExportRequest, def ResolvedDefinition) (RowSource, error)

// RowSourceRegistry stores row source factories.
type RowSourceRegistry struct {
	mu        sync.RWMutex
	factories map[string]RowSourceFactory
}

// NewRowSourceRegistry creates an empty registry.
func NewRowSourceRegistry() *RowSourceRegistry {
	return &RowSourceRegistry{factories: make(map[string]RowSourceFactory)}
}

// Register adds a row source factory.
func (r *RowSourceRegistry) Register(key string, factory RowSourceFactory) error {
	if key == "" {
		return NewError(KindValidation, "row source key is required", nil)
	}
	if factory == nil {
		return NewError(KindValidation, "row source factory is required", nil)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.factories[key]; exists {
		return NewError(KindValidation, fmt.Sprintf("row source %q already registered", key), nil)
	}
	r.factories[key] = factory
	return nil
}

// Resolve finds a row source factory by key.
func (r *RowSourceRegistry) Resolve(key string) (RowSourceFactory, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	factory, ok := r.factories[key]
	return factory, ok
}

// RendererRegistry stores renderers by format.
type RendererRegistry struct {
	mu        sync.RWMutex
	renderers map[Format]Renderer
}

// NewRendererRegistry creates a registry.
func NewRendererRegistry() *RendererRegistry {
	return &RendererRegistry{renderers: make(map[Format]Renderer)}
}

// Register adds a renderer for a format.
func (r *RendererRegistry) Register(format Format, renderer Renderer) error {
	if format == "" {
		return NewError(KindValidation, "renderer format is required", nil)
	}
	if renderer == nil {
		return NewError(KindValidation, "renderer is required", nil)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.renderers[format]; exists {
		return NewError(KindValidation, fmt.Sprintf("renderer for %q already registered", format), nil)
	}
	r.renderers[format] = renderer
	return nil
}

// Resolve returns the renderer for the format.
func (r *RendererRegistry) Resolve(format Format) (Renderer, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	renderer, ok := r.renderers[format]
	return renderer, ok
}
