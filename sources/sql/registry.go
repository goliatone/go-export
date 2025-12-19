package exportsql

import (
	"fmt"
	"sync"

	"github.com/goliatone/go-export/export"
)

// Definition registers a named query.
type Definition struct {
	Name          string
	Query         string
	Validate      func(params any) error
	ScopeInjector func(scope export.Scope, params any) (any, error)
}

// Registry stores named query definitions.
type Registry struct {
	mu   sync.RWMutex
	defs map[string]Definition
}

// NewRegistry creates an empty registry.
func NewRegistry() *Registry {
	return &Registry{defs: make(map[string]Definition)}
}

// Register adds a named query definition.
func (r *Registry) Register(def Definition) error {
	if def.Name == "" {
		return export.NewError(export.KindValidation, "query name is required", nil)
	}
	if def.Query == "" {
		return export.NewError(export.KindValidation, "query string is required", nil)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.defs[def.Name]; exists {
		return export.NewError(export.KindValidation, fmt.Sprintf("query %q already registered", def.Name), nil)
	}
	r.defs[def.Name] = def
	return nil
}

// Resolve returns a query definition by name.
func (r *Registry) Resolve(name string) (Definition, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	def, ok := r.defs[name]
	return def, ok
}
