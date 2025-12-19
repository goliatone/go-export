package exportsql

import (
	"context"
	"fmt"

	"github.com/goliatone/go-export/export"
)

// QuerySpec describes a named query execution.
type QuerySpec struct {
	Name    string
	Query   string
	Params  any
	Actor   export.Actor
	Scope   export.Scope
	Columns []export.Column
}

// Executor runs a named query and returns a row iterator.
type Executor interface {
	Query(ctx context.Context, spec QuerySpec) (export.RowIterator, error)
}

// Source executes a named query with validated params.
type Source struct {
	Registry  *Registry
	Executor  Executor
	QueryName string
}

// NewSource creates a named query row source.
func NewSource(reg *Registry, exec Executor, name string) *Source {
	return &Source{Registry: reg, Executor: exec, QueryName: name}
}

// Open validates params and executes the named query.
func (s *Source) Open(ctx context.Context, spec export.RowSourceSpec) (export.RowIterator, error) {
	if s == nil || s.Registry == nil {
		return nil, export.NewError(export.KindValidation, "query registry is required", nil)
	}
	if s.Executor == nil {
		return nil, export.NewError(export.KindValidation, "query executor is required", nil)
	}
	if s.QueryName == "" {
		return nil, export.NewError(export.KindValidation, "query name is required", nil)
	}

	def, ok := s.Registry.Resolve(s.QueryName)
	if !ok {
		return nil, export.NewError(export.KindNotFound, fmt.Sprintf("query %q not registered", s.QueryName), nil)
	}

	params, err := validateParams(def, spec.Request.Query)
	if err != nil {
		return nil, err
	}

	if def.ScopeInjector != nil {
		params, err = def.ScopeInjector(spec.Actor.Scope, params)
		if err != nil {
			return nil, err
		}
	} else if injector, ok := params.(interface {
		WithScope(scope export.Scope) (any, error)
	}); ok {
		params, err = injector.WithScope(spec.Actor.Scope)
		if err != nil {
			return nil, err
		}
	}

	return s.Executor.Query(ctx, QuerySpec{
		Name:    def.Name,
		Query:   def.Query,
		Params:  params,
		Actor:   spec.Actor,
		Scope:   spec.Actor.Scope,
		Columns: spec.Columns,
	})
}

func validateParams(def Definition, params any) (any, error) {
	if def.Validate != nil {
		if err := def.Validate(params); err != nil {
			return nil, err
		}
		return params, nil
	}
	if validator, ok := params.(interface{ Validate() error }); ok {
		if err := validator.Validate(); err != nil {
			return nil, err
		}
	}
	return params, nil
}
