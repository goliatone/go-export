package exportrepo

import (
	"context"

	"github.com/goliatone/go-export/export"
)

// Spec captures repository query inputs.
type Spec struct {
	Request   export.ExportRequest
	Columns   []export.Column
	Actor     export.Actor
	Scope     export.Scope
	Selection export.Selection
	Query     any
}

// Repository streams rows for a repository-backed export.
type Repository interface {
	Stream(ctx context.Context, spec Spec) (export.RowIterator, error)
}

// Source adapts a repository to a RowSource.
type Source struct {
	Repo Repository
}

// NewSource creates a repository-backed RowSource.
func NewSource(repo Repository) *Source {
	return &Source{Repo: repo}
}

// Open delegates to the repository stream method.
func (s *Source) Open(ctx context.Context, spec export.RowSourceSpec) (export.RowIterator, error) {
	if s == nil || s.Repo == nil {
		return nil, export.NewError(export.KindValidation, "repository is required", nil)
	}
	return s.Repo.Stream(ctx, Spec{
		Request:   spec.Request,
		Columns:   spec.Columns,
		Actor:     spec.Actor,
		Scope:     spec.Actor.Scope,
		Selection: spec.Request.Selection,
		Query:     spec.Request.Query,
	})
}
