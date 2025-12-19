package exportcrud

import (
	"context"
	"fmt"

	"github.com/goliatone/go-export/export"
)

// Filter describes a simple query filter.
type Filter struct {
	Field string
	Op    string
	Value any
}

// Sort describes a sort directive.
type Sort struct {
	Field string
	Desc  bool
}

// Query captures datagrid query inputs.
type Query struct {
	Filters []Filter
	Search  string
	Sort    []Sort
	Cursor  string
	Limit   int
	Offset  int
}

// Spec captures row source inputs for a crud adapter.
type Spec struct {
	Query     Query
	Selection export.Selection
	Columns   []string
	Actor     export.Actor
	Scope     export.Scope
}

// Streamer executes a query and returns a row iterator.
type Streamer interface {
	Stream(ctx context.Context, spec Spec) (export.RowIterator, error)
}

// Config configures default ordering.
type Config struct {
	PrimaryKey string
}

// Source adapts a datagrid streamer to a RowSource.
type Source struct {
	Streamer Streamer
	Config   Config
}

// NewSource creates a crud row source.
func NewSource(streamer Streamer, cfg Config) *Source {
	return &Source{Streamer: streamer, Config: cfg}
}

// Open builds a spec and delegates to the configured streamer.
func (s *Source) Open(ctx context.Context, spec export.RowSourceSpec) (export.RowIterator, error) {
	if s == nil || s.Streamer == nil {
		return nil, export.NewError(export.KindValidation, "crud streamer is required", nil)
	}

	query, err := decodeQuery(spec.Request.Query)
	if err != nil {
		return nil, err
	}

	columns := make([]string, 0, len(spec.Columns))
	for _, col := range spec.Columns {
		columns = append(columns, col.Name)
	}

	if len(query.Sort) == 0 {
		primaryKey := s.Config.PrimaryKey
		if primaryKey == "" {
			primaryKey = "id"
		}
		query.Sort = []Sort{{Field: primaryKey}}
	}

	return s.Streamer.Stream(ctx, Spec{
		Query:     query,
		Selection: spec.Request.Selection,
		Columns:   columns,
		Actor:     spec.Actor,
		Scope:     spec.Actor.Scope,
	})
}

func decodeQuery(raw any) (Query, error) {
	if raw == nil {
		return Query{}, nil
	}
	switch value := raw.(type) {
	case Query:
		return value, nil
	case *Query:
		if value == nil {
			return Query{}, nil
		}
		return *value, nil
	default:
		return Query{}, export.NewError(export.KindValidation, fmt.Sprintf("unsupported query type %T", raw), nil)
	}
}
