package exportcallback

import (
	"context"

	"github.com/goliatone/go-export/export"
)

// SourceFunc builds a RowIterator for a request.
type SourceFunc func(ctx context.Context, spec export.RowSourceSpec) (export.RowIterator, error)

// Source wraps a callback function as a RowSource.
type Source struct {
	fn SourceFunc
}

// NewSource creates a callback-based RowSource.
func NewSource(fn SourceFunc) *Source {
	return &Source{fn: fn}
}

// Open delegates to the configured callback.
func (s *Source) Open(ctx context.Context, spec export.RowSourceSpec) (export.RowIterator, error) {
	if s == nil || s.fn == nil {
		return nil, export.NewError(export.KindValidation, "callback source requires a function", nil)
	}
	return s.fn(ctx, spec)
}

// IteratorFunc yields a row or io.EOF.
type IteratorFunc func(ctx context.Context) (export.Row, error)

// FuncIterator wraps a function into a RowIterator.
type FuncIterator struct {
	NextFunc  IteratorFunc
	CloseFunc func() error
}

func (it *FuncIterator) Next(ctx context.Context) (export.Row, error) {
	if it == nil || it.NextFunc == nil {
		return nil, export.NewError(export.KindValidation, "iterator requires NextFunc", nil)
	}
	return it.NextFunc(ctx)
}

func (it *FuncIterator) Close() error {
	if it == nil || it.CloseFunc == nil {
		return nil
	}
	return it.CloseFunc()
}
