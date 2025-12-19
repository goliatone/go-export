package exportsql

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/goliatone/go-export/export"
)

type captureExecutor struct {
	spec  QuerySpec
	calls int
	iter  export.RowIterator
}

func (e *captureExecutor) Query(ctx context.Context, spec QuerySpec) (export.RowIterator, error) {
	_ = ctx
	e.calls++
	e.spec = spec
	if e.iter != nil {
		return e.iter, nil
	}
	return &sliceIterator{rows: []export.Row{{"1"}}}, nil
}

type sliceIterator struct {
	rows  []export.Row
	index int
}

func (it *sliceIterator) Next(ctx context.Context) (export.Row, error) {
	_ = ctx
	if it.index >= len(it.rows) {
		return nil, io.EOF
	}
	row := it.rows[it.index]
	it.index++
	return row, nil
}

func (it *sliceIterator) Close() error { return nil }

type countingIterator struct {
	calls int
}

func (it *countingIterator) Next(ctx context.Context) (export.Row, error) {
	_ = ctx
	it.calls++
	return nil, io.EOF
}

func (it *countingIterator) Close() error { return nil }

type params struct {
	Status   string
	TenantID string
}

func TestSource_ValidatesAndInjectsScope(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Register(Definition{
		Name:  "users",
		Query: "select * from users",
		Validate: func(p any) error {
			if p == nil {
				return errors.New("params required")
			}
			return nil
		},
		ScopeInjector: func(scope export.Scope, p any) (any, error) {
			value := p.(params)
			value.TenantID = scope.TenantID
			return value, nil
		},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	exec := &captureExecutor{}
	source := NewSource(reg, exec, "users")

	_, err := source.Open(context.Background(), export.RowSourceSpec{
		Request: export.ExportRequest{Query: params{Status: "active"}},
		Actor:   export.Actor{ID: "actor-1", Scope: export.Scope{TenantID: "tenant-1"}},
		Columns: []export.Column{{Name: "id"}},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if exec.calls != 1 {
		t.Fatalf("expected executor to be called")
	}
	gotParams := exec.spec.Params.(params)
	if gotParams.TenantID != "tenant-1" {
		t.Fatalf("expected scope injection")
	}
}

func TestSource_InvalidParamsShortCircuits(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Register(Definition{
		Name:     "users",
		Query:    "select * from users",
		Validate: func(any) error { return errors.New("invalid") },
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	exec := &captureExecutor{}
	source := NewSource(reg, exec, "users")

	_, err := source.Open(context.Background(), export.RowSourceSpec{
		Request: export.ExportRequest{Query: params{Status: "active"}},
	})
	if err == nil {
		t.Fatalf("expected validation error")
	}
	if exec.calls != 0 {
		t.Fatalf("expected executor not to be called")
	}
}

func TestSource_DoesNotPrefetchRows(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Register(Definition{
		Name:  "users",
		Query: "select * from users",
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	iter := &countingIterator{}
	exec := &captureExecutor{iter: iter}
	source := NewSource(reg, exec, "users")

	_, err := source.Open(context.Background(), export.RowSourceSpec{
		Request: export.ExportRequest{},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if iter.calls != 0 {
		t.Fatalf("expected no Next calls during Open")
	}
}
