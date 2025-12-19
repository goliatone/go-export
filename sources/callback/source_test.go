package exportcallback

import (
	"context"
	"io"
	"testing"

	"github.com/goliatone/go-export/export"
)

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

func TestSource_OpenCallsFunc(t *testing.T) {
	called := false
	source := NewSource(func(ctx context.Context, spec export.RowSourceSpec) (export.RowIterator, error) {
		_ = ctx
		if spec.Request.Definition != "users" {
			t.Fatalf("unexpected definition: %q", spec.Request.Definition)
		}
		called = true
		return &sliceIterator{rows: []export.Row{{"1", "alice"}}}, nil
	})

	it, err := source.Open(context.Background(), export.RowSourceSpec{
		Request: export.ExportRequest{Definition: "users"},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	row, err := it.Next(context.Background())
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if row[0] != "1" {
		t.Fatalf("expected row data")
	}
	if !called {
		t.Fatalf("expected callback to be invoked")
	}
}

func TestFuncIterator_NextNil(t *testing.T) {
	it := &FuncIterator{}
	if _, err := it.Next(context.Background()); err == nil {
		t.Fatalf("expected error")
	}
}
