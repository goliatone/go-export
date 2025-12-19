package exportrepo

import (
	"context"
	"io"
	"testing"

	"github.com/goliatone/go-export/export"
)

type stubRepo struct {
	spec Spec
}

func (r *stubRepo) Stream(ctx context.Context, spec Spec) (export.RowIterator, error) {
	_ = ctx
	r.spec = spec
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

func TestSource_OpenPassesSpec(t *testing.T) {
	repo := &stubRepo{}
	source := NewSource(repo)

	_, err := source.Open(context.Background(), export.RowSourceSpec{
		Request: export.ExportRequest{
			Definition: "users",
			Query:      map[string]any{"status": "active"},
			Selection:  export.Selection{Mode: export.SelectionIDs, IDs: []string{"1", "2"}},
		},
		Columns: []export.Column{{Name: "id"}},
		Actor:   export.Actor{ID: "actor-1", Scope: export.Scope{TenantID: "tenant-1"}},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if repo.spec.Request.Definition != "users" {
		t.Fatalf("expected definition to be passed through")
	}
	if repo.spec.Scope.TenantID != "tenant-1" {
		t.Fatalf("expected scope injection")
	}
	if repo.spec.Selection.Mode != export.SelectionIDs || len(repo.spec.Selection.IDs) != 2 {
		t.Fatalf("expected selection to be passed through")
	}
}
