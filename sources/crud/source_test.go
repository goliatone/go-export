package exportcrud

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"testing"

	"github.com/goliatone/go-export/export"
)

type captureStreamer struct {
	spec  Spec
	iter  export.RowIterator
	calls int
}

func (s *captureStreamer) Stream(ctx context.Context, spec Spec) (export.RowIterator, error) {
	_ = ctx
	s.calls++
	s.spec = spec
	if s.iter != nil {
		return s.iter, nil
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

func TestSource_UsesStableOrderingDefault(t *testing.T) {
	streamer := &captureStreamer{}
	source := NewSource(streamer, Config{PrimaryKey: "id"})

	_, err := source.Open(context.Background(), export.RowSourceSpec{
		Request: export.ExportRequest{
			Definition: "users",
			Selection:  export.Selection{Mode: export.SelectionAll},
		},
		Columns: []export.Column{{Name: "id"}},
		Actor:   export.Actor{ID: "actor-1", Scope: export.Scope{TenantID: "tenant-1"}},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if len(streamer.spec.Query.Sort) != 1 || streamer.spec.Query.Sort[0].Field != "id" {
		t.Fatalf("expected default sort by primary key")
	}
	if streamer.spec.Scope.TenantID != "tenant-1" {
		t.Fatalf("expected scope injection")
	}
}

func TestSource_PreservesSortsFromQuery(t *testing.T) {
	streamer := &captureStreamer{}
	source := NewSource(streamer, Config{PrimaryKey: "id"})

	query := Query{Sort: []Sort{{Field: "created_at", Desc: true}}}
	_, err := source.Open(context.Background(), export.RowSourceSpec{
		Request: export.ExportRequest{
			Definition: "users",
			Query:      query,
		},
		Columns: []export.Column{{Name: "id"}},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if len(streamer.spec.Query.Sort) != 1 || streamer.spec.Query.Sort[0].Field != "created_at" {
		t.Fatalf("expected sort to be preserved")
	}
}

func TestSource_SelectionIDsPassThrough(t *testing.T) {
	streamer := &captureStreamer{}
	source := NewSource(streamer, Config{})

	_, err := source.Open(context.Background(), export.RowSourceSpec{
		Request: export.ExportRequest{
			Definition: "users",
			Selection:  export.Selection{Mode: export.SelectionIDs, IDs: []string{"1", "2"}},
		},
		Columns: []export.Column{{Name: "id"}},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if streamer.spec.Selection.Mode != export.SelectionIDs || len(streamer.spec.Selection.IDs) != 2 {
		t.Fatalf("expected selection IDs to pass through")
	}
}

func TestSource_DoesNotPrefetchRows(t *testing.T) {
	iter := &countingIterator{}
	streamer := &captureStreamer{iter: iter}
	source := NewSource(streamer, Config{})

	_, err := source.Open(context.Background(), export.RowSourceSpec{
		Request: export.ExportRequest{Definition: "users"},
		Columns: []export.Column{{Name: "id"}},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if iter.calls != 0 {
		t.Fatalf("expected no Next calls during Open")
	}
}

func TestQueryFixture_Decodes(t *testing.T) {
	data, err := os.ReadFile("testdata/query.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var query Query
	if err := json.Unmarshal(data, &query); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(query.Filters) != 1 || query.Filters[0].Field != "status" {
		t.Fatalf("unexpected fixture data")
	}
	if len(query.Sort) != 1 || !query.Sort[0].Desc {
		t.Fatalf("expected sort to be decoded")
	}
}

type guardStub struct {
	order *[]string
}

func (g guardStub) AuthorizeExport(ctx context.Context, actor export.Actor, req export.ExportRequest, def export.ResolvedDefinition) error {
	_ = ctx
	_ = actor
	_ = req
	_ = def
	*g.order = append(*g.order, "guard")
	return nil
}

func (g guardStub) AuthorizeDownload(ctx context.Context, actor export.Actor, exportID string) error {
	_ = ctx
	_ = actor
	_ = exportID
	return nil
}

type orderStreamer struct {
	order *[]string
	iter  export.RowIterator
}

func (s orderStreamer) Stream(ctx context.Context, spec Spec) (export.RowIterator, error) {
	_ = ctx
	_ = spec
	*s.order = append(*s.order, "stream")
	return s.iter, nil
}

func TestRunner_GuardBeforeCrudStream(t *testing.T) {
	order := []string{}
	iter := &sliceIterator{rows: []export.Row{{"1"}}}
	streamer := orderStreamer{order: &order, iter: iter}

	runner := export.NewRunner()
	runner.Guard = guardStub{order: &order}
	if err := runner.Definitions.Register(export.ExportDefinition{
		Name:         "users",
		RowSourceKey: "crud",
		Schema: export.Schema{Columns: []export.Column{
			{Name: "id"},
		}},
	}); err != nil {
		t.Fatalf("register definition: %v", err)
	}
	if err := runner.RowSources.Register("crud", func(req export.ExportRequest, def export.ResolvedDefinition) (export.RowSource, error) {
		_ = req
		_ = def
		return NewSource(streamer, Config{}), nil
	}); err != nil {
		t.Fatalf("register source: %v", err)
	}

	buf := &bytes.Buffer{}
	_, err := runner.Run(context.Background(), export.ExportRequest{
		Definition: "users",
		Format:     export.FormatCSV,
		Output:     buf,
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(order) != 2 || order[0] != "guard" || order[1] != "stream" {
		t.Fatalf("expected guard before stream, got %v", order)
	}
}
