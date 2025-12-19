package export

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

type stubSource struct {
	order *[]string
	iter  RowIterator
}

func (s *stubSource) Open(ctx context.Context, spec RowSourceSpec) (RowIterator, error) {
	_ = ctx
	_ = spec
	if s.order != nil {
		*s.order = append(*s.order, "open")
	}
	return s.iter, nil
}

type stubIterator struct {
	rows   []Row
	index  int
	closed bool
}

func (it *stubIterator) Next(ctx context.Context) (Row, error) {
	_ = ctx
	if it.index >= len(it.rows) {
		return nil, io.EOF
	}
	row := it.rows[it.index]
	it.index++
	return row, nil
}

func (it *stubIterator) Close() error {
	it.closed = true
	return nil
}

type blockingIterator struct {
	calls  int
	closed bool
}

func (it *blockingIterator) Next(ctx context.Context) (Row, error) {
	it.calls++
	if it.calls == 1 {
		return Row{"a"}, nil
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func (it *blockingIterator) Close() error {
	it.closed = true
	return nil
}

type stubGuard struct {
	order *[]string
	err   error
}

func (g *stubGuard) AuthorizeExport(ctx context.Context, actor Actor, req ExportRequest, def ResolvedDefinition) error {
	_ = ctx
	_ = actor
	_ = req
	_ = def
	if g.order != nil {
		*g.order = append(*g.order, "guard")
	}
	return g.err
}

func (g *stubGuard) AuthorizeDownload(ctx context.Context, actor Actor, exportID string) error {
	_ = ctx
	_ = actor
	_ = exportID
	return nil
}

func TestRunner_GuardFirst(t *testing.T) {
	order := []string{}
	iter := &stubIterator{rows: []Row{{"1"}}}
	source := &stubSource{order: &order, iter: iter}
	guard := &stubGuard{order: &order}

	runner := NewRunner()
	runner.Guard = guard
	if err := runner.Definitions.Register(ExportDefinition{
		Name:         "users",
		RowSourceKey: "stub",
		Schema:       Schema{Columns: []Column{{Name: "id"}}},
	}); err != nil {
		t.Fatalf("register definition: %v", err)
	}
	if err := runner.RowSources.Register("stub", func(req ExportRequest, def ResolvedDefinition) (RowSource, error) {
		_ = req
		_ = def
		return source, nil
	}); err != nil {
		t.Fatalf("register source: %v", err)
	}

	buf := &bytes.Buffer{}
	_, err := runner.Run(context.Background(), ExportRequest{
		Definition: "users",
		Format:     FormatCSV,
		Output:     buf,
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(order) < 2 || order[0] != "guard" || order[1] != "open" {
		t.Fatalf("expected guard before open, got %v", order)
	}
}

func TestRunner_GuardBlocksOpen(t *testing.T) {
	order := []string{}
	source := &stubSource{order: &order, iter: &stubIterator{rows: []Row{{"1"}}}}
	guard := &stubGuard{order: &order, err: errors.New("deny")}

	runner := NewRunner()
	runner.Guard = guard
	if err := runner.Definitions.Register(ExportDefinition{
		Name:         "users",
		RowSourceKey: "stub",
		Schema:       Schema{Columns: []Column{{Name: "id"}}},
	}); err != nil {
		t.Fatalf("register definition: %v", err)
	}
	if err := runner.RowSources.Register("stub", func(req ExportRequest, def ResolvedDefinition) (RowSource, error) {
		_ = req
		_ = def
		return source, nil
	}); err != nil {
		t.Fatalf("register source: %v", err)
	}

	buf := &bytes.Buffer{}
	_, err := runner.Run(context.Background(), ExportRequest{
		Definition: "users",
		Format:     FormatCSV,
		Output:     buf,
	})
	if err == nil {
		t.Fatalf("expected error")
	}
	if len(order) != 1 || order[0] != "guard" {
		t.Fatalf("expected guard only, got %v", order)
	}
}

func TestRunner_EndToEndFormats(t *testing.T) {
	formats := []Format{FormatCSV, FormatJSON, FormatNDJSON}

	for _, format := range formats {
		buf := &bytes.Buffer{}
		iter := &stubIterator{rows: []Row{{"1", "alice"}, {"2", "bob"}}}
		source := &stubSource{iter: iter}

		runner := NewRunner()
		runner.Guard = &stubGuard{}
		if err := runner.Definitions.Register(ExportDefinition{
			Name:         "users",
			RowSourceKey: "stub",
			Schema: Schema{Columns: []Column{
				{Name: "id"},
				{Name: "name"},
			}},
		}); err != nil {
			t.Fatalf("register definition: %v", err)
		}
		if err := runner.RowSources.Register("stub", func(req ExportRequest, def ResolvedDefinition) (RowSource, error) {
			_ = req
			_ = def
			return source, nil
		}); err != nil {
			t.Fatalf("register source: %v", err)
		}

		_, err := runner.Run(context.Background(), ExportRequest{
			Definition: "users",
			Format:     format,
			Output:     buf,
		})
		if err != nil {
			t.Fatalf("run %s: %v", format, err)
		}

		output := buf.String()
		switch format {
		case FormatCSV:
			if !strings.Contains(output, "id,name") {
				t.Fatalf("expected csv headers, got %q", output)
			}
			if !strings.Contains(output, "1,alice") {
				t.Fatalf("expected csv row, got %q", output)
			}
		case FormatJSON:
			if !strings.HasPrefix(strings.TrimSpace(output), "[") {
				t.Fatalf("expected json array, got %q", output)
			}
		case FormatNDJSON:
			lines := strings.Split(strings.TrimSpace(output), "\n")
			if len(lines) != 2 {
				t.Fatalf("expected 2 ndjson lines, got %d", len(lines))
			}
		}
	}
}

func TestRunner_EndToEndXLSX(t *testing.T) {
	buf := &bytes.Buffer{}
	iter := &stubIterator{rows: []Row{{"1", "alice"}}}
	source := &stubSource{iter: iter}

	runner := NewRunner()
	if err := runner.Definitions.Register(ExportDefinition{
		Name:         "users",
		RowSourceKey: "stub",
		Schema: Schema{Columns: []Column{
			{Name: "id"},
			{Name: "name"},
		}},
	}); err != nil {
		t.Fatalf("register definition: %v", err)
	}
	if err := runner.RowSources.Register("stub", func(req ExportRequest, def ResolvedDefinition) (RowSource, error) {
		_ = req
		_ = def
		return source, nil
	}); err != nil {
		t.Fatalf("register source: %v", err)
	}

	_, err := runner.Run(context.Background(), ExportRequest{
		Definition: "users",
		Format:     FormatXLSX,
		Output:     buf,
	})
	if err != nil {
		t.Fatalf("run xlsx: %v", err)
	}
	if buf.Len() == 0 {
		t.Fatalf("expected xlsx output")
	}
}

func TestRunner_ContextCancelStopsIteration(t *testing.T) {
	iter := &blockingIterator{}
	source := &stubSource{iter: iter}
	buf := &bytes.Buffer{}

	runner := NewRunner()
	if err := runner.Definitions.Register(ExportDefinition{
		Name:         "users",
		RowSourceKey: "stub",
		Schema:       Schema{Columns: []Column{{Name: "name"}}},
	}); err != nil {
		t.Fatalf("register definition: %v", err)
	}
	if err := runner.RowSources.Register("stub", func(req ExportRequest, def ResolvedDefinition) (RowSource, error) {
		_ = req
		_ = def
		return source, nil
	}); err != nil {
		t.Fatalf("register source: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	time.AfterFunc(10*time.Millisecond, cancel)

	_, err := runner.Run(ctx, ExportRequest{
		Definition: "users",
		Format:     FormatJSON,
		Output:     buf,
	})
	if err == nil {
		t.Fatalf("expected cancellation error")
	}
}
