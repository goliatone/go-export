package export

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	errorslib "github.com/goliatone/go-errors"
)

type stubBufferedTransformer struct {
	process func(ctx context.Context, rows RowIterator, schema Schema) ([]Row, Schema, error)
}

func (t stubBufferedTransformer) Process(ctx context.Context, rows RowIterator, schema Schema) ([]Row, Schema, error) {
	if t.process == nil {
		return nil, Schema{}, nil
	}
	return t.process(ctx, rows, schema)
}

type signalIterator struct {
	started chan struct{}
	calls   int
}

func (it *signalIterator) Next(ctx context.Context) (Row, error) {
	it.calls++
	if it.calls == 1 {
		close(it.started)
		return Row{"1"}, nil
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func (it *signalIterator) Close() error {
	return nil
}

func TestRunner_TransformerOrder(t *testing.T) {
	runner := NewRunner()
	if err := runner.Transformers.Register("first", func(cfg TransformerConfig) (RowTransformer, error) {
		_ = cfg
		return NewMapTransformer(func(ctx context.Context, row Row) (Row, error) {
			_ = ctx
			next := append(Row(nil), row...)
			next[0] = fmt.Sprintf("%s-first", next[0])
			return next, nil
		}), nil
	}); err != nil {
		t.Fatalf("register first: %v", err)
	}
	if err := runner.Transformers.Register("second", func(cfg TransformerConfig) (RowTransformer, error) {
		_ = cfg
		return NewMapTransformer(func(ctx context.Context, row Row) (Row, error) {
			_ = ctx
			next := append(Row(nil), row...)
			next[0] = fmt.Sprintf("%s-second", next[0])
			return next, nil
		}), nil
	}); err != nil {
		t.Fatalf("register second: %v", err)
	}

	if err := runner.Definitions.Register(ExportDefinition{
		Name:         "users",
		RowSourceKey: "stub",
		Schema:       Schema{Columns: []Column{{Name: "name"}}},
		Transformers: []TransformerConfig{
			{Key: "first"},
			{Key: "second"},
		},
	}); err != nil {
		t.Fatalf("register definition: %v", err)
	}
	iter := &stubIterator{rows: []Row{{"alice"}}}
	if err := runner.RowSources.Register("stub", func(req ExportRequest, def ResolvedDefinition) (RowSource, error) {
		_ = req
		_ = def
		return &stubSource{iter: iter}, nil
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
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) < 2 {
		t.Fatalf("expected rows, got %q", buf.String())
	}
	if !strings.Contains(lines[1], "alice-first-second") {
		t.Fatalf("expected transformed value, got %q", lines[1])
	}
}

func TestRunner_TransformerSchemaChange(t *testing.T) {
	runner := NewRunner()
	if err := runner.Transformers.Register("augment", func(cfg TransformerConfig) (RowTransformer, error) {
		_ = cfg
		return NewAugmentTransformer([]Column{{Name: "extra"}}, func(ctx context.Context, row Row) ([]any, error) {
			_ = ctx
			_ = row
			return []any{"x"}, nil
		}), nil
	}); err != nil {
		t.Fatalf("register augment: %v", err)
	}

	if err := runner.Definitions.Register(ExportDefinition{
		Name:         "users",
		RowSourceKey: "stub",
		Schema:       Schema{Columns: []Column{{Name: "id"}}},
		Transformers: []TransformerConfig{{Key: "augment"}},
	}); err != nil {
		t.Fatalf("register definition: %v", err)
	}
	iter := &stubIterator{rows: []Row{{"1"}}}
	if err := runner.RowSources.Register("stub", func(req ExportRequest, def ResolvedDefinition) (RowSource, error) {
		_ = req
		_ = def
		return &stubSource{iter: iter}, nil
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
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) < 2 {
		t.Fatalf("expected rows, got %q", buf.String())
	}
	if !strings.Contains(lines[0], "id,extra") {
		t.Fatalf("expected updated headers, got %q", lines[0])
	}
	if !strings.Contains(lines[1], "1,x") {
		t.Fatalf("expected augmented row, got %q", lines[1])
	}
}

func TestRunner_TransformerErrorPropagation(t *testing.T) {
	runner := NewRunner()
	if err := runner.Transformers.Register("boom", func(cfg TransformerConfig) (RowTransformer, error) {
		_ = cfg
		return NewMapTransformer(func(ctx context.Context, row Row) (Row, error) {
			_ = ctx
			_ = row
			return nil, NewError(KindValidation, "boom", nil)
		}), nil
	}); err != nil {
		t.Fatalf("register boom: %v", err)
	}

	if err := runner.Definitions.Register(ExportDefinition{
		Name:         "users",
		RowSourceKey: "stub",
		Schema:       Schema{Columns: []Column{{Name: "id"}}},
		Transformers: []TransformerConfig{{Key: "boom"}},
	}); err != nil {
		t.Fatalf("register definition: %v", err)
	}
	iter := &stubIterator{rows: []Row{{"1"}}}
	if err := runner.RowSources.Register("stub", func(req ExportRequest, def ResolvedDefinition) (RowSource, error) {
		_ = req
		_ = def
		return &stubSource{iter: iter}, nil
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
	var mapped *errorslib.Error
	if !errors.As(err, &mapped) {
		t.Fatalf("expected go-errors error, got %T", err)
	}
	if mapped.TextCode != "validation" {
		t.Fatalf("expected validation error, got %q", mapped.TextCode)
	}
}

func TestRunner_TransformerUnknownKey(t *testing.T) {
	runner := NewRunner()
	if err := runner.Definitions.Register(ExportDefinition{
		Name:         "users",
		RowSourceKey: "stub",
		Schema:       Schema{Columns: []Column{{Name: "id"}}},
		Transformers: []TransformerConfig{{Key: "missing"}},
	}); err != nil {
		t.Fatalf("register definition: %v", err)
	}
	iter := &stubIterator{rows: []Row{{"1"}}}
	if err := runner.RowSources.Register("stub", func(req ExportRequest, def ResolvedDefinition) (RowSource, error) {
		_ = req
		_ = def
		return &stubSource{iter: iter}, nil
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
	var mapped *errorslib.Error
	if !errors.As(err, &mapped) {
		t.Fatalf("expected go-errors error, got %T", err)
	}
	if mapped.TextCode != "validation" {
		t.Fatalf("expected validation error, got %q", mapped.TextCode)
	}
}

func TestRunner_TransformerInvalidConfig(t *testing.T) {
	runner := NewRunner()
	if err := runner.Transformers.Register("needs_param", func(cfg TransformerConfig) (RowTransformer, error) {
		if cfg.Params == nil || cfg.Params["mode"] == "" {
			return nil, errors.New("missing mode")
		}
		return NewMapTransformer(func(ctx context.Context, row Row) (Row, error) {
			_ = ctx
			return row, nil
		}), nil
	}); err != nil {
		t.Fatalf("register transformer: %v", err)
	}

	if err := runner.Definitions.Register(ExportDefinition{
		Name:         "users",
		RowSourceKey: "stub",
		Schema:       Schema{Columns: []Column{{Name: "id"}}},
		Transformers: []TransformerConfig{{Key: "needs_param"}},
	}); err != nil {
		t.Fatalf("register definition: %v", err)
	}
	iter := &stubIterator{rows: []Row{{"1"}}}
	if err := runner.RowSources.Register("stub", func(req ExportRequest, def ResolvedDefinition) (RowSource, error) {
		_ = req
		_ = def
		return &stubSource{iter: iter}, nil
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
	var mapped *errorslib.Error
	if !errors.As(err, &mapped) {
		t.Fatalf("expected go-errors error, got %T", err)
	}
	if mapped.TextCode != "validation" {
		t.Fatalf("expected validation error, got %q", mapped.TextCode)
	}
}

func TestRunner_BufferedTransformerLimit(t *testing.T) {
	runner := NewRunner()
	if err := runner.Transformers.RegisterBuffered("buffered", func(cfg TransformerConfig) (BufferedTransformer, error) {
		_ = cfg
		return stubBufferedTransformer{
			process: func(ctx context.Context, rows RowIterator, schema Schema) ([]Row, Schema, error) {
				collected := []Row{}
				for {
					row, err := rows.Next(ctx)
					if err != nil {
						if err == io.EOF {
							break
						}
						return nil, Schema{}, err
					}
					collected = append(collected, row)
				}
				return collected, schema, nil
			},
		}, nil
	}); err != nil {
		t.Fatalf("register buffered: %v", err)
	}

	if err := runner.Definitions.Register(ExportDefinition{
		Name:         "users",
		RowSourceKey: "stub",
		Schema:       Schema{Columns: []Column{{Name: "id"}}},
		Policy:       ExportPolicy{MaxRows: 1},
		Transformers: []TransformerConfig{{Key: "buffered"}},
	}); err != nil {
		t.Fatalf("register definition: %v", err)
	}
	iter := &stubIterator{rows: []Row{{"1"}, {"2"}}}
	if err := runner.RowSources.Register("stub", func(req ExportRequest, def ResolvedDefinition) (RowSource, error) {
		_ = req
		_ = def
		return &stubSource{iter: iter}, nil
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
	if !strings.Contains(err.Error(), "buffered transform max rows exceeded") {
		t.Fatalf("expected limit error, got %v", err)
	}
}

func TestRunner_BufferedTransformerCancel(t *testing.T) {
	runner := NewRunner()
	if err := runner.Transformers.RegisterBuffered("buffered", func(cfg TransformerConfig) (BufferedTransformer, error) {
		_ = cfg
		return stubBufferedTransformer{
			process: func(ctx context.Context, rows RowIterator, schema Schema) ([]Row, Schema, error) {
				collected := []Row{}
				for {
					row, err := rows.Next(ctx)
					if err != nil {
						if err == io.EOF {
							break
						}
						return nil, Schema{}, err
					}
					collected = append(collected, row)
				}
				return collected, schema, nil
			},
		}, nil
	}); err != nil {
		t.Fatalf("register buffered: %v", err)
	}

	started := make(chan struct{})
	iter := &signalIterator{started: started}
	if err := runner.Definitions.Register(ExportDefinition{
		Name:         "users",
		RowSourceKey: "stub",
		Schema:       Schema{Columns: []Column{{Name: "id"}}},
		Transformers: []TransformerConfig{{Key: "buffered"}},
	}); err != nil {
		t.Fatalf("register definition: %v", err)
	}
	if err := runner.RowSources.Register("stub", func(req ExportRequest, def ResolvedDefinition) (RowSource, error) {
		_ = req
		_ = def
		return &stubSource{iter: iter}, nil
	}); err != nil {
		t.Fatalf("register source: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		buf := &bytes.Buffer{}
		_, err := runner.Run(ctx, ExportRequest{
			Definition: "users",
			Format:     FormatCSV,
			Output:     buf,
		})
		errCh <- err
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for transformer")
	}
	cancel()

	select {
	case err := <-errCh:
		var mapped *errorslib.Error
		if !errors.As(err, &mapped) {
			t.Fatalf("expected go-errors error, got %T", err)
		}
		if mapped.TextCode != "canceled" {
			t.Fatalf("expected canceled error, got %q", mapped.TextCode)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for cancellation")
	}
}
