package export

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestTransformerPipelineRunnerIntegration(t *testing.T) {
	runner := NewRunner()
	if err := registerPipelineTransformers(runner); err != nil {
		t.Fatalf("register transformers: %v", err)
	}

	if err := runner.Definitions.Register(ExportDefinition{
		Name:         "users",
		RowSourceKey: "stub",
		Schema: Schema{Columns: []Column{
			{Name: "id"},
			{Name: "email"},
		}},
		Transformers: []TransformerConfig{
			{Key: "normalize"},
			{Key: "augment"},
			{Key: "filter"},
		},
	}); err != nil {
		t.Fatalf("register definition: %v", err)
	}

	iter := &stubIterator{rows: []Row{
		{"1", " Alice@Example.com "},
		{"2", "   "},
	}}
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

	output := strings.TrimSpace(buf.String())
	if !strings.Contains(output, "id,email,domain") {
		t.Fatalf("expected transformed schema headers, got %q", output)
	}
	if !strings.Contains(output, "1,alice@example.com,example.com") {
		t.Fatalf("expected transformed row, got %q", output)
	}
	if strings.Contains(output, "2,") {
		t.Fatalf("expected filtered rows to be removed, got %q", output)
	}
}

func TestTransformerPipelineServiceIntegration(t *testing.T) {
	runner := NewRunner()
	if err := registerPipelineTransformers(runner); err != nil {
		t.Fatalf("register transformers: %v", err)
	}

	if err := runner.Definitions.Register(ExportDefinition{
		Name:         "users",
		RowSourceKey: "stub",
		Schema: Schema{Columns: []Column{
			{Name: "id"},
			{Name: "email"},
		}},
		Transformers: []TransformerConfig{
			{Key: "normalize"},
			{Key: "augment"},
			{Key: "filter"},
		},
	}); err != nil {
		t.Fatalf("register definition: %v", err)
	}

	iter := &stubIterator{rows: []Row{
		{"1", " Alice@Example.com "},
		{"2", "   "},
	}}
	if err := runner.RowSources.Register("stub", func(req ExportRequest, def ResolvedDefinition) (RowSource, error) {
		_ = req
		_ = def
		return &stubSource{iter: iter}, nil
	}); err != nil {
		t.Fatalf("register source: %v", err)
	}

	svc := NewService(ServiceConfig{Runner: runner})
	buf := &bytes.Buffer{}
	_, err := svc.RequestExport(context.Background(), Actor{ID: "actor-1"}, ExportRequest{
		Definition: "users",
		Format:     FormatCSV,
		Output:     buf,
	})
	if err != nil {
		t.Fatalf("request export: %v", err)
	}
	output := strings.TrimSpace(buf.String())
	if !strings.Contains(output, "id,email,domain") {
		t.Fatalf("expected transformed schema headers, got %q", output)
	}
	if !strings.Contains(output, "1,alice@example.com,example.com") {
		t.Fatalf("expected transformed row, got %q", output)
	}
}

func TestTransformerConfigSerializationRoundTrip(t *testing.T) {
	configs := []TransformerConfig{
		{Key: "normalize", Params: map[string]any{"mode": "lower"}},
	}
	payload, err := json.Marshal(configs)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded []TransformerConfig
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	runner := NewRunner()
	if err := runner.Transformers.Register("normalize", func(cfg TransformerConfig) (RowTransformer, error) {
		mode, _ := cfg.Params["mode"].(string)
		if mode == "" {
			return nil, errors.New("missing mode")
		}
		return NewMapTransformer(func(ctx context.Context, row Row) (Row, error) {
			_ = ctx
			return row, nil
		}), nil
	}); err != nil {
		t.Fatalf("register transformer: %v", err)
	}

	transformers, err := runner.resolveTransformers(ResolvedDefinition{
		ExportDefinition: ExportDefinition{Transformers: decoded},
	})
	if err != nil {
		t.Fatalf("resolve transformers: %v", err)
	}
	if len(transformers) != 1 {
		t.Fatalf("expected transformer to resolve, got %d", len(transformers))
	}
}

func registerPipelineTransformers(runner *Runner) error {
	if err := runner.Transformers.Register("normalize", func(cfg TransformerConfig) (RowTransformer, error) {
		_ = cfg
		return NewMapTransformer(func(ctx context.Context, row Row) (Row, error) {
			_ = ctx
			next := append(Row(nil), row...)
			if len(next) > 1 {
				next[1] = strings.ToLower(strings.TrimSpace(stringify(next[1])))
			}
			return next, nil
		}), nil
	}); err != nil {
		return err
	}

	if err := runner.Transformers.Register("augment", func(cfg TransformerConfig) (RowTransformer, error) {
		_ = cfg
		return NewAugmentTransformer([]Column{{Name: "domain"}}, func(ctx context.Context, row Row) ([]any, error) {
			_ = ctx
			email := stringify(row[1])
			domain := ""
			if parts := strings.SplitN(email, "@", 2); len(parts) == 2 {
				domain = parts[1]
			}
			return []any{domain}, nil
		}), nil
	}); err != nil {
		return err
	}

	if err := runner.Transformers.Register("filter", func(cfg TransformerConfig) (RowTransformer, error) {
		_ = cfg
		return NewFilterTransformer(func(ctx context.Context, row Row) (bool, error) {
			_ = ctx
			email := stringify(row[1])
			return email != "", nil
		}), nil
	}); err != nil {
		return err
	}

	return nil
}
