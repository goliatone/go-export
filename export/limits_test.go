package export

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	errorslib "github.com/goliatone/go-errors"
)

func TestRunner_MaxBytesLimit(t *testing.T) {
	runner := NewRunner()
	if err := runner.Definitions.Register(ExportDefinition{
		Name:         "users",
		RowSourceKey: "stub",
		Schema:       Schema{Columns: []Column{{Name: "id"}}},
		Policy: ExportPolicy{
			MaxBytes: 1,
		},
	}); err != nil {
		t.Fatalf("register definition: %v", err)
	}
	if err := runner.RowSources.Register("stub", func(req ExportRequest, def ResolvedDefinition) (RowSource, error) {
		_ = req
		_ = def
		return &stubSource{iter: &stubIterator{rows: []Row{{"1"}}}}, nil
	}); err != nil {
		t.Fatalf("register source: %v", err)
	}

	_, err := runner.Run(context.Background(), ExportRequest{
		Definition: "users",
		Format:     FormatCSV,
		Output:     &bytes.Buffer{},
	})
	if err == nil {
		t.Fatalf("expected max bytes error")
	}
	var mapped *errorslib.Error
	if !errors.As(err, &mapped) {
		t.Fatalf("expected go-errors error, got %T", err)
	}
	if mapped.TextCode != "validation" {
		t.Fatalf("expected validation error, got %q", mapped.TextCode)
	}
}

func TestRunner_MaxDurationLimit(t *testing.T) {
	runner := NewRunner()
	if err := runner.Definitions.Register(ExportDefinition{
		Name:         "users",
		RowSourceKey: "stub",
		Schema:       Schema{Columns: []Column{{Name: "name"}}},
		Policy: ExportPolicy{
			MaxDuration: time.Millisecond,
		},
	}); err != nil {
		t.Fatalf("register definition: %v", err)
	}
	if err := runner.RowSources.Register("stub", func(req ExportRequest, def ResolvedDefinition) (RowSource, error) {
		_ = req
		_ = def
		return &stubSource{iter: &blockingIterator{}}, nil
	}); err != nil {
		t.Fatalf("register source: %v", err)
	}

	_, err := runner.Run(context.Background(), ExportRequest{
		Definition: "users",
		Format:     FormatJSON,
		Output:     &bytes.Buffer{},
	})
	if err == nil {
		t.Fatalf("expected timeout error")
	}
	var mapped *errorslib.Error
	if !errors.As(err, &mapped) {
		t.Fatalf("expected go-errors error, got %T", err)
	}
	if mapped.TextCode != "timeout" {
		t.Fatalf("expected timeout error, got %q", mapped.TextCode)
	}
}
