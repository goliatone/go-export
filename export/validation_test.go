package export

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestResolveExport_DisallowedColumn(t *testing.T) {
	def := ResolvedDefinition{
		ExportDefinition: ExportDefinition{
			Name:           "users",
			AllowedFormats: []Format{FormatCSV},
			Schema: Schema{Columns: []Column{
				{Name: "id"},
				{Name: "secret"},
			}},
			Policy: ExportPolicy{AllowedColumns: []string{"id"}},
		},
	}

	_, err := ResolveExport(ExportRequest{
		Definition: "users",
		Format:     FormatCSV,
		Columns:    []string{"id", "secret"},
	}, def, testNow())
	if err == nil {
		t.Fatalf("expected validation error")
	}
	if exportErr, ok := err.(*ExportError); !ok || exportErr.Kind != KindValidation {
		t.Fatalf("expected validation error, got %v", err)
	}
}

func TestRunner_RedactsColumns(t *testing.T) {
	buf := &bytes.Buffer{}
	iter := &stubIterator{rows: []Row{{"1", "secret"}}}

	runner := NewRunner()
	if err := runner.Definitions.Register(ExportDefinition{
		Name:         "users",
		RowSourceKey: "stub",
		Schema: Schema{Columns: []Column{
			{Name: "id"},
			{Name: "token"},
		}},
		Policy: ExportPolicy{
			AllowedColumns: []string{"id", "token"},
			RedactColumns:  []string{"token"},
			RedactionValue: "MASK",
		},
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

	_, err := runner.Run(context.Background(), ExportRequest{
		Definition: "users",
		Format:     FormatJSON,
		Output:     buf,
		Columns:    []string{"id", "token"},
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	var payload []map[string]any
	if err := json.Unmarshal(buf.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(payload) != 1 {
		t.Fatalf("expected 1 row")
	}
	if payload[0]["token"] != "MASK" {
		t.Fatalf("expected redacted value, got %v", payload[0]["token"])
	}
}

func testNow() time.Time {
	return time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
}
