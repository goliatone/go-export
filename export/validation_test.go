package export

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
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

func TestResolveExport_SelectionQueryRequiresName(t *testing.T) {
	def := ResolvedDefinition{
		ExportDefinition: ExportDefinition{
			Name:           "users",
			AllowedFormats: []Format{FormatCSV},
			Schema: Schema{Columns: []Column{
				{Name: "id"},
			}},
		},
	}

	_, err := ResolveExport(ExportRequest{
		Definition: "users",
		Format:     FormatCSV,
		Selection:  Selection{Mode: SelectionQuery},
	}, def, testNow())
	if err == nil {
		t.Fatalf("expected validation error")
	}
	if exportErr, ok := err.(*ExportError); !ok || exportErr.Kind != KindValidation {
		t.Fatalf("expected validation error, got %v", err)
	}
}

func TestResolveExport_TemplateDefaults(t *testing.T) {
	def := ResolvedDefinition{
		ExportDefinition: ExportDefinition{
			Name:           "users",
			AllowedFormats: []Format{FormatTemplate},
			Schema: Schema{Columns: []Column{
				{Name: "id"},
			}},
		},
	}

	resolved, err := ResolveExport(ExportRequest{
		Definition: "users",
		Format:     FormatTemplate,
	}, def, testNow())
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if resolved.Request.RenderOptions.Template.Definition != "users" {
		t.Fatalf("expected definition, got %q", resolved.Request.RenderOptions.Template.Definition)
	}
	if resolved.Request.RenderOptions.Template.Title != "users" {
		t.Fatalf("expected title, got %q", resolved.Request.RenderOptions.Template.Title)
	}
	if !resolved.Request.RenderOptions.Template.GeneratedAt.Equal(testNow()) {
		t.Fatalf("expected generated_at to match now")
	}
}

func TestResolveExport_PDFTemplateDefaults(t *testing.T) {
	def := ResolvedDefinition{
		ExportDefinition: ExportDefinition{
			Name:           "users",
			AllowedFormats: []Format{FormatPDF},
			Schema: Schema{Columns: []Column{
				{Name: "id"},
			}},
		},
	}

	resolved, err := ResolveExport(ExportRequest{
		Definition: "users",
		Format:     FormatPDF,
	}, def, testNow())
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if resolved.Request.RenderOptions.Template.Definition != "users" {
		t.Fatalf("expected definition, got %q", resolved.Request.RenderOptions.Template.Definition)
	}
	if resolved.Request.RenderOptions.Template.Title != "users" {
		t.Fatalf("expected title, got %q", resolved.Request.RenderOptions.Template.Title)
	}
	if !resolved.Request.RenderOptions.Template.GeneratedAt.Equal(testNow()) {
		t.Fatalf("expected generated_at to match now")
	}
}

func TestResolveExport_TemplateDefinitionDefaults(t *testing.T) {
	def := ResolvedDefinition{
		ExportDefinition: ExportDefinition{
			Name:           "users",
			AllowedFormats: []Format{FormatTemplate},
			Schema: Schema{Columns: []Column{
				{Name: "id"},
			}},
			Template: TemplateOptions{
				TemplateName: "summary",
				Layout:       "compact",
				ChartConfig:  map[string]any{"title": "Totals"},
				Theme:        map[string]any{"accent": "#111111", "bg": "#ffffff"},
				Header:       map[string]any{"title": "Summary Report"},
				Footer:       map[string]any{"note": "Confidential"},
				Data:         map[string]any{"pdf_assets_host": "/assets/"},
			},
		},
	}

	resolved, err := ResolveExport(ExportRequest{
		Definition: "users",
		Format:     FormatTemplate,
		RenderOptions: RenderOptions{
			Template: TemplateOptions{
				Layout: "detailed",
				Theme:  map[string]any{"accent": "#222222"},
				Header: map[string]any{"subtitle": "Q1"},
			},
		},
	}, def, testNow())
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if resolved.Request.RenderOptions.Template.TemplateName != "summary" {
		t.Fatalf("expected template_name, got %q", resolved.Request.RenderOptions.Template.TemplateName)
	}
	if resolved.Request.RenderOptions.Template.Layout != "detailed" {
		t.Fatalf("expected layout override, got %q", resolved.Request.RenderOptions.Template.Layout)
	}
	theme := resolved.Request.RenderOptions.Template.Theme
	if theme["accent"] != "#222222" || theme["bg"] != "#ffffff" {
		t.Fatalf("unexpected theme: %#v", theme)
	}
	header := resolved.Request.RenderOptions.Template.Header
	if header["title"] != "Summary Report" || header["subtitle"] != "Q1" {
		t.Fatalf("unexpected header: %#v", header)
	}
	if resolved.Request.RenderOptions.Template.Footer["note"] != "Confidential" {
		t.Fatalf("unexpected footer: %#v", resolved.Request.RenderOptions.Template.Footer)
	}
	if resolved.Request.RenderOptions.Template.Data["pdf_assets_host"] != "/assets/" {
		t.Fatalf("unexpected data: %#v", resolved.Request.RenderOptions.Template.Data)
	}
	chart, ok := resolved.Request.RenderOptions.Template.ChartConfig.(map[string]any)
	if !ok || chart["title"] != "Totals" {
		t.Fatalf("unexpected chart config: %#v", resolved.Request.RenderOptions.Template.ChartConfig)
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

func TestRunner_CSVHeadersOptOut(t *testing.T) {
	buf := &bytes.Buffer{}
	iter := &stubIterator{rows: []Row{{"1", "alice"}}}

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
		return &stubSource{iter: iter}, nil
	}); err != nil {
		t.Fatalf("register source: %v", err)
	}

	_, err := runner.Run(context.Background(), ExportRequest{
		Definition: "users",
		Format:     FormatCSV,
		Output:     buf,
		RenderOptions: RenderOptions{
			CSV: CSVOptions{
				IncludeHeaders: false,
				HeadersSet:     true,
			},
		},
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 1 || lines[0] != "1,alice" {
		t.Fatalf("expected csv without headers, got %q", buf.String())
	}
}
