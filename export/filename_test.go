package export

import (
	"strings"
	"testing"
	"time"
)

func TestRenderFilename_TemplateUsesHTML(t *testing.T) {
	def := ResolvedDefinition{ExportDefinition: ExportDefinition{Name: "users"}}
	req := ExportRequest{Definition: "users", Format: FormatTemplate}
	now := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)

	name, err := renderFilename(def, req, now)
	if err != nil {
		t.Fatalf("render filename: %v", err)
	}
	if !strings.HasSuffix(name, ".html") {
		t.Fatalf("expected .html extension, got %q", name)
	}
}

func TestRenderFilename_PDFUsesPDF(t *testing.T) {
	def := ResolvedDefinition{ExportDefinition: ExportDefinition{Name: "users"}}
	req := ExportRequest{Definition: "users", Format: FormatPDF}
	now := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)

	name, err := renderFilename(def, req, now)
	if err != nil {
		t.Fatalf("render filename: %v", err)
	}
	if !strings.HasSuffix(name, ".pdf") {
		t.Fatalf("expected .pdf extension, got %q", name)
	}
}
