package exportpdf

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/goliatone/go-export/export"
)

func TestParseLengthInches(t *testing.T) {
	tests := []struct {
		input string
		want  float64
	}{
		{input: "1in", want: 1},
		{input: "25.4mm", want: 1},
		{input: "2.54cm", want: 1},
		{input: "72pt", want: 1},
		{input: "96px", want: 1},
		{input: "2", want: 2},
	}

	for _, tc := range tests {
		got, err := parseLengthInches(tc.input)
		if err != nil {
			t.Fatalf("parseLengthInches(%q): %v", tc.input, err)
		}
		if diff := got - tc.want; diff > 0.0001 || diff < -0.0001 {
			t.Fatalf("parseLengthInches(%q): expected %f, got %f", tc.input, tc.want, got)
		}
	}
}

func TestBuildPrintToPDFParams_PageSize(t *testing.T) {
	params, err := buildPrintToPDFParams(export.PDFOptions{
		PageSize:        "A4",
		PrintBackground: boolPtr(true),
		MarginTop:       "10mm",
	})
	if err != nil {
		t.Fatalf("buildPrintToPDFParams: %v", err)
	}
	if params.PaperWidth == 0 || params.PaperHeight == 0 {
		t.Fatalf("expected paper size to be set, got width=%f height=%f", params.PaperWidth, params.PaperHeight)
	}
	if params.MarginTop == 0 {
		t.Fatalf("expected margin top to be set")
	}
	if !params.PrintBackground {
		t.Fatalf("expected print background true")
	}
}

func TestInjectBaseURL(t *testing.T) {
	input := []byte("<html><head><title>Test</title></head><body>ok</body></html>")
	out := injectBaseURL(input, "https://assets.local/")
	if !bytes.Contains(out, []byte("<base")) {
		t.Fatalf("expected base tag to be injected")
	}
}

func TestChromiumEngine_Render_Smoke(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping chromium smoke test in short mode")
	}

	chromePath := os.Getenv("CHROME_BIN")
	if chromePath == "" {
		paths := []string{"google-chrome", "chromium", "chromium-browser"}
		for _, candidate := range paths {
			if path, err := exec.LookPath(candidate); err == nil {
				chromePath = path
				break
			}
		}
	}
	if chromePath == "" {
		t.Skip("chromium binary not found; set CHROME_BIN to run this test")
	}

	engine := &ChromiumEngine{
		BrowserPath: chromePath,
		Headless:    true,
		Timeout:     10 * time.Second,
		Args:        []string{"--no-sandbox", "--disable-dev-shm-usage"},
		DefaultPDF: export.PDFOptions{
			PrintBackground: boolPtr(true),
		},
	}

	pdf, err := engine.Render(context.Background(), RenderRequest{
		HTML: []byte("<html><body><h1>Hello</h1></body></html>"),
		Options: export.RenderOptions{
			PDF: export.PDFOptions{PageSize: "A4"},
		},
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if len(pdf) < 4 || string(pdf[:4]) != "%PDF" {
		t.Fatalf("expected pdf output, got %q", string(pdf[:4]))
	}
}
