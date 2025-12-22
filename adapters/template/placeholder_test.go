package exporttemplate

import (
	"bytes"
	"context"
	"html/template"
	"io"
	"testing"

	"github.com/goliatone/go-export/export"
)

type stubIterator struct {
	rows []export.Row
	idx  int
}

func (it *stubIterator) Next(ctx context.Context) (export.Row, error) {
	_ = ctx
	if it.idx >= len(it.rows) {
		return nil, io.EOF
	}
	row := it.rows[it.idx]
	it.idx++
	return row, nil
}

func (it *stubIterator) Close() error { return nil }

func TestRenderer_Disabled(t *testing.T) {
	renderer := Renderer{}
	buf := &bytes.Buffer{}
	_, err := renderer.Render(context.Background(), export.Schema{}, &stubIterator{}, buf, export.RenderOptions{})
	if err == nil {
		t.Fatalf("expected error")
	}
	if export.KindFromError(err) != export.KindNotImpl {
		t.Fatalf("expected not_implemented, got %v", export.KindFromError(err))
	}
}

func TestRenderer_MissingTemplates(t *testing.T) {
	renderer := Renderer{Enabled: true}
	buf := &bytes.Buffer{}
	_, err := renderer.Render(context.Background(), export.Schema{}, &stubIterator{}, buf, export.RenderOptions{})
	if err == nil {
		t.Fatalf("expected error")
	}
	if export.KindFromError(err) != export.KindValidation {
		t.Fatalf("expected validation error, got %v", export.KindFromError(err))
	}
}

func TestRenderer_BufferedMaxRows(t *testing.T) {
	tmpl := template.Must(template.New("export").Parse("{{range .Rows}}{{index . 0}}{{end}}"))
	renderer := Renderer{
		Enabled:   true,
		Templates: tmpl,
		Strategy:  BufferedStrategy{MaxRows: 1},
	}
	buf := &bytes.Buffer{}
	_, err := renderer.Render(context.Background(), export.Schema{}, &stubIterator{
		rows: []export.Row{{"1"}, {"2"}},
	}, buf, export.RenderOptions{})
	if err == nil {
		t.Fatalf("expected error")
	}
	if export.KindFromError(err) != export.KindValidation {
		t.Fatalf("expected validation error, got %v", export.KindFromError(err))
	}
}

func TestRenderer_OptionsBufferedMaxRows(t *testing.T) {
	tmpl := template.Must(template.New("export").Parse("{{range .Rows}}{{index . 0}}{{end}}"))
	renderer := Renderer{
		Enabled:   true,
		Templates: tmpl,
	}
	buf := &bytes.Buffer{}
	_, err := renderer.Render(context.Background(), export.Schema{}, &stubIterator{
		rows: []export.Row{{"1"}, {"2"}},
	}, buf, export.RenderOptions{
		Template: export.TemplateOptions{MaxRows: 1},
	})
	if err == nil {
		t.Fatalf("expected error")
	}
	if export.KindFromError(err) != export.KindValidation {
		t.Fatalf("expected validation error, got %v", export.KindFromError(err))
	}
}

func TestRenderer_OptionsStrategyStreaming(t *testing.T) {
	tmpl := template.Must(template.New("export").Parse("{{range .Rows}}{{index . 0}}{{end}}"))
	renderer := Renderer{
		Enabled:   true,
		Templates: tmpl,
	}
	buf := &bytes.Buffer{}
	stats, err := renderer.Render(context.Background(), export.Schema{}, &stubIterator{
		rows: []export.Row{{"1"}, {"2"}},
	}, buf, export.RenderOptions{
		Template: export.TemplateOptions{Strategy: export.TemplateStrategyStreaming},
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if stats.Rows != 2 {
		t.Fatalf("expected 2 rows, got %d", stats.Rows)
	}
	if got := buf.String(); got != "12" {
		t.Fatalf("unexpected output: %q", got)
	}
}

func TestRenderer_OptionsUnknownStrategy(t *testing.T) {
	tmpl := template.Must(template.New("export").Parse("{{range .Rows}}{{index . 0}}{{end}}"))
	renderer := Renderer{
		Enabled:   true,
		Templates: tmpl,
	}
	buf := &bytes.Buffer{}
	_, err := renderer.Render(context.Background(), export.Schema{}, &stubIterator{
		rows: []export.Row{{"1"}},
	}, buf, export.RenderOptions{
		Template: export.TemplateOptions{Strategy: "unknown"},
	})
	if err == nil {
		t.Fatalf("expected error")
	}
	if export.KindFromError(err) != export.KindValidation {
		t.Fatalf("expected validation error, got %v", export.KindFromError(err))
	}
}

func TestRenderer_BufferedRenders(t *testing.T) {
	tmpl := template.Must(template.New("export").Parse("{{range .Rows}}{{index . 1}}{{end}}"))
	renderer := Renderer{
		Enabled:   true,
		Templates: tmpl,
	}
	buf := &bytes.Buffer{}
	stats, err := renderer.Render(context.Background(), export.Schema{}, &stubIterator{
		rows: []export.Row{{"1", "alice"}, {"2", "bob"}},
	}, buf, export.RenderOptions{})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if stats.Rows != 2 {
		t.Fatalf("expected 2 rows, got %d", stats.Rows)
	}
	if got := buf.String(); got != "alicebob" {
		t.Fatalf("unexpected output: %q", got)
	}
}
