package exportpdf

import (
	"bytes"
	"context"
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

type stubHTMLRenderer struct {
	html  string
	stats export.RenderStats
	err   error
}

func (r stubHTMLRenderer) Render(ctx context.Context, schema export.Schema, rows export.RowIterator, w io.Writer, opts export.RenderOptions) (export.RenderStats, error) {
	_ = ctx
	_ = schema
	_ = rows
	_ = opts
	if r.err != nil {
		return export.RenderStats{}, r.err
	}
	if _, err := io.WriteString(w, r.html); err != nil {
		return export.RenderStats{}, err
	}
	return r.stats, nil
}

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

func TestRenderer_MissingHTMLRenderer(t *testing.T) {
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

func TestRenderer_MissingEngine(t *testing.T) {
	renderer := Renderer{
		Enabled:      true,
		HTMLRenderer: stubHTMLRenderer{html: "<html></html>"},
	}
	buf := &bytes.Buffer{}
	_, err := renderer.Render(context.Background(), export.Schema{}, &stubIterator{}, buf, export.RenderOptions{})
	if err == nil {
		t.Fatalf("expected error")
	}
	if export.KindFromError(err) != export.KindValidation {
		t.Fatalf("expected validation error, got %v", export.KindFromError(err))
	}
}

func TestRenderer_RendersPDF(t *testing.T) {
	engine := EngineFunc(func(ctx context.Context, req RenderRequest) ([]byte, error) {
		_ = ctx
		if string(req.HTML) != "<html>ok</html>" {
			return nil, export.NewError(export.KindValidation, "unexpected html", nil)
		}
		return []byte("%PDF-1.4"), nil
	})
	renderer := Renderer{
		Enabled:      true,
		HTMLRenderer: stubHTMLRenderer{html: "<html>ok</html>", stats: export.RenderStats{Rows: 2}},
		Engine:       engine,
	}
	buf := &bytes.Buffer{}
	stats, err := renderer.Render(context.Background(), export.Schema{}, &stubIterator{}, buf, export.RenderOptions{})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if stats.Rows != 2 {
		t.Fatalf("expected rows 2, got %d", stats.Rows)
	}
	if stats.Bytes != int64(len("%PDF-1.4")) {
		t.Fatalf("expected bytes %d, got %d", len("%PDF-1.4"), stats.Bytes)
	}
	if got := buf.String(); got != "%PDF-1.4" {
		t.Fatalf("unexpected output: %q", got)
	}
}

func TestRenderer_MaxHTMLBytes(t *testing.T) {
	renderer := Renderer{
		Enabled:      true,
		HTMLRenderer: stubHTMLRenderer{html: "0123456789"},
		Engine: EngineFunc(func(ctx context.Context, req RenderRequest) ([]byte, error) {
			_ = ctx
			_ = req
			return []byte("pdf"), nil
		}),
		MaxHTMLBytes: 4,
	}
	buf := &bytes.Buffer{}
	_, err := renderer.Render(context.Background(), export.Schema{}, &stubIterator{}, buf, export.RenderOptions{})
	if err == nil {
		t.Fatalf("expected error")
	}
	if export.KindFromError(err) != export.KindValidation {
		t.Fatalf("expected validation error, got %v", export.KindFromError(err))
	}
}
