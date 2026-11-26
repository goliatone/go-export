package gotemplate

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"testing/fstest"

	exporttemplate "github.com/goliatone/go-export/adapters/template"
	"github.com/goliatone/go-export/export"
	template "github.com/goliatone/go-template"
)

func testEngine(t *testing.T) *template.Engine {
	t.Helper()
	engine, err := NewEngine(template.WithFS(fstest.MapFS{
		"report.tpl": {Data: []byte(`{{ title }}:{% for row in rows %}{{ row.1 }};{% endfor %}`)},
		"json.tpl":   {Data: []byte(`{{ data|to_json|safe }}`)},
	}))
	if err != nil {
		t.Fatal(err)
	}
	return engine
}

type rowIterator struct {
	rows  []export.Row
	calls int
}

func (r *rowIterator) Next(ctx context.Context) (export.Row, error) {
	r.calls++
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(r.rows) == 0 {
		return nil, io.EOF
	}
	row := r.rows[0]
	r.rows = r.rows[1:]
	return row, nil
}
func (*rowIterator) Close() error { return nil }

func TestPublishedRendererContract(t *testing.T) {
	renderer := exporttemplate.Renderer{Enabled: true, Templates: NewExecutor(testEngine(t)), TemplateName: "report"}
	var output bytes.Buffer
	stats, err := renderer.Render(context.Background(), export.Schema{}, &rowIterator{rows: []export.Row{{"1", "<Alice>"}, {"2", "Bob"}}}, &output, export.RenderOptions{Template: export.TemplateOptions{Title: "People"}})
	if err != nil {
		t.Fatal(err)
	}
	if got := output.String(); got != "People:&lt;Alice&gt;;Bob;" {
		t.Fatalf("rendered %q", got)
	}
	if stats.Rows != 2 || stats.Bytes != int64(output.Len()) {
		t.Fatalf("stats=%+v", stats)
	}
}

func TestRendererCancellationAndLimit(t *testing.T) {
	renderer := exporttemplate.Renderer{Enabled: true, Templates: NewExecutor(testEngine(t)), TemplateName: "report"}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rows := &rowIterator{rows: []export.Row{{"1", "Alice"}}}
	var output bytes.Buffer
	_, err := renderer.Render(ctx, export.Schema{}, rows, &output, export.RenderOptions{})
	if !errors.Is(err, context.Canceled) || rows.calls != 0 || output.Len() != 0 {
		t.Fatalf("cancellation err=%v calls=%d output=%q", err, rows.calls, output.String())
	}
	rows = &rowIterator{rows: []export.Row{{"1", "Alice"}, {"2", "Bob"}}}
	_, err = renderer.Render(context.Background(), export.Schema{}, rows, &output, export.RenderOptions{Template: export.TemplateOptions{MaxRows: 1}})
	if err == nil || output.Len() != 0 {
		t.Fatalf("limit err=%v output=%q", err, output.String())
	}
}

type errorWriter struct{ err error }

func (w errorWriter) Write([]byte) (int, error) { return 0, w.err }
func TestExecutorErrors(t *testing.T) {
	if err := (Executor{}).ExecuteTemplate(io.Discard, "report", nil); err == nil {
		t.Fatal("nil renderer accepted")
	}
	if err := RegisterToJSON(nil); err == nil {
		t.Fatal("nil engine accepted")
	}
	executor := NewExecutor(testEngine(t))
	if err := executor.ExecuteTemplate(io.Discard, "missing", nil); err == nil {
		t.Fatal("missing template accepted")
	}
	failure := errors.New("writer failed")
	if err := executor.ExecuteTemplate(errorWriter{failure}, "report", map[string]any{"title": "People"}); !errors.Is(err, failure) {
		t.Fatalf("writer error=%v", err)
	}
}
func TestJSONFilterAndRepeatedEngines(t *testing.T) {
	for range 2 {
		engine := testEngine(t)
		if err := RegisterToJSON(engine); err != nil {
			t.Fatal(err)
		}
		var output bytes.Buffer
		if err := NewExecutor(engine).ExecuteTemplate(&output, "json", map[string]any{"data": map[string]any{"name": "</script>"}}); err != nil {
			t.Fatal(err)
		}
		if output.String() != `{"name":"\u003c/script\u003e"}` {
			t.Fatalf("json=%q", output.String())
		}
		output.Reset()
		if err := NewExecutor(engine).ExecuteTemplate(&output, "json", map[string]any{"data": make(chan int)}); err == nil || !strings.Contains(err.Error(), "unsupported type") {
			t.Fatalf("marshal error=%v", err)
		}
	}
}
