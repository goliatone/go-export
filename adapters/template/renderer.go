package exporttemplate

import (
	"context"
	"html/template"
	"io"

	"github.com/goliatone/go-export/export"
)

// Renderer renders templated HTML exports.
type Renderer struct {
	Enabled      bool
	Templates    *template.Template
	TemplateName string
}

// TemplateData is the context passed to templates.
type TemplateData struct {
	Schema export.Schema
	Rows   []export.Row
}

// Render executes a template with the provided rows.
func (r Renderer) Render(ctx context.Context, schema export.Schema, rows export.RowIterator, w io.Writer, opts export.RenderOptions) (export.RenderStats, error) {
	_ = opts
	if !r.Enabled {
		return export.RenderStats{}, export.NewError(export.KindNotImpl, "template renderer is disabled", nil)
	}
	if r.Templates == nil {
		return export.RenderStats{}, export.NewError(export.KindValidation, "template renderer requires templates", nil)
	}

	name := r.TemplateName
	if name == "" {
		name = "export"
	}

	data := TemplateData{Schema: schema}
	for {
		if err := ctx.Err(); err != nil {
			return export.RenderStats{}, err
		}

		row, err := rows.Next(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}
			return export.RenderStats{}, err
		}
		data.Rows = append(data.Rows, row)
	}

	cw := &countingWriter{w: w}
	if err := r.Templates.ExecuteTemplate(cw, name, data); err != nil {
		return export.RenderStats{}, err
	}

	return export.RenderStats{
		Rows:  int64(len(data.Rows)),
		Bytes: cw.count,
	}, nil
}

type countingWriter struct {
	w     io.Writer
	count int64
}

func (cw *countingWriter) Write(p []byte) (int, error) {
	n, err := cw.w.Write(p)
	cw.count += int64(n)
	return n, err
}
