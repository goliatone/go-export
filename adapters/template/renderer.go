package exporttemplate

import (
	"context"
	"io"
	"time"

	"github.com/goliatone/go-export/export"
)

// Renderer renders templated HTML exports.
type Renderer struct {
	Enabled      bool
	Templates    TemplateExecutor
	TemplateName string
	Strategy     Strategy
}

// TemplateData is the context passed to templates.
type TemplateMeta struct {
	TemplateName string         `json:"template_name,omitempty"`
	Layout       string         `json:"layout,omitempty"`
	Title        string         `json:"title,omitempty"`
	Definition   string         `json:"definition,omitempty"`
	Generated    string         `json:"generated,omitempty"`
	GeneratedAt  time.Time      `json:"generated_at,omitempty"`
	ChartConfig  any            `json:"chart_config,omitempty"`
	Theme        map[string]any `json:"theme,omitempty"`
	Header       map[string]any `json:"header,omitempty"`
	Footer       map[string]any `json:"footer,omitempty"`
	Data         map[string]any `json:"data,omitempty"`
}

type TemplateData struct {
	Schema   export.Schema `json:"schema"`
	Columns  []string      `json:"columns"`
	Rows     []export.Row  `json:"rows"`
	RowCount int           `json:"row_count"`
	TemplateMeta
}

// Render executes a template with the provided rows.
func (r Renderer) Render(ctx context.Context, schema export.Schema, rows export.RowIterator, w io.Writer, opts export.RenderOptions) (export.RenderStats, error) {
	if !r.Enabled {
		return export.RenderStats{}, export.NewError(export.KindNotImpl, "template renderer is disabled", nil)
	}
	if r.Templates == nil {
		return export.RenderStats{}, export.NewError(export.KindValidation, "template renderer requires templates", nil)
	}

	name := opts.Template.TemplateName
	if name == "" {
		name = r.TemplateName
	}
	if name == "" {
		name = "export"
	}

	strategy, err := r.resolveStrategy(opts)
	if err != nil {
		return export.RenderStats{}, err
	}
	return strategy.Render(ctx, r.Templates, name, schema, rows, w, opts)
}

func (r Renderer) resolveStrategy(opts export.RenderOptions) (Strategy, error) {
	if r.Strategy != nil {
		return r.Strategy, nil
	}

	switch opts.Template.Strategy {
	case "":
	case export.TemplateStrategyBuffered:
		return BufferedStrategy{MaxRows: opts.Template.MaxRows}, nil
	case export.TemplateStrategyStreaming:
		return StreamingStrategy{}, nil
	default:
		return nil, export.NewError(export.KindValidation, "unknown template strategy", nil)
	}

	if opts.Template.MaxRows > 0 {
		return BufferedStrategy{MaxRows: opts.Template.MaxRows}, nil
	}
	return BufferedStrategy{}, nil
}
