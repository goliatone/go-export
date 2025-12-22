package exporttemplate

import (
	"context"
	"io"
	"sync/atomic"
	"time"

	"github.com/goliatone/go-export/export"
)

// DefaultMaxBufferedRows bounds template buffering by default.
const DefaultMaxBufferedRows = 10000

// TemplateExecutor executes a named template with data.
type TemplateExecutor interface {
	ExecuteTemplate(w io.Writer, name string, data any) error
}

// Strategy renders template output with a selectable buffering strategy.
type Strategy interface {
	Render(ctx context.Context, tmpl TemplateExecutor, name string, schema export.Schema, rows export.RowIterator, w io.Writer, opts export.RenderOptions) (export.RenderStats, error)
}

// BufferedStrategy collects rows in memory before executing the template.
// MaxRows controls the maximum number of rows buffered before returning an error.
type BufferedStrategy struct {
	MaxRows int
}

func (s BufferedStrategy) Render(ctx context.Context, tmpl TemplateExecutor, name string, schema export.Schema, rows export.RowIterator, w io.Writer, opts export.RenderOptions) (export.RenderStats, error) {
	maxRows := s.MaxRows
	if maxRows <= 0 {
		maxRows = DefaultMaxBufferedRows
	}

	data := TemplateData{Schema: schema}
	data.Columns = templateColumns(schema)
	data.TemplateMeta = templateMetaFromOptions(opts)
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
		if maxRows > 0 && len(data.Rows) >= maxRows {
			return export.RenderStats{}, export.NewError(export.KindValidation, "template renderer max rows exceeded", nil)
		}
		data.Rows = append(data.Rows, row)
	}
	data.RowCount = len(data.Rows)

	cw := &countingWriter{w: w}
	if err := tmpl.ExecuteTemplate(cw, name, data); err != nil {
		return export.RenderStats{}, err
	}

	return export.RenderStats{
		Rows:  int64(len(data.Rows)),
		Bytes: cw.count,
	}, nil
}

// StreamingStrategy streams rows into the template via a channel.
type StreamingStrategy struct{}

func (s StreamingStrategy) Render(ctx context.Context, tmpl TemplateExecutor, name string, schema export.Schema, rows export.RowIterator, w io.Writer, opts export.RenderOptions) (export.RenderStats, error) {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	rowsCh := make(chan export.Row)
	errCh := make(chan error, 1)
	var rowCount int64
	done := make(chan struct{})

	go func() {
		defer close(rowsCh)
		for {
			select {
			case <-done:
				return
			default:
			}
			if err := streamCtx.Err(); err != nil {
				select {
				case <-done:
				default:
					errCh <- err
				}
				return
			}
			row, err := rows.Next(streamCtx)
			if err != nil {
				if err != io.EOF {
					select {
					case <-done:
					default:
						errCh <- err
					}
				}
				return
			}
			atomic.AddInt64(&rowCount, 1)
			select {
			case <-done:
				return
			case rowsCh <- row:
			}
		}
	}()

	data := templateStreamData{
		Schema:       schema,
		Columns:      templateColumns(schema),
		TemplateMeta: templateMetaFromOptions(opts),
		Rows:         rowsCh,
	}
	cw := &countingWriter{w: w}
	if err := tmpl.ExecuteTemplate(cw, name, data); err != nil {
		close(done)
		cancel()
		return export.RenderStats{}, err
	}
	close(done)
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			return export.RenderStats{}, err
		}
	default:
	}

	return export.RenderStats{
		Rows:  atomic.LoadInt64(&rowCount),
		Bytes: cw.count,
	}, nil
}

type templateStreamData struct {
	Schema   export.Schema     `json:"schema"`
	Columns  []string          `json:"columns"`
	Rows     <-chan export.Row `json:"rows"`
	RowCount int               `json:"row_count"`
	TemplateMeta
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

func templateColumns(schema export.Schema) []string {
	columns := make([]string, 0, len(schema.Columns))
	for _, col := range schema.Columns {
		label := col.Label
		if label == "" {
			label = col.Name
		}
		columns = append(columns, label)
	}
	return columns
}

func templateMetaFromOptions(opts export.RenderOptions) TemplateMeta {
	meta := TemplateMeta{
		TemplateName: opts.Template.TemplateName,
		Layout:       opts.Template.Layout,
		Title:        opts.Template.Title,
		Definition:   opts.Template.Definition,
		GeneratedAt:  opts.Template.GeneratedAt,
		ChartConfig:  opts.Template.ChartConfig,
		Theme:        opts.Template.Theme,
		Header:       opts.Template.Header,
		Footer:       opts.Template.Footer,
		Data:         opts.Template.Data,
	}
	if meta.Title == "" {
		meta.Title = meta.Definition
	}
	if meta.GeneratedAt.IsZero() {
		meta.GeneratedAt = time.Now()
	}
	meta.Generated = meta.GeneratedAt.Format(time.RFC3339)
	return meta
}
