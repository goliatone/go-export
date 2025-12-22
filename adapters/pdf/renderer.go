package exportpdf

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/goliatone/go-export/export"
)

// DefaultMaxHTMLBytes guards in-memory HTML buffering before PDF conversion.
const DefaultMaxHTMLBytes int64 = 8 * 1024 * 1024

// RenderRequest contains HTML input and render options for PDF engines.
type RenderRequest struct {
	HTML    []byte
	Options export.RenderOptions
}

// Engine renders HTML content into PDF bytes.
type Engine interface {
	Render(ctx context.Context, req RenderRequest) ([]byte, error)
}

// EngineFunc adapts a function to an Engine.
type EngineFunc func(ctx context.Context, req RenderRequest) ([]byte, error)

func (f EngineFunc) Render(ctx context.Context, req RenderRequest) ([]byte, error) {
	if f == nil {
		return nil, errors.New("pdf engine func is nil")
	}
	return f(ctx, req)
}

// Renderer converts HTML templates into PDF output.
type Renderer struct {
	Enabled      bool
	HTMLRenderer export.Renderer
	Engine       Engine
	MaxHTMLBytes int64
}

// Render renders HTML using the configured HTML renderer and converts it to PDF.
func (r Renderer) Render(ctx context.Context, schema export.Schema, rows export.RowIterator, w io.Writer, opts export.RenderOptions) (export.RenderStats, error) {
	if !r.Enabled {
		return export.RenderStats{}, export.NewError(export.KindNotImpl, "pdf renderer is disabled", nil)
	}
	if r.HTMLRenderer == nil {
		return export.RenderStats{}, export.NewError(export.KindValidation, "pdf renderer requires html renderer", nil)
	}
	if r.Engine == nil {
		return export.RenderStats{}, export.NewError(export.KindValidation, "pdf renderer requires engine", nil)
	}

	buffer := newLimitedBuffer(r.MaxHTMLBytes)
	htmlStats, err := r.HTMLRenderer.Render(ctx, schema, rows, buffer, opts)
	if err != nil {
		return export.RenderStats{}, err
	}

	pdf, err := r.Engine.Render(ctx, RenderRequest{
		HTML:    buffer.Bytes(),
		Options: opts,
	})
	if err != nil {
		return export.RenderStats{}, err
	}

	cw := &countingWriter{w: w}
	if len(pdf) > 0 {
		if _, err := cw.Write(pdf); err != nil {
			return export.RenderStats{Rows: htmlStats.Rows, Bytes: cw.count}, err
		}
	}

	return export.RenderStats{Rows: htmlStats.Rows, Bytes: cw.count}, nil
}

// WKHTMLTOPDFEngine invokes wkhtmltopdf for HTML-to-PDF conversion.
type WKHTMLTOPDFEngine struct {
	Command string
	Args    []string
	Env     []string
	Timeout time.Duration
}

// Render executes wkhtmltopdf using stdin/stdout for HTML/PDF.
func (e WKHTMLTOPDFEngine) Render(ctx context.Context, req RenderRequest) ([]byte, error) {
	cmdPath := strings.TrimSpace(e.Command)
	if cmdPath == "" {
		cmdPath = "wkhtmltopdf"
	}
	if ctx == nil {
		ctx = context.Background()
	}
	cmdCtx := ctx
	if e.Timeout > 0 {
		var cancel context.CancelFunc
		cmdCtx, cancel = context.WithTimeout(ctx, e.Timeout)
		defer cancel()
	}

	args := append([]string{}, e.Args...)
	args = append(args, "-", "-")
	cmd := exec.CommandContext(cmdCtx, cmdPath, args...)
	if len(e.Env) > 0 {
		cmd.Env = append(os.Environ(), e.Env...)
	}
	cmd.Stdin = bytes.NewReader(req.HTML)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		message := strings.TrimSpace(stderr.String())
		if message == "" {
			message = "wkhtmltopdf failed"
		}
		return nil, export.NewError(export.KindInternal, message, err)
	}
	return stdout.Bytes(), nil
}

type limitedBuffer struct {
	buf     bytes.Buffer
	maxSize int64
}

func newLimitedBuffer(maxSize int64) *limitedBuffer {
	if maxSize <= 0 {
		maxSize = DefaultMaxHTMLBytes
	}
	return &limitedBuffer{maxSize: maxSize}
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	if b.maxSize > 0 && int64(b.buf.Len()+len(p)) > b.maxSize {
		return 0, export.NewError(export.KindValidation, "pdf renderer max html bytes exceeded", nil)
	}
	return b.buf.Write(p)
}

func (b *limitedBuffer) Bytes() []byte {
	return b.buf.Bytes()
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
