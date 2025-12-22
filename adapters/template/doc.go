// Package exporttemplate provides templated renderer adapters for go-export.
//
// Renderer is disabled by default; set Renderer.Enabled to true and supply
// Templates (TemplateExecutor). The default template name is "export".
//
// Templates can use Go's html/template or Django/Pongo2-style syntax via a
// compatible executor wrapper (for example, wrapping pongo2 or
// github.com/gofiber/template/django/v3). BufferedStrategy is the default and
// enforces bounded buffering (DefaultMaxBufferedRows); StreamingStrategy streams
// rows through a channel so templates can range over .Rows without loading all
// rows into memory (channel-based rows work best with range blocks).
//
// For server-side PDF output, pair the template renderer with adapters/pdf
// (wkhtmltopdf or a custom chromedp/rod engine).
package exporttemplate
