// Package exportpdf provides server-side PDF rendering adapters for go-export.
//
// It renders HTML templates via an injected HTML renderer and converts them to
// PDFs using a pluggable engine (wkhtmltopdf, chromedp, rod). Rendering is
// gated by Renderer.Enabled.
package exportpdf
