// Package exportpdf provides server-side PDF rendering adapters for go-export.
//
// It renders HTML templates via an injected HTML renderer and converts them to
// PDFs using a pluggable engine (wkhtmltopdf or Chromium via chromedp).
// Rendering is gated by Renderer.Enabled.
package exportpdf
