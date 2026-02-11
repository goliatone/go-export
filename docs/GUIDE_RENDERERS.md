# Renderers Guide

This guide covers the renderer system in go-export, which transforms row data into various output formats like CSV, JSON, XLSX, SQLite, HTML templates, and PDF.

## Overview

Renderers are responsible for:

- **Writing headers** (if applicable)
- **Streaming rows** from the iterator to the output
- **Applying formatting** based on column types and options
- **Tracking statistics** (rows written, bytes written)

The streaming design allows go-export to handle datasets of any size without memory constraints.

## Renderer Interface

All renderers implement the `Renderer` interface:

```go
type Renderer interface {
    Render(ctx context.Context, schema Schema, rows RowIterator, w io.Writer, opts RenderOptions) (RenderStats, error)
}

type RenderStats struct {
    Rows  int64  // Number of rows written
    Bytes int64  // Number of bytes written
}
```

The `Render` method receives:
- `ctx` - Context for cancellation
- `schema` - Column definitions
- `rows` - Iterator providing the data
- `w` - Output destination (file, HTTP response, buffer)
- `opts` - Format-specific configuration

## Built-in Renderers

| Renderer | Package | Format | Streaming | Description |
|----------|---------|--------|-----------|-------------|
| CSVRenderer | `export` | CSV | Yes | Comma-separated values |
| JSONRenderer | `export` | JSON/NDJSON | Yes | JSON array or newline-delimited |
| XLSXRenderer | `export` | XLSX | Yes | Excel spreadsheet |
| SQLite Renderer | `adapters/sqlite` | SQLite | Buffered | SQLite database file |
| Template Renderer | `adapters/template` | HTML | Configurable | HTML from templates |
| PDF Renderer | `adapters/pdf` | PDF | Buffered | PDF from HTML |

## CSV Renderer

The CSV renderer streams rows as comma-separated values.

### Basic Usage

```go
import "github.com/goliatone/go-export/export"

renderer := export.CSVRenderer{}

stats, err := renderer.Render(ctx, schema, rows, output, export.RenderOptions{
    CSV: export.CSVOptions{
        IncludeHeaders: true,
        Delimiter:      ',',
    },
})
```

### CSVOptions

```go
type CSVOptions struct {
    IncludeHeaders bool  // Include header row (default: true)
    Delimiter      rune  // Field separator (default: ',')
    HeadersSet     bool  // Internal flag for explicit header config
}
```

### Configuration Examples

```go
// Standard CSV with headers
opts := export.RenderOptions{
    CSV: export.CSVOptions{
        IncludeHeaders: true,
        Delimiter:      ',',
    },
}

// Semicolon-separated (European format)
opts := export.RenderOptions{
    CSV: export.CSVOptions{
        IncludeHeaders: true,
        Delimiter:      ';',
    },
}

// Tab-separated values (TSV)
opts := export.RenderOptions{
    CSV: export.CSVOptions{
        IncludeHeaders: true,
        Delimiter:      '\t',
    },
}

// Data only (no headers)
opts := export.RenderOptions{
    CSV: export.CSVOptions{
        IncludeHeaders: false,
        HeadersSet:     true,  // Explicit opt-out
    },
}
```

### Output Example

```csv
ID,Email,Name,Created At
1,alice@example.com,Alice Johnson,2024-01-15
2,bob@example.com,Bob Smith,2024-01-16
```

## JSON Renderer

The JSON renderer supports multiple output modes.

### JSONOptions

```go
type JSONOptions struct {
    Mode JSONMode  // Output mode
}

type JSONMode string

const (
    JSONModeArray  JSONMode = "array"   // JSON array (default)
    JSONModeLines  JSONMode = "ndjson"  // Newline-delimited JSON
    JSONModeObject JSONMode = "object"  // Single object (first row only)
)
```

### JSON Array Mode (Default)

```go
opts := export.RenderOptions{
    JSON: export.JSONOptions{
        Mode: export.JSONModeArray,
    },
}
```

Output:
```json
[{"id":"1","email":"alice@example.com","name":"Alice Johnson"},{"id":"2","email":"bob@example.com","name":"Bob Smith"}]
```

### NDJSON Mode

Newline-delimited JSON is ideal for streaming and log processing:

```go
opts := export.RenderOptions{
    JSON: export.JSONOptions{
        Mode: export.JSONModeLines,
    },
}
```

Output:
```json
{"id":"1","email":"alice@example.com","name":"Alice Johnson"}
{"id":"2","email":"bob@example.com","name":"Bob Smith"}
```

### Format Selection

Use `FormatNDJSON` for automatic NDJSON mode:

```go
// Explicit NDJSON format
request := export.ExportRequest{
    Definition: "users",
    Format:     export.FormatNDJSON,  // Automatically sets JSONModeLines
    Output:     buf,
}

// Or use FormatJSON with explicit mode
request := export.ExportRequest{
    Definition: "users",
    Format:     export.FormatJSON,
    Output:     buf,
    RenderOptions: export.RenderOptions{
        JSON: export.JSONOptions{
            Mode: export.JSONModeLines,
        },
    },
}
```

## XLSX Renderer

The XLSX renderer creates Excel spreadsheets using streaming writes.

### XLSXOptions

```go
type XLSXOptions struct {
    IncludeHeaders bool   // Include header row (default: true)
    HeadersSet     bool   // Internal flag for explicit config
    SheetName      string // Worksheet name (default: "Sheet1")
    MaxRows        int    // Maximum rows (default: Excel limit 1,048,576)
    MaxBytes       int64  // Maximum file size
}
```

### Basic Usage

```go
opts := export.RenderOptions{
    XLSX: export.XLSXOptions{
        IncludeHeaders: true,
        SheetName:      "Users",
    },
}
```

### Type-Aware Formatting

The XLSX renderer applies automatic formatting based on column types:

| Column Type | Excel Format |
|-------------|--------------|
| `date` | `yyyy-mm-dd` |
| `datetime`, `timestamp` | `yyyy-mm-dd hh:mm:ss` |
| `time` | `hh:mm:ss` |
| `float`, `number`, `decimal` | `0.00` |
| `string` | Text |
| `integer` | Number |
| `boolean` | Boolean |

### Custom Excel Formats

Use `ColumnFormat.Excel` for custom formatting:

```go
schema := export.Schema{
    Columns: []export.Column{
        {Name: "price", Type: "number", Format: export.ColumnFormat{Excel: "$#,##0.00"}},
        {Name: "percent", Type: "number", Format: export.ColumnFormat{Excel: "0.00%"}},
        {Name: "date", Type: "date", Format: export.ColumnFormat{Excel: "mmmm d, yyyy"}},
    },
}
```

### Row and Size Limits

```go
opts := export.RenderOptions{
    XLSX: export.XLSXOptions{
        MaxRows:  50000,              // Limit to 50,000 rows
        MaxBytes: 50 * 1024 * 1024,   // Limit to 50MB
    },
}
```

## SQLite Renderer

The SQLite renderer creates a portable database file. It's disabled by default and must be explicitly enabled.

### Setup

```go
import exportsqlite "github.com/goliatone/go-export/adapters/sqlite"

// Register the renderer
runner.Renderers.Register(export.FormatSQLite, exportsqlite.Renderer{
    Enabled: true,
})

// Include in allowed formats
registry.Register(export.ExportDefinition{
    Name: "users",
    AllowedFormats: []export.Format{
        export.FormatCSV,
        export.FormatSQLite,
    },
    // ...
})
```

### SQLiteOptions

```go
type SQLiteOptions struct {
    TableName string  // Table name (default: "data")
}
```

### Usage

```go
opts := export.RenderOptions{
    SQLite: export.SQLiteOptions{
        TableName: "users",
    },
}
```

### Type Mapping

| Column Type | SQLite Type |
|-------------|-------------|
| `bool`, `boolean` | INTEGER |
| `int`, `integer` | INTEGER |
| `float`, `number`, `decimal` | REAL |
| `date`, `datetime`, `time`, `string` | TEXT |

### Notes

- Uses the pure Go `modernc.org/sqlite` driver (no CGO required)
- Creates a temporary file during rendering, then streams to output
- Buffered operation (not streaming)

## Template Renderer

The template renderer generates HTML output using Pongo2 (Django-style) templates.

### Setup

```go
import exporttemplate "github.com/goliatone/go-export/adapters/template"

renderer := exporttemplate.Renderer{
    Enabled:      true,
    Templates:    templateExecutor,  // Your template engine
    TemplateName: "export",
}

runner.Renderers.Register(export.FormatTemplate, renderer)
```

### TemplateExecutor Interface

Implement this interface to provide templates:

```go
type TemplateExecutor interface {
    ExecuteTemplate(w io.Writer, name string, data any) error
}
```

### TemplateOptions

```go
type TemplateOptions struct {
    Strategy     TemplateStrategy   // "buffered" or "streaming"
    MaxRows      int                // Max rows for buffered strategy
    TemplateName string             // Template file name
    Layout       string             // Layout template name
    Title        string             // Export title
    Definition   string             // Definition name (auto-filled)
    GeneratedAt  time.Time          // Generation timestamp (auto-filled)
    ChartConfig  any                // Chart configuration
    Theme        map[string]any     // Theme customization
    Header       map[string]any     // Header content
    Footer       map[string]any     // Footer content
    Data         map[string]any     // Additional template data
}
```

### Template Data Context

Templates receive a `TemplateData` struct:

```go
type TemplateData struct {
    Schema   export.Schema  // Column definitions
    Columns  []string       // Column names
    Rows     []export.Row   // Row data (buffered) or channel (streaming)
    RowCount int            // Number of rows

    // From TemplateOptions
    TemplateName string
    Layout       string
    Title        string
    Definition   string
    GeneratedAt  time.Time
    Theme        map[string]any
    Header       map[string]any
    Footer       map[string]any
    Data         map[string]any
}
```

### Strategies

**Buffered (default):** Collects all rows before rendering.

```go
opts := export.RenderOptions{
    Template: export.TemplateOptions{
        Strategy: export.TemplateStrategyBuffered,
        MaxRows:  10000,
    },
}
```

**Streaming:** Passes a channel for row-by-row iteration.

```go
opts := export.RenderOptions{
    Template: export.TemplateOptions{
        Strategy: export.TemplateStrategyStreaming,
    },
}
```

### Example Template

```html
<!DOCTYPE html>
<html>
<head>
    <title>{{ Title }}</title>
</head>
<body>
    <h1>{{ Title }}</h1>
    <p>Generated: {{ GeneratedAt }}</p>

    <table>
        <thead>
            <tr>
                {% for col in Schema.Columns %}
                <th>{{ col.Label|default:col.Name }}</th>
                {% endfor %}
            </tr>
        </thead>
        <tbody>
            {% for row in Rows %}
            <tr>
                {% for value in row %}
                <td>{{ value }}</td>
                {% endfor %}
            </tr>
            {% endfor %}
        </tbody>
    </table>

    <p>Total: {{ RowCount }} rows</p>
</body>
</html>
```

## PDF Renderer

The PDF renderer converts HTML templates to PDF using a headless browser engine.

### Setup

```go
import exportpdf "github.com/goliatone/go-export/adapters/pdf"

pdfRenderer := exportpdf.Renderer{
    Enabled:      true,
    HTMLRenderer: templateRenderer,  // Template renderer for HTML
    Engine:       chromiumEngine,    // PDF engine
}

runner.Renderers.Register(export.FormatPDF, pdfRenderer)
```

### Engine Interface

```go
type Engine interface {
    Render(ctx context.Context, req RenderRequest) ([]byte, error)
}

type RenderRequest struct {
    HTML    []byte
    Options export.RenderOptions
}
```

### Chromium Engine

The recommended engine using headless Chrome/Chromium:

```go
engine := &exportpdf.ChromiumEngine{
    BrowserPath: "/usr/bin/chromium",  // Optional: path to browser
    Headless:    true,
    Timeout:     30 * time.Second,
    Args:        []string{"--no-sandbox", "--disable-gpu"},
    DefaultPDF: export.PDFOptions{
        PageSize:        "A4",
        PrintBackground: boolPtr(true),
    },
}
```

### wkhtmltopdf Engine (Legacy)

```go
engine := exportpdf.WKHTMLTOPDFEngine{
    Command: "wkhtmltopdf",
    Args:    []string{"--quiet"},
    Timeout: 30 * time.Second,
}
```

### PDFOptions

```go
type PDFOptions struct {
    PageSize             string                  // A3, A4, A5, LETTER, LEGAL
    Landscape            *bool                   // Landscape orientation
    PrintBackground      *bool                   // Print background colors
    Scale                float64                 // Scale factor (0.1-2.0)
    MarginTop            string                  // Top margin (e.g., "1in", "2cm")
    MarginBottom         string                  // Bottom margin
    MarginLeft           string                  // Left margin
    MarginRight          string                  // Right margin
    PreferCSSPageSize    *bool                   // Use CSS @page size
    BaseURL              string                  // Base URL for relative assets
    ExternalAssetsPolicy PDFExternalAssetsPolicy // Allow or block external assets
}

type PDFExternalAssetsPolicy string

const (
    PDFExternalAssetsUnspecified PDFExternalAssetsPolicy = ""
    PDFExternalAssetsAllow       PDFExternalAssetsPolicy = "allow"
    PDFExternalAssetsBlock       PDFExternalAssetsPolicy = "block"
)
```

### Supported Units

For margins:
- `in` - Inches (default)
- `cm` - Centimeters
- `mm` - Millimeters
- `pt` - Points (1/72 inch)
- `px` - Pixels (1/96 inch)

### Configuration Example

```go
opts := export.RenderOptions{
    PDF: export.PDFOptions{
        PageSize:        "A4",
        Landscape:       boolPtr(false),
        PrintBackground: boolPtr(true),
        Scale:           1.0,
        MarginTop:       "1cm",
        MarginBottom:    "1cm",
        MarginLeft:      "1.5cm",
        MarginRight:     "1.5cm",
        BaseURL:         "http://localhost:8080/assets/",
        ExternalAssetsPolicy: export.PDFExternalAssetsBlock,
    },
}

func boolPtr(b bool) *bool { return &b }
```

## Renderer Registry

Register and resolve renderers by format:

```go
registry := export.NewRendererRegistry()

// Register built-in renderers
registry.Register(export.FormatCSV, export.CSVRenderer{})
registry.Register(export.FormatJSON, export.JSONRenderer{})
registry.Register(export.FormatNDJSON, export.JSONRenderer{})
registry.Register(export.FormatXLSX, export.XLSXRenderer{})

// Register optional renderers
registry.Register(export.FormatSQLite, exportsqlite.Renderer{Enabled: true})
registry.Register(export.FormatTemplate, templateRenderer)
registry.Register(export.FormatPDF, pdfRenderer)

// Resolve by format
renderer, ok := registry.Resolve(export.FormatCSV)
```

## Implementing Custom Renderers

### Basic Custom Renderer

```go
type XMLRenderer struct{}

func (r XMLRenderer) Render(ctx context.Context, schema export.Schema, rows export.RowIterator, w io.Writer, opts export.RenderOptions) (export.RenderStats, error) {
    stats := export.RenderStats{}

    // Write XML header
    if _, err := w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>\n<records>\n`)); err != nil {
        return stats, err
    }

    // Stream rows
    for {
        // Check for cancellation
        if err := ctx.Err(); err != nil {
            return stats, err
        }

        row, err := rows.Next(ctx)
        if err != nil {
            if err == io.EOF {
                break
            }
            return stats, err
        }

        // Validate row length
        if len(row) != len(schema.Columns) {
            return stats, export.NewError(export.KindValidation, "row length mismatch", nil)
        }

        // Write record
        if _, err := w.Write([]byte("  <record>\n")); err != nil {
            return stats, err
        }
        for i, col := range schema.Columns {
            value := fmt.Sprintf("%v", row[i])
            if _, err := fmt.Fprintf(w, "    <%s>%s</%s>\n", col.Name, escapeXML(value), col.Name); err != nil {
                return stats, err
            }
        }
        if _, err := w.Write([]byte("  </record>\n")); err != nil {
            return stats, err
        }
        stats.Rows++
    }

    // Write footer
    if _, err := w.Write([]byte("</records>\n")); err != nil {
        return stats, err
    }

    return stats, nil
}

func escapeXML(s string) string {
    // Escape special XML characters
    s = strings.ReplaceAll(s, "&", "&amp;")
    s = strings.ReplaceAll(s, "<", "&lt;")
    s = strings.ReplaceAll(s, ">", "&gt;")
    return s
}
```

### Tracking Bytes Written

Use a counting writer to track bytes:

```go
type countingWriter struct {
    w     io.Writer
    count int64
}

func (cw *countingWriter) Write(p []byte) (int, error) {
    n, err := cw.w.Write(p)
    cw.count += int64(n)
    return n, err
}

func (r MyRenderer) Render(ctx context.Context, schema export.Schema, rows export.RowIterator, w io.Writer, opts export.RenderOptions) (export.RenderStats, error) {
    cw := &countingWriter{w: w}
    stats := export.RenderStats{}

    // Write to cw instead of w
    // ...

    stats.Bytes = cw.count
    return stats, nil
}
```

### Enforcing Size Limits

```go
type limitedWriter struct {
    w     io.Writer
    count int64
    limit int64
}

func (lw *limitedWriter) Write(p []byte) (int, error) {
    if lw.limit > 0 && lw.count+int64(len(p)) > lw.limit {
        return 0, export.NewError(export.KindValidation, "max bytes exceeded", nil)
    }
    n, err := lw.w.Write(p)
    lw.count += int64(n)
    return n, err
}
```

## Format Options

Configure locale and timezone formatting:

```go
type FormatOptions struct {
    Locale   string  // Locale for formatting (e.g., "en-US")
    Timezone string  // Timezone for dates (e.g., "America/New_York")
}
```

Usage:
```go
opts := export.RenderOptions{
    Format: export.FormatOptions{
        Locale:   "en-US",
        Timezone: "America/New_York",
    },
}
```

## Streaming vs Buffered Rendering

| Strategy | Memory | Use Case |
|----------|--------|----------|
| Streaming | Low | CSV, JSON, XLSX - large datasets |
| Buffered | High | Templates, PDF - need all data upfront |

### Streaming Renderers

CSV, JSON, and XLSX renderers stream data row-by-row:

```go
for {
    row, err := rows.Next(ctx)
    if err == io.EOF {
        break
    }
    // Write row immediately
}
```

### Buffered Renderers

Template and PDF renderers collect all rows first:

```go
var allRows []export.Row
for {
    row, err := rows.Next(ctx)
    if err == io.EOF {
        break
    }
    allRows = append(allRows, row)
}
// Render with all data
```

## Next Steps

- [GUIDE_DEFINITIONS.md](GUIDE_DEFINITIONS.md) - Configure export definitions
- [GUIDE_ROW_SOURCES.md](GUIDE_ROW_SOURCES.md) - Implement row sources
- [GUIDE_TEMPLATES.md](GUIDE_TEMPLATES.md) - Create custom HTML templates
- [GUIDE_PDF.md](GUIDE_PDF.md) - Advanced PDF configuration
- [GUIDE_DELIVERY.md](GUIDE_DELIVERY.md) - Sync and async delivery modes
