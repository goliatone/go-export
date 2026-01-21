# Getting Started with go-export

This guide will help you run your first export in under 5 minutes using go-export's streaming export engine.

## Overview

go-export provides a streaming export engine with:

- **Pluggable row sources** - Stream data from callbacks, repositories, SQL queries, or go-crud datagrids
- **Multiple output formats** - CSV, JSON, NDJSON, XLSX, HTML templates, and PDF
- **Sync and async delivery** - Stream directly to HTTP responses or generate artifacts for later download
- **Guard-first authorization** - Built-in authorization hooks for secure exports

## Installation

```bash
go get github.com/goliatone/go-export
```

## Minimal Setup

The simplest way to use go-export is with the `Runner` and a callback-based row source.

### Step 1: Create a Runner

The `Runner` is the core orchestrator that wires together definitions, row sources, and renderers:

```go
package main

import (
    "bytes"
    "context"
    "fmt"
    "io"

    "github.com/goliatone/go-export/export"
    exportcallback "github.com/goliatone/go-export/sources/callback"
)

func main() {
    // Create a new runner with default registries
    runner := export.NewRunner()
```

The runner comes pre-configured with CSV, JSON, NDJSON, and XLSX renderers.

### Step 2: Define Your Export

An `ExportDefinition` declares an exportable dataset with its schema:

```go
    // Register an export definition
    err := runner.Definitions.Register(export.ExportDefinition{
        Name:         "users",
        RowSourceKey: "users-source",
        Schema: export.Schema{
            Columns: []export.Column{
                {Name: "id", Label: "ID"},
                {Name: "email", Label: "Email Address"},
                {Name: "name", Label: "Full Name"},
                {Name: "role", Label: "Role"},
            },
        },
    })
    if err != nil {
        panic(err)
    }
```

Key fields:
- `Name` - Unique identifier for the export
- `RowSourceKey` - References the registered row source
- `Schema` - Defines columns and their metadata

### Step 3: Register a Row Source

Row sources stream data for exports. The callback source is the simplest way to provide data:

```go
    // Register a callback-based row source
    err = runner.RowSources.Register("users-source", func(req export.ExportRequest, def export.ResolvedDefinition) (export.RowSource, error) {
        return exportcallback.NewSource(func(ctx context.Context, spec export.RowSourceSpec) (export.RowIterator, error) {
            // Your data - could come from a database, API, etc.
            users := [][]any{
                {"1", "alice@example.com", "Alice Johnson", "admin"},
                {"2", "bob@example.com", "Bob Smith", "user"},
                {"3", "carol@example.com", "Carol Williams", "editor"},
            }

            idx := 0
            return &exportcallback.FuncIterator{
                NextFunc: func(ctx context.Context) (export.Row, error) {
                    if idx >= len(users) {
                        return nil, io.EOF // Signal end of data
                    }
                    row := users[idx]
                    idx++
                    return row, nil
                },
            }, nil
        }), nil
    })
    if err != nil {
        panic(err)
    }
```

The iterator pattern allows streaming large datasets without loading everything into memory.

### Step 4: Run the Export

Execute the export and capture the output:

```go
    // Create an output buffer
    buf := &bytes.Buffer{}

    // Run the export
    result, err := runner.Run(context.Background(), export.ExportRequest{
        Definition: "users",
        Format:     export.FormatCSV,
        Output:     buf,
    })
    if err != nil {
        panic(err)
    }

    fmt.Printf("Exported %d rows, %d bytes\n", result.Rows, result.Bytes)
    fmt.Println(buf.String())
}
```

## Complete Example

Here's the full working example:

```go
package main

import (
    "bytes"
    "context"
    "fmt"
    "io"

    "github.com/goliatone/go-export/export"
    exportcallback "github.com/goliatone/go-export/sources/callback"
)

func main() {
    // Create runner with default registries
    runner := export.NewRunner()

    // Register export definition
    _ = runner.Definitions.Register(export.ExportDefinition{
        Name:         "users",
        RowSourceKey: "users-source",
        Schema: export.Schema{
            Columns: []export.Column{
                {Name: "id", Label: "ID"},
                {Name: "email", Label: "Email Address"},
                {Name: "name", Label: "Full Name"},
                {Name: "role", Label: "Role"},
            },
        },
    })

    // Register callback row source
    _ = runner.RowSources.Register("users-source", func(req export.ExportRequest, def export.ResolvedDefinition) (export.RowSource, error) {
        return exportcallback.NewSource(func(ctx context.Context, spec export.RowSourceSpec) (export.RowIterator, error) {
            users := [][]any{
                {"1", "alice@example.com", "Alice Johnson", "admin"},
                {"2", "bob@example.com", "Bob Smith", "user"},
                {"3", "carol@example.com", "Carol Williams", "editor"},
            }

            idx := 0
            return &exportcallback.FuncIterator{
                NextFunc: func(ctx context.Context) (export.Row, error) {
                    if idx >= len(users) {
                        return nil, io.EOF
                    }
                    row := users[idx]
                    idx++
                    return row, nil
                },
            }, nil
        }), nil
    })

    // Run CSV export
    buf := &bytes.Buffer{}
    result, err := runner.Run(context.Background(), export.ExportRequest{
        Definition: "users",
        Format:     export.FormatCSV,
        Output:     buf,
    })
    if err != nil {
        panic(err)
    }

    fmt.Printf("Exported %d rows, %d bytes\n", result.Rows, result.Bytes)
    fmt.Println(buf.String())
}
```

**Output:**

```
Exported 3 rows, 126 bytes
id,email,name,role
1,alice@example.com,Alice Johnson,admin
2,bob@example.com,Bob Smith,user
3,carol@example.com,Carol Williams,editor
```

## Exporting to Different Formats

Change the `Format` field to export in different formats:

### JSON Export

```go
result, _ := runner.Run(ctx, export.ExportRequest{
    Definition: "users",
    Format:     export.FormatJSON,
    Output:     buf,
})
```

Output:
```json
[{"id":"1","email":"alice@example.com","name":"Alice Johnson","role":"admin"},{"id":"2","email":"bob@example.com","name":"Bob Smith","role":"user"},{"id":"3","email":"carol@example.com","name":"Carol Williams","role":"editor"}]
```

### NDJSON Export (Newline Delimited JSON)

```go
result, _ := runner.Run(ctx, export.ExportRequest{
    Definition: "users",
    Format:     export.FormatNDJSON,
    Output:     buf,
})
```

Output:
```json
{"id":"1","email":"alice@example.com","name":"Alice Johnson","role":"admin"}
{"id":"2","email":"bob@example.com","name":"Bob Smith","role":"user"}
{"id":"3","email":"carol@example.com","name":"Carol Williams","role":"editor"}
```

### XLSX Export

```go
file, _ := os.Create("users.xlsx")
defer file.Close()

result, _ := runner.Run(ctx, export.ExportRequest{
    Definition: "users",
    Format:     export.FormatXLSX,
    Output:     file,
})
```

## Selecting Columns

You can export a subset of columns:

```go
result, _ := runner.Run(ctx, export.ExportRequest{
    Definition: "users",
    Format:     export.FormatCSV,
    Columns:    []string{"email", "name"}, // Only these columns
    Output:     buf,
})
```

Output:
```csv
email,name
alice@example.com,Alice Johnson
bob@example.com,Bob Smith
carol@example.com,Carol Williams
```

## Basic Configuration Options

### CSV Options

```go
result, _ := runner.Run(ctx, export.ExportRequest{
    Definition: "users",
    Format:     export.FormatCSV,
    Output:     buf,
    RenderOptions: export.RenderOptions{
        CSV: export.CSVOptions{
            IncludeHeaders: true,  // Include header row (default: true)
            Delimiter:      ';',   // Use semicolon instead of comma
        },
    },
})
```

### JSON Options

```go
result, _ := runner.Run(ctx, export.ExportRequest{
    Definition: "users",
    Format:     export.FormatJSON,
    Output:     buf,
    RenderOptions: export.RenderOptions{
        JSON: export.JSONOptions{
            Mode: export.JSONModeArray,  // "array" (default), "ndjson", or "object"
        },
    },
})
```

### XLSX Options

```go
result, _ := runner.Run(ctx, export.ExportRequest{
    Definition: "users",
    Format:     export.FormatXLSX,
    Output:     file,
    RenderOptions: export.RenderOptions{
        XLSX: export.XLSXOptions{
            SheetName:      "Users",  // Custom sheet name
            IncludeHeaders: true,
        },
    },
})
```

## Adding Export Limits

Protect against runaway exports with policy limits:

```go
_ = runner.Definitions.Register(export.ExportDefinition{
    Name:         "users",
    RowSourceKey: "users-source",
    Schema: export.Schema{
        Columns: []export.Column{
            {Name: "id"},
            {Name: "email"},
        },
    },
    Policy: export.ExportPolicy{
        MaxRows:     1000,              // Maximum rows to export
        MaxBytes:    10 * 1024 * 1024,  // 10MB max file size
        MaxDuration: 30 * time.Second,  // 30 second timeout
    },
})
```

## Next Steps

Now that you've run your first export, explore these topics:

1. **[GUIDE_DEFINITIONS](GUIDE_DEFINITIONS.md)** - Learn about advanced definition configuration including schema types, variants, and policies

2. **[GUIDE_ROW_SOURCES](GUIDE_ROW_SOURCES.md)** - Explore different row sources:
   - `sources/callback` - Function-based sources (covered here)
   - `sources/crud` - go-crud datagrid integration
   - `sources/repo` - Repository-backed streaming
   - `sources/sql` - Named SQL queries

3. **[GUIDE_HTTP_API](GUIDE_HTTP_API.md)** - Set up HTTP endpoints for browser-based exports

4. **[GUIDE_DELIVERY](GUIDE_DELIVERY.md)** - Understand sync vs async delivery modes

5. **[GUIDE_TEMPLATES](GUIDE_TEMPLATES.md)** - Create custom HTML/PDF exports with templates

## Example Application

For a complete working example, see the `examples/web/` directory which demonstrates:

- HTTP API integration with Fiber
- Multiple export definitions (users, products, orders)
- Template and PDF exports
- Notifications and inbox integration
- Scheduled delivery

Run it with:

```bash
cd examples/web
./taskfile dev:serve
```

Then open http://localhost:8324 in your browser.

## Package Layout Reference

```
export/        Core runner, validation, registries, service, memory adapters
adapters/      HTTP handlers, job adapter, tracker, store, template, activity
sources/       Row sources: crud, repo, sql, callback
command/       go-command write handlers
query/         go-command read handlers
examples/      Wiring helpers + example app
```
