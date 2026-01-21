# Export Definitions Guide

This guide covers how to define exportable datasets in go-export. Export definitions declare the schema, allowed formats, policies, and other metadata for each exportable dataset.

## Overview

An `ExportDefinition` is the central configuration object that describes:

- **What data** can be exported (via `RowSourceKey`)
- **What columns** are available (via `Schema`)
- **What formats** are allowed (via `AllowedFormats`)
- **What limits** apply (via `Policy`)
- **How delivery** should work (via `DeliveryPolicy`)

## ExportDefinition Structure

Here's the complete structure of an export definition:

```go
type ExportDefinition struct {
    Name             string              // Unique identifier
    Resource         string              // Optional resource name for lookup
    Schema           Schema              // Column definitions
    AllowedFormats   []Format            // Permitted output formats
    DefaultFilename  string              // Filename template
    RowSourceKey     string              // References registered row source
    Transformers     []TransformerConfig // Data transformations
    DefaultSelection Selection           // Default row selection
    SelectionPolicy  SelectionPolicy     // Dynamic selection provider
    SourceVariants   map[string]SourceVariant // Alternate configurations
    Policy           ExportPolicy        // Limits and redactions
    DeliveryPolicy   *DeliveryPolicy     // Sync/async thresholds
    Template         TemplateOptions     // Template/PDF options
}
```

## Basic Definition

The simplest definition requires only a name, row source key, and schema:

```go
package main

import (
    "github.com/goliatone/go-export/export"
)

func main() {
    registry := export.NewDefinitionRegistry()

    err := registry.Register(export.ExportDefinition{
        Name:         "users",
        RowSourceKey: "users-source",
        Schema: export.Schema{
            Columns: []export.Column{
                {Name: "id", Label: "ID"},
                {Name: "email", Label: "Email Address"},
                {Name: "name", Label: "Full Name"},
                {Name: "created_at", Label: "Created At"},
            },
        },
    })
    if err != nil {
        panic(err)
    }
}
```

When `AllowedFormats` is not specified, it defaults to CSV, JSON, NDJSON, and XLSX.

## Schema and Columns

The schema defines the columns available for export. Each column has:

```go
type Column struct {
    Name   string       // Internal column identifier
    Label  string       // Display label for headers
    Type   string       // Data type hint (string, number, integer, datetime)
    Format ColumnFormat // Renderer-specific formatting
}

type ColumnFormat struct {
    Layout string // Date/time layout (e.g., "2006-01-02")
    Number string // Number format (e.g., "#,##0.00")
    Excel  string // Excel-specific format code
}
```

### Column Types

The `Type` field provides hints to renderers for formatting:

- `"string"` - Text values (default)
- `"number"` - Floating point numbers
- `"integer"` - Whole numbers
- `"datetime"` - Date/time values

```go
Schema: export.Schema{
    Columns: []export.Column{
        {Name: "id", Label: "ID", Type: "string"},
        {Name: "price", Label: "Price", Type: "number"},
        {Name: "quantity", Label: "Quantity", Type: "integer"},
        {Name: "created_at", Label: "Created At", Type: "datetime"},
    },
},
```

### Column Formatting

Use `ColumnFormat` to customize how values are rendered:

```go
{
    Name:  "created_at",
    Label: "Created At",
    Type:  "datetime",
    Format: export.ColumnFormat{
        Layout: "Jan 2, 2006",           // Go time format for display
        Excel:  "yyyy-mm-dd hh:mm:ss",   // Excel format code
    },
},
{
    Name:  "price",
    Label: "Price",
    Type:  "number",
    Format: export.ColumnFormat{
        Number: "#,##0.00",              // Number formatting pattern
        Excel:  "$#,##0.00",             // Excel format with currency
    },
},
```

## Allowed Formats

Control which output formats are permitted:

```go
AllowedFormats: []export.Format{
    export.FormatCSV,
    export.FormatJSON,
    export.FormatXLSX,
},
```

Available formats:

| Format | Constant | Description |
|--------|----------|-------------|
| CSV | `FormatCSV` | Comma-separated values |
| JSON | `FormatJSON` | JSON array |
| NDJSON | `FormatNDJSON` | Newline-delimited JSON |
| XLSX | `FormatXLSX` | Excel spreadsheet |
| SQLite | `FormatSQLite` | SQLite database file |
| Template | `FormatTemplate` | Custom HTML template |
| PDF | `FormatPDF` | PDF via HTML template |

To enable template and PDF formats:

```go
AllowedFormats: []export.Format{
    export.FormatCSV,
    export.FormatJSON,
    export.FormatNDJSON,
    export.FormatXLSX,
    export.FormatTemplate,
    export.FormatPDF,
},
```

## Default Filename

Customize the exported filename using Go templates:

```go
DefaultFilename: "users-export-{{.Date}}"
```

Available template variables:

| Variable | Description | Example |
|----------|-------------|---------|
| `{{.Definition}}` | Definition name | `users` |
| `{{.Format}}` | Output format | `csv` |
| `{{.Timestamp}}` | UTC timestamp | `20240115T143022Z` |
| `{{.Date}}` | UTC date | `20240115` |
| `{{.Resource}}` | Resource name | `users` |
| `{{.Variant}}` | Source variant | `summary` |

The file extension is appended automatically based on the format.

**Examples:**

```go
// Simple with date: users-export-20240115.csv
DefaultFilename: "users-export-{{.Date}}"

// With timestamp: users_20240115T143022Z.xlsx
DefaultFilename: "{{.Definition}}_{{.Timestamp}}"

// Custom prefix: monthly-report-users-20240115.json
DefaultFilename: "monthly-report-{{.Definition}}-{{.Date}}"
```

## Row Source Key

The `RowSourceKey` references a registered row source that provides the data:

```go
// Definition references the source by key
registry.Register(export.ExportDefinition{
    Name:         "users",
    RowSourceKey: "users-source",  // Must match registered source
    // ...
})

// Row source is registered separately
sourceRegistry.Register("users-source", func(req export.ExportRequest, def export.ResolvedDefinition) (export.RowSource, error) {
    return myRowSource, nil
})
```

See [GUIDE_ROW_SOURCES.md](GUIDE_ROW_SOURCES.md) for details on implementing row sources.

## Source Variants

Variants allow alternate configurations for the same definition:

```go
registry.Register(export.ExportDefinition{
    Name:         "orders",
    RowSourceKey: "orders-source",
    Schema: export.Schema{
        Columns: []export.Column{
            {Name: "id", Label: "Order ID"},
            {Name: "customer", Label: "Customer"},
            {Name: "total", Label: "Total"},
            {Name: "status", Label: "Status"},
            {Name: "created_at", Label: "Created At"},
        },
    },
    SourceVariants: map[string]export.SourceVariant{
        "summary": {
            RowSourceKey:    "orders-summary-source",
            DefaultFilename: "orders-summary-{{.Date}}",
            Policy: &export.ExportPolicy{
                AllowedColumns: []string{"id", "total", "status"},
            },
        },
        "detailed": {
            RowSourceKey:    "orders-detailed-source",
            AllowedFormats:  []export.Format{export.FormatXLSX},
            DefaultFilename: "orders-detailed-{{.Date}}",
        },
    },
})
```

Request a variant via the `SourceVariant` field:

```go
result, err := runner.Run(ctx, export.ExportRequest{
    Definition:    "orders",
    SourceVariant: "summary",  // Use the summary variant
    Format:        export.FormatCSV,
    Output:        buf,
})
```

### Variant Override Rules

When a variant is requested, its fields override the base definition:

| Field | Behavior |
|-------|----------|
| `RowSourceKey` | Replaces base if non-empty |
| `AllowedFormats` | Replaces base if non-empty |
| `DefaultFilename` | Replaces base if non-empty |
| `Transformers` | Replaces base if non-empty |
| `Policy` | Merges with base (variant values take precedence) |
| `Template` | Merges with base (variant values take precedence) |

## Export Policy

Policies enforce limits and control column access:

```go
type ExportPolicy struct {
    AllowedColumns []string      // Columns users can request
    RedactColumns  []string      // Columns to mask in output
    RedactionValue any           // Replacement value (default: "[redacted]")
    MaxRows        int           // Maximum rows to export
    MaxBytes       int64         // Maximum output size
    MaxDuration    time.Duration // Maximum export time
}
```

### Column Restrictions

Limit which columns can be exported:

```go
Policy: export.ExportPolicy{
    AllowedColumns: []string{"id", "name", "email", "created_at"},
    // Columns not in this list cannot be requested
},
```

### Column Redaction

Mask sensitive columns while preserving column structure:

```go
Policy: export.ExportPolicy{
    RedactColumns:  []string{"email", "phone"},
    RedactionValue: "***",  // Optional custom value
},
```

Output will show `***` for email and phone columns instead of actual values.

### Export Limits

Protect against runaway exports:

```go
Policy: export.ExportPolicy{
    MaxRows:     10000,              // Cap at 10,000 rows
    MaxBytes:    50 * 1024 * 1024,   // Cap at 50MB
    MaxDuration: 2 * time.Minute,    // Cap at 2 minutes
},
```

Requests exceeding these limits will fail validation.

### Complete Policy Example

```go
registry.Register(export.ExportDefinition{
    Name:         "customers",
    RowSourceKey: "customers-source",
    Schema: export.Schema{
        Columns: []export.Column{
            {Name: "id", Label: "ID"},
            {Name: "name", Label: "Name"},
            {Name: "email", Label: "Email"},
            {Name: "ssn", Label: "SSN"},
            {Name: "created_at", Label: "Created At"},
        },
    },
    Policy: export.ExportPolicy{
        AllowedColumns: []string{"id", "name", "email", "created_at"},
        RedactColumns:  []string{"email"},
        MaxRows:        5000,
        MaxBytes:       10 * 1024 * 1024,
        MaxDuration:    30 * time.Second,
    },
})
```

## Delivery Policy

Control when exports use sync vs async delivery:

```go
type DeliveryPolicy struct {
    Default    DeliveryMode       // Default mode (sync, async, auto)
    Thresholds DeliveryThresholds // Auto-mode thresholds
}

type DeliveryThresholds struct {
    MaxRows     int           // Switch to async above this row count
    MaxBytes    int64         // Switch to async above this size
    MaxDuration time.Duration // Switch to async above this duration
}
```

### Configuring Auto Delivery

When `DeliveryMode` is `auto`, the runner evaluates estimated values against thresholds:

```go
DeliveryPolicy: &export.DeliveryPolicy{
    Default: export.DeliveryAuto,
    Thresholds: export.DeliveryThresholds{
        MaxRows:     1000,              // Async if > 1000 rows
        MaxBytes:    5 * 1024 * 1024,   // Async if > 5MB
        MaxDuration: 30 * time.Second,  // Async if > 30 seconds
    },
},
```

If `EstimatedRows`, `EstimatedBytes`, or `EstimatedDuration` in the request exceeds the threshold, the export runs asynchronously.

### Force Sync or Async

Override auto-detection:

```go
// Always sync (stream to response)
DeliveryPolicy: &export.DeliveryPolicy{
    Default: export.DeliverySync,
},

// Always async (write to artifact store)
DeliveryPolicy: &export.DeliveryPolicy{
    Default: export.DeliveryAsync,
},
```

## Template Options

Configure template and PDF rendering:

```go
type TemplateOptions struct {
    Strategy     TemplateStrategy   // "buffered" or "streaming"
    MaxRows      int                // Max rows for buffered strategy
    TemplateName string             // Template file name
    Layout       string             // Layout template name
    Title        string             // Export title
    Definition   string             // Definition name (auto-filled)
    GeneratedAt  time.Time          // Generation timestamp (auto-filled)
    ChartConfig  any                // Chart configuration data
    Theme        map[string]any     // Theme customization
    Header       map[string]any     // Header content
    Footer       map[string]any     // Footer content
    Data         map[string]any     // Additional template data
}
```

### Basic Template Configuration

```go
registry.Register(export.ExportDefinition{
    Name:           "users",
    RowSourceKey:   "users-source",
    AllowedFormats: []export.Format{
        export.FormatCSV,
        export.FormatTemplate,
        export.FormatPDF,
    },
    Template: export.TemplateOptions{
        TemplateName: "export",      // Uses templates/export/export.html
        Layout:       "default",     // Uses templates/export/default.html
        Title:        "Users Export",
    },
    Schema: export.Schema{
        Columns: []export.Column{
            {Name: "id", Label: "ID"},
            {Name: "name", Label: "Name"},
        },
    },
})
```

### Template Data

Pass custom data to templates:

```go
Template: export.TemplateOptions{
    TemplateName: "users-report",
    Title:        "Monthly User Report",
    Data: map[string]any{
        "company_name": "Acme Corp",
        "report_type":  "monthly",
        "pdf_assets_host": export.DefaultPDFAssetsHost(),
    },
    Header: map[string]any{
        "logo_url": "/assets/logo.png",
    },
    Footer: map[string]any{
        "copyright": "2024 Acme Corp",
    },
},
```

See [GUIDE_TEMPLATES.md](GUIDE_TEMPLATES.md) and [GUIDE_PDF.md](GUIDE_PDF.md) for more details.

## Default Selection

Configure default row selection when none is specified:

```go
registry.Register(export.ExportDefinition{
    Name:         "orders",
    RowSourceKey: "orders-source",
    DefaultSelection: export.Selection{
        Mode: export.SelectionAll,  // Export all rows by default
    },
    // ...
})
```

Selection modes:

| Mode | Description |
|------|-------------|
| `SelectionAll` | Export all rows |
| `SelectionIDs` | Export specific IDs |
| `SelectionQuery` | Export rows matching a named query |

### Dynamic Selection with SelectionPolicy

For dynamic default selections based on the actor or request:

```go
registry.Register(export.ExportDefinition{
    Name:         "orders",
    RowSourceKey: "orders-source",
    SelectionPolicy: export.SelectionPolicyFunc(func(
        ctx context.Context,
        actor export.Actor,
        req export.ExportRequest,
        def export.ResolvedDefinition,
    ) (export.Selection, bool, error) {
        // Return tenant-scoped selection
        return export.Selection{
            Mode: export.SelectionQuery,
            Query: export.SelectionQueryRef{
                Name:   "by-tenant",
                Params: map[string]any{"tenant_id": actor.Scope.TenantID},
            },
        }, true, nil
    }),
    // ...
})
```

## Registering Definitions

### Using DefinitionRegistry

```go
registry := export.NewDefinitionRegistry()

// Register multiple definitions
err := registry.Register(export.ExportDefinition{
    Name:         "users",
    RowSourceKey: "users-source",
    // ...
})
if err != nil {
    // Handle duplicate registration error
}

// Retrieve a definition
def, ok := registry.Get("users")
if !ok {
    // Definition not found
}

// List all definitions
defs := registry.List()  // Returns sorted by name

// Resolve with variant handling
resolved, err := registry.Resolve(export.ExportRequest{
    Definition:    "users",
    SourceVariant: "summary",
    Format:        export.FormatCSV,
})
```

### Lookup by Resource

For REST API patterns, look up definitions by resource name:

```go
registry.Register(export.ExportDefinition{
    Name:     "users",
    Resource: "users",  // Enables resource-based lookup
    // ...
})

// Find by resource
def, err := registry.ResolveByResource("users")
```

## Common Patterns

### Users Export

```go
registry.Register(export.ExportDefinition{
    Name:            "users",
    Resource:        "users",
    RowSourceKey:    "users-source",
    DefaultFilename: "users-export-{{.Date}}",
    AllowedFormats: []export.Format{
        export.FormatCSV,
        export.FormatJSON,
        export.FormatXLSX,
    },
    Schema: export.Schema{
        Columns: []export.Column{
            {Name: "id", Label: "ID", Type: "string"},
            {Name: "email", Label: "Email", Type: "string"},
            {Name: "name", Label: "Name", Type: "string"},
            {Name: "role", Label: "Role", Type: "string"},
            {Name: "created_at", Label: "Created At", Type: "datetime"},
        },
    },
    Policy: export.ExportPolicy{
        MaxRows: 10000,
    },
})
```

### Orders Export with Variants

```go
registry.Register(export.ExportDefinition{
    Name:            "orders",
    Resource:        "orders",
    RowSourceKey:    "orders-source",
    DefaultFilename: "orders-{{.Timestamp}}",
    AllowedFormats: []export.Format{
        export.FormatCSV,
        export.FormatJSON,
        export.FormatXLSX,
        export.FormatPDF,
    },
    Schema: export.Schema{
        Columns: []export.Column{
            {Name: "id", Label: "Order ID", Type: "string"},
            {Name: "customer", Label: "Customer", Type: "string"},
            {Name: "total", Label: "Total", Type: "number",
                Format: export.ColumnFormat{Number: "#,##0.00", Excel: "$#,##0.00"}},
            {Name: "status", Label: "Status", Type: "string"},
            {Name: "created_at", Label: "Created At", Type: "datetime"},
        },
    },
    SourceVariants: map[string]export.SourceVariant{
        "summary": {
            RowSourceKey:    "orders-summary-source",
            DefaultFilename: "orders-summary-{{.Date}}",
            Policy: &export.ExportPolicy{
                AllowedColumns: []string{"id", "total", "status"},
            },
        },
    },
    Policy: export.ExportPolicy{
        MaxRows:     50000,
        MaxBytes:    100 * 1024 * 1024,
        MaxDuration: 5 * time.Minute,
    },
    DeliveryPolicy: &export.DeliveryPolicy{
        Default: export.DeliveryAuto,
        Thresholds: export.DeliveryThresholds{
            MaxRows: 5000,
        },
    },
    Template: export.TemplateOptions{
        TemplateName: "orders-report",
        Title:        "Orders Report",
    },
})
```

### Products Export with Redaction

```go
registry.Register(export.ExportDefinition{
    Name:            "products",
    Resource:        "products",
    RowSourceKey:    "products-source",
    DefaultFilename: "products-catalog-{{.Date}}",
    AllowedFormats: []export.Format{
        export.FormatCSV,
        export.FormatXLSX,
    },
    Schema: export.Schema{
        Columns: []export.Column{
            {Name: "id", Label: "ID", Type: "string"},
            {Name: "name", Label: "Product Name", Type: "string"},
            {Name: "sku", Label: "SKU", Type: "string"},
            {Name: "cost", Label: "Cost", Type: "number"},
            {Name: "price", Label: "Price", Type: "number"},
            {Name: "quantity", Label: "Quantity", Type: "integer"},
        },
    },
    Policy: export.ExportPolicy{
        RedactColumns:  []string{"cost"},  // Hide cost from exports
        RedactionValue: "N/A",
        MaxRows:        100000,
    },
})
```

## Next Steps

- [GUIDE_ROW_SOURCES.md](GUIDE_ROW_SOURCES.md) - Implement row sources to provide data
- [GUIDE_RENDERERS.md](GUIDE_RENDERERS.md) - Configure output format renderers
- [GUIDE_TEMPLATES.md](GUIDE_TEMPLATES.md) - Create custom HTML templates
- [GUIDE_DELIVERY.md](GUIDE_DELIVERY.md) - Configure sync/async delivery
- [GUIDE_AUTHORIZATION.md](GUIDE_AUTHORIZATION.md) - Implement export guards
