# go-export

go-export provides a streaming export engine with guard-first authorization, pluggable row sources, and sync/async delivery options for go-crud datagrids and custom datasets.

## Features
- Core runner + service layer with transport-agnostic interfaces.
- Row sources for go-crud, repositories, named SQL queries, and callbacks.
- Renderers for CSV, JSON/NDJSON, XLSX, plus optional templated outputs.
- Sync downloads or async artifact generation with idempotency and retries.
- Progress tracking, retention cleanup hooks, and observability events/metrics.

## Package Layout
```
export/        Core runner, validation, registries, service, memory adapters
adapters/      exportapi (shared transport), router, http, job, tracker, store, template, activity, delivery adapters
sources/       crud, repo, sql, callback row sources
command/       go-command commands
query/         go-command queries
examples/      wiring helpers + app example
```

## Core Concepts

### Definitions and Registry
Exports are declared via `ExportDefinition` and registered in a `DefinitionRegistry`:
- `Name`, `Resource`, `Schema` (columns + types)
- `AllowedFormats`, `DefaultFilename`
- `RowSourceKey` and optional `SourceVariants`
- `Transformers` ordered pipeline (resolved via transformer registry)
- `Policy` (allowed columns, redactions, max rows/bytes/duration)

### Export Requests
`ExportRequest` captures the datagrid view:
- `Definition`, `SourceVariant`, `Format`
- `Query` (filters/sorts/search)
- `Selection` (`all`, `ids`, or `query`)
- `Columns` projection
- `Locale`/`Timezone` formatting hints
- `Delivery` (`sync|async|auto`)
- `IdempotencyKey` for async requests
- `RenderOptions` (CSV/JSON/XLSX/Template) are JSON-serializable for storage

### Selection Semantics
- `SelectionAll` exports the full filtered view.
- `SelectionIDs` exports only the explicit list of IDs.
- `SelectionQuery` executes a named server-registered selection query with validated params.
- Projection defaults to the schema order; policy can restrict columns.
- Selection defaults are applied after `Guard.AuthorizeExport` and before `RowSource.Open`.
- Default resolution order: `SelectionPolicy` (if set) → `DefaultSelection` → `SelectionAll`.
- Idempotency signatures are computed from the original request before defaults are applied.

### Datagrid Client Helper
Use the embedded datagrid client helper (source in `export/assets/datagrid_export_client.js`) in a go-admin DataGrid page to build an `exportcrud.DatagridRequest` payload.
```js
import { buildDatagridExportRequest } from './datagrid_export_client.js';

const payload = buildDatagridExportRequest(grid, {
  definition: 'users',
  format: 'xlsx',
  delivery: 'auto'
});

fetch('/admin/exports', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify(payload)
});
```
History is served at `GET /admin/exports/history`. Querystring exports are supported via `GET /admin/exports?definition=users&format=csv`.
To ship the helper with a Go app (go-formgen pattern), serve the embedded assets from `export.RuntimeAssetsFS()`:
```go
mux.Handle("/assets/",
  http.StripPrefix("/assets/",
    http.FileServer(http.FS(export.RuntimeAssetsFS())),
  ),
)
```
Then load it in your template:
```html
<script type="module" src="/assets/datagrid_export_client.js"></script>
```

### HTTP API Contract
Default base path is `/admin/exports` (configurable on the handler).

Endpoints:
- `POST /admin/exports` creates an export (sync stream or async record).
- `GET /admin/exports?definition=users&format=csv` runs a querystring export.
- `GET /admin/exports/history` lists export history.
- `GET /admin/exports/{id}` returns status + artifact metadata when available.
- `GET /admin/exports/{id}/download` streams or redirects to the artifact.
- `DELETE /admin/exports/{id}` deletes an export artifact.

Request payload (JSON). Provide `definition` or `resource` (definition takes precedence):
```json
{
  "definition": "users",
  "source_variant": "inactive",
  "format": "csv",
  "query": {
    "filters": [{"field": "status", "op": "eq", "value": "inactive"}],
    "sort": [{"field": "email", "desc": false}]
  },
  "selection": {"mode": "all"},
  "columns": ["id", "email"],
  "delivery": "auto",
  "locale": "en-US",
  "timezone": "UTC",
  "idempotency_key": "req-123"
}
```

Response behavior:
- Sync responses stream the file with `Content-Disposition: attachment` and `X-Export-Id`.
- Async responses return `202` with `{id, status_url, download_url}`.
- `GET /admin/exports/{id}` returns an `ExportRecord` (includes `artifact` metadata when available).

### Row Sources
Row sources stream rows in the schema column order:
- `sources/crud`: go-crud datagrid queries with stable ordering + scope injection.
- `sources/repo`: repository-backed streaming iterators.
- `sources/sql`: named query registry with validated params + scope injection.
- `sources/callback`: function-based sources for computed exports.

### Row Transformations
Transformers apply after `RowSource.Open()` and before rendering, passing the updated schema forward.
- `ExportDefinition.Transformers` is an ordered list of transformer configs (keys + params).
- Register factories in `TransformerRegistry` to resolve named transformers.
- Streaming transforms are preferred; buffered transforms should be bounded with `ExportPolicy.MaxRows/MaxBytes`.
- For heavy aggregation, prefer SQL/materialized views and keep buffered transforms for small exports.

#### Transformer Config Serialization
`TransformerConfig` is JSON-serializable for storage alongside export definitions. Example:
```json
[
  {"key": "normalize", "params": {"mode": "lower", "trim": true}},
  {"key": "augment", "params": {"column": "domain"}},
  {"key": "filter", "params": {"field": "email", "op": "not_empty"}}
]
```

Storage guidance:
- Persist the transformer array in a JSON/JSONB column tied to your export definition records.
- Load configs into `ExportDefinition.Transformers` and resolve via `TransformerRegistry` at runtime.
- Keep transformer keys stable and versioned; unknown keys or invalid params fail validation.

### Renderers
Built-in renderers stream results without loading all rows:
- CSV (headers, delimiter)
- JSON array or NDJSON
- XLSX streaming writer with type-aware formatting
- Template renderer via `adapters/template` using go-template (Django/Pongo2 syntax); enable explicitly and choose buffered vs streaming strategies (buffered default).
- Server-side PDF renderer via `adapters/pdf` (Format `pdf`), gated by `Enabled`, with a wkhtmltopdf engine or a custom chromedp/rod engine.

Renderer behavior summary:
- CSV/JSON/NDJSON/XLSX: streaming
- Template: buffered by default; streaming supported when templates range over `.Rows` (channel-backed)
- PDF: HTML template render + server-side conversion (buffered HTML)

### Delivery Modes
`DeliveryAuto` selects sync or async based on configured thresholds.
- Sync: stream directly to an `io.Writer` (HTTP response).
- Async: write to an `ArtifactStore` and download later via guarded endpoint.
- `export.Runner` only supports sync delivery; async requires `export.Service` (and usually `adapters/job`) and returns `not_implemented` if forced.

### Async, Idempotency, and Cancellation
Use `adapters/job` for go-job execution:
- `IdempotencyKey` dedupes async requests by actor/scope/definition/format/query.
- Cancellation propagates through context to sources/renderers.
- Retry policy avoids unsafe partial writes.

### Artifact Stores
`export.MemoryStore` is dev/test-only and does not implement signed URLs; use `adapters/store/fs` or a production store for signed URL downloads.

### Retention and Cleanup
Retention is configured via `RetentionPolicy` and cleanup commands:
- TTL can be derived from definition/format/actor role.
- Cleanup removes expired artifacts and records.

### Limits and Quotas
Limits are enforced in core and hooks:
- `ExportPolicy` max rows/bytes/duration enforced during runs.
- `QuotaHook` allows rate limiting per actor/scope.
- `export.RateLimiter` provides an in-memory, per-actor window limiter.

### Observability
Hooks for audit and metrics:
- `ChangeEmitter` emits lifecycle events.
- `MetricsHook` emits counters for rows/bytes/duration/error kinds.
- `adapters/activity` logs events to go-users ActivitySink.

## Documentation
- Examples: `docs/EXAMPLES.md`
- Release notes + migration: `docs/RELEASE_NOTES.md`

## Quickstart
```go
runner := export.NewRunner()
_ = runner.Definitions.Register(export.ExportDefinition{
    Name:         "users",
    RowSourceKey: "callback",
    Schema: export.Schema{
        Columns: []export.Column{{Name: "id"}, {Name: "email"}},
    },
})
_ = runner.RowSources.Register("callback", func(req export.ExportRequest, def export.ResolvedDefinition) (export.RowSource, error) {
    return exportcallback.NewSource(func(ctx context.Context, spec export.RowSourceSpec) (export.RowIterator, error) {
        rows := []export.Row{{"1", "demo@example.com"}}
        idx := 0
        return &exportcallback.FuncIterator{
            NextFunc: func(ctx context.Context) (export.Row, error) {
                if idx >= len(rows) {
                    return nil, io.EOF
                }
                row := rows[idx]
                idx++
                return row, nil
            },
        }, nil
    }), nil
})

buf := &bytes.Buffer{}
_, _ = runner.Run(context.Background(), export.ExportRequest{
    Definition: "users",
    Format:     export.FormatCSV,
    Output:     buf,
})
```
