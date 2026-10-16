# go-export

go-export provides a streaming export engine with guard-first authorization, pluggable row sources, and sync/async delivery options for go-crud datagrids and custom datasets.

## Features

- Core runner + service layer with transport-agnostic interfaces.
- Row sources for go-crud, repositories, named SQL queries, and callbacks.
- Renderers for CSV, JSON/NDJSON, XLSX, plus optional SQLite, template, and PDF outputs.
- Sync downloads or async artifact generation with idempotency and retries.
- Progress tracking, retention cleanup hooks, and observability events/metrics.
- Optional go-notifications integration for export-ready email + inbox delivery with realtime updates.

## Package Layout

    export/        Core runner, validation, registries, service, memory adapters
    adapters/      exportapi (shared transport), router, http, job, tracker, store, template, activity, delivery adapters
    sources/       crud, repo, sql, callback row sources
    command/       go-command commands
    query/         go-command queries
    examples/      wiring helpers + app example

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

### Guarded scoped exports

Configure a trusted `ActorProvider` and `Guard`; unconfigured exports are not an authorization boundary. Service and runner export policies both apply before sync execution, queueing and generation. HTTP controller and runner export guards also run before delivery branching, custom queue adapters and idempotent reuse; record routes continue to use service policy. `GenerateExport` also requires `AuthorizeDownload` for its target ID before tracker/store access, including first-time generation: guarded hosts should authorize the queued record or explicitly permit a new ID. Guards can run more than once and must recheck current policy without depending on a single invocation.

Arbitrary Group labels remain separate from `Scope`'s tenant/workspace fields. Host adapters carry trusted Group context, enforce current native grants and predicate SQL before counts, selection and pagination. Export history needs the same SQL prefilter and a host authorization check before `List`; the service's per-record checks do not replace them. Row sources, trackers and stores are trusted infrastructure.

Choose an application-owned projection and de-identification policy. The scoped fixture demonstrates redaction and literal spreadsheet text through existing transformers; CSV quoting alone does not neutralize formulas. HTTP error responses use go-errors' public representation. Operational activity needs a trusted Group projector and remains best effort, without delivery or compliance guarantees.

See [the Group A/B SQL/HTTP fixture](tests/scopedexport/README.md) and the recorded workspace in `evidence/go-export`. The nested integration module is run separately from root tests; its exact dependency snapshots identify the verified composition.

### Datagrid Client Helper

Use the embedded datagrid client helper (source in `export/assets/datagrid_export_client.js`) in a go-admin DataGrid page to build an `exportcrud.DatagridRequest` payload.

``` js
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

History is served at `GET /admin/exports/history`. Querystring exports are supported via `GET /admin/exports?definition=users&format=csv`. To ship the helper with a Go app (go-formgen pattern), serve the embedded assets from `export.RuntimeAssetsFS()`:

``` go
mux.Handle("/assets/",
  http.StripPrefix("/assets/",
    http.FileServer(http.FS(export.RuntimeAssetsFS())),
  ),
)
```

Then load it in your template:

``` html
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

``` json
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

``` json
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
- SQLite renderer via `adapters/sqlite` (buffered, file-backed); enable explicitly and register the adapter.
- Template renderer via `adapters/template` using go-template (Django/Pongo2 syntax); enable explicitly and choose buffered vs streaming strategies (buffered default).
- Server-side PDF renderer via `adapters/pdf` (Format `pdf`), gated by `Enabled`, with a wkhtmltopdf engine or a custom chromedp/rod engine.

Renderer behavior summary:

- CSV/JSON/NDJSON/XLSX: streaming
- SQLite: buffered (temp file-backed database)
- Template: buffered by default; streaming supported when templates range over `.Rows` (channel-backed)
- PDF: HTML template render + server-side conversion (buffered HTML)

SQLite notes:

- The SQLite renderer is an optional adapter; register it on the runner and allowlist `FormatSQLite` in definitions.
- Uses the pure Go `modernc.org/sqlite` driver (no CGO).
- Table name defaults to `data` and can be overridden via `RenderOptions.SQLite.TableName`.

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

### Logging Contract

`go-export` uses a single logger contract aligned with `go-logger`:

- `export.Logger` is an alias of `github.com/goliatone/go-logger/glog.Logger`.
- `export.LoggerProvider` is an alias of `glog.LoggerProvider`.
- `export.FieldsLogger` is an alias of `glog.FieldsLogger`.
- `export.NopLogger()` returns the canonical no-op logger.

### Realtime Inbox (go-notifications)

The example app wires `go-notifications` to deliver export-ready events into an inbox channel and broadcast realtime updates.

Demo wiring highlights:

- Enable notifications: `EXPORT_NOTIFY_ENABLED=true`.
- For inbox-only, set `EXPORT_NOTIFY_CHANNELS=inbox` (default when enabled is `email,inbox`).
- WebSocket stream: `ws://localhost:8080/ws/inbox` (topics: `inbox.created`, `inbox.updated`).
- Inbox REST endpoints:
  - `GET /api/inbox` (list)
  - `GET /api/inbox/badge` (unread count)
  - `PATCH /api/inbox/read` (mark read/unread)
  - `DELETE /api/inbox/{id}` (dismiss)

## Documentation

- Examples: `docs/EXAMPLES.md`
- Release notes + migration: `docs/RELEASE_NOTES.md`

## Quickstart

``` go
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

### Durable local artifacts and replay

`adapters/store/fs.NewDurableStore(root)` is an opt-in persistent envelope store. It atomically publishes checksummed bytes and metadata together, syncs the file and containing directories, validates integrity before exposing a reader, and confines keys with `os.Root`. Provision a private persistent root supporting atomic rename and directory fsync. Serve artifacts through guarded `Open`/download handlers; envelope files cannot be served as raw static exports. The existing `NewStore` raw-file/sidecar format is unchanged. Use a separate root when adopting the durable format.

`adapters/replay/bun.NewService` implements `export.Service` and works with the existing command handlers or an HTTP controller's `Service`/`AsyncRequester`. Call its additive `Migrate(ctx, db)` after the native tracker schema migration. Supply `ServiceConfig`, a native Bun tracker and a trusted `Namespace(ctx, actor)` provider; Group scope must remain independent of tenant/workspace. The service commits replay identity and the complete sanitized queued request in one SQL transaction. It includes normalized query/selection, columns, format, locale/timezone and rendering options in its request signature; estimates, output handles, delivery and generated-at output metadata are excluded. The first queued timestamp and filename remain authoritative. Keys expire after the configured TTL (24 hours by default); exact expiry starts a new identity. Canceled/deleted records are never implicitly reactivated.

SQL transactions serialize independent processes. Generation holds a record lock and returns an acknowledged completed artifact without querying or rendering again. Artifact publication precedes committed completed tracking; interrupted transactions remain queued and retry authorized generation. Native progress tracking now marks publishing until the store and advertised artifact-metadata update succeed. `Tracker.WithTx` binds all native tracker operations to a caller-owned transaction. Queued requests are retained for every format with Output and IdempotencyKey removed.

For native Group authorization, supply `WithTransaction` using `groups.Service.WithAuthorized`, repeat current session/account checks in that transaction, and let transaction-aware guards/sources reuse the trusted fence. This transaction boundary applies to every asynchronous submission, including requests without an idempotency key. Do not open another write transaction inside it. Configure SQLite WAL/busy timeout and keep transactions bounded. The authenticated integration fixture demonstrates this composition on SQLite and PostgreSQL. Keep controller/runner guards enabled, configure the durable service as the async requester, and omit a second in-memory HTTP idempotency store.

Activity is buffered until commit and emitted afterward; it has no outbox guarantee. A provisional artifact is unavailable through the service until completed SQL state commits. Restart recovery does not promise a frozen data snapshot for an unacknowledged generation. Old records without retained requests fail closed rather than inventing a payload. Back up SQL and the artifact root together while quiescent. SIGKILL and restore checks do not establish physical power-loss durability, live cross-storage snapshots, external scheduler delivery or production readiness. Interrupted writes can leave private temporary files; reclaim them only while writers are stopped. No root dependency pins or existing public method signatures change.

### Transactional scheduler and outbox

`adapters/job/bun.NewScheduler` composes the durable replay service with a transaction-aware SQL queue. Supply `Replay` (the same durable store, tracker, trusted namespace and optional authorized transaction fence used above) and `Queue`. The queue's `Database()` must be the tracker's exact `*sql.DB`; current go-job `queue/adapters/postgres.Storage` implements `Database` and `EnqueueTx` for PostgreSQL or SQLite (`WithDialect(DialectSQLite)`). Migrate tracker/replay and queue schemas before accepting requests.

Use this scheduler directly as `export.Service` in HTTP and the existing typed request/generation commands. `RequestExport` forces async semantics. A new record, optional replay key, executable message and queue acceptance status commit together; keyed replays do not enqueue again, including after acknowledgment. `AvailableAt` selects a new message's persisted schedule time; replay keeps its first time. It schedules individual jobs; recurring cron occurrence persistence is separate. This opt-in composition uses interfaces so declared dependency graphs still compile; actual durable operation requires the transaction-aware queue implementation. Legacy `exportjob.Scheduler` remains enqueue-after-request and cannot provide this atomicity.

Durable generation coalesces per-row progress in constant memory inside its SQL transaction. It flushes progress and publishing only after the runner closes the source cursor and the artifact store acknowledges publication. This prevents PostgreSQL driver result truncation when source rows and tracking share one transaction connection. Running is recorded before opening the cursor; failure/cancellation aborts the transaction without cursor-active tracking SQL. Intermediate progress was already uncommitted and private until transaction commit.

Workers consume the native queue through `postgres.NewAdapter` / `worker.NewWorker` or `queue.NewStorageOutboxAdapter`. Register the existing `exportjob.NewGenerateTask` and dispatch the existing generation command to this service. Reconstruct trusted current actor/scope and authorization at execution; serialized actor fields are not authentication. Transaction-aware guards and sources must reuse the supplied transaction or its trusted fence, including scoped ownership reads; do not borrow extra pooled connections or start nested writes while holding the transaction. In durable composition leave `TaskConfig.Store` nil (legacy retry cleanup must not delete an acknowledged artifact), and choose one retry owner. Queue retries are at least once; the durable service serializes generation and reuses completed artifacts after an uncertain ACK.

Renew leases before expiry for long tasks. Expired or superseded receipts cannot ack, nack or renew. A lost ACK response is resolved through durable dispatch status or reclaim/redelivery; repeated ACK is not itself idempotent. Retain replay identities for the caller's retry horizon; explicit TTL expiry allows a new operation. Keep queue tables and export/replay records in the same backup/restore boundary with private artifact storage. Existing records accepted before scheduler adoption are not backfilled automatically; migrate or drain them before enabling new scheduling.

This contract covers delivery to native export generation. Email/webhook/notification providers, activity emission and other external effects still require a durable per-effect outbox and receiver idempotency to handle ambiguous acknowledgments. The generic delivery adapter remains outside this guarantee.

### Durable webhook target receipts

`adapters/delivery/bun.NewService` adds opt-in webhook notification scheduling for an already completed export. Run `deliverybun.Migrate` on the same database as the native transactional queue. Supply current `Namespace`, `WithAuthorizedTransaction`, `Authorize`, `AuthorizeTarget` and stable guarded `Link` policies. The transaction callback must invoke its function once on that database, after checking current identity/session/account, export ownership, completed artifact metadata and grants. Reuse its trusted transaction for nested reads. Destination and credential policy is mandatory at acceptance and dispatch; never pass arbitrary browser-supplied URLs/headers through an allow-all policy.

Use the shared `command.ScheduleWebhooks` / `ScheduleWebhooksHandler`, or the service's `Schedule(ctx, actor, request)`, with an export ID, stable operation key and stable target IDs. Target order is irrelevant; changing the target set, endpoint, headers or metadata under the same scoped key conflicts. Namespace, actor ID and tenant/workspace scope isolate identities. Intent, exact JSON body/digest and one executable queue message per target commit together. The first timestamp/link/body are retained across retries. Actor roles/details are excluded from the wire payload; do not place sensitive data in notification metadata. Bodies are limited to 1 MiB and target sets to 20.

Register `exportjob.NewWebhookTask` with the native worker, supplying a mandatory current trusted identity/context resolver and dispatch to `command.DeliverWebhookHandler`. Queue actor fields are hints, never authentication. The service authorizes receipt status and sending before reading persisted intent, skips completed targets, sends outside SQL transactions and commits the per-target receipt before queue acknowledgment. Concurrent sends and uncertain acknowledgments may repeat HTTP requests with the same key and bytes. Choose queue retry/dead-letter policy for transient failures and terminal authorization/configuration denials; no in-memory dedup cache or automatic legacy delivery adoption is used.

Use `delivery.HTTPReceiptWebhookSender` for the receipt protocol: POST JSON with `Idempotency-Key` and `X-Export-Payload-SHA256`; accept only a 2xx response echoing both exact values. It disables redirects, defaults an unbounded client to a 30-second timeout and returns safe provider errors. The HTTP transport may transparently retry an idempotent request; the receiver must deduplicate it too. HTTPS and endpoint/network allowlists are host policy; provision trusted credentials and private SQL storage because persisted intent contains authorized headers. Credential/endpoint changes require an explicit new operation key or a separately designed migration.

At the receiver, migrate `deliverybun.MigrateReceiver` and configure `ReceiverHandler` with an authenticated sender `Namespace` and transactional `Apply`. `Receiver.Receive` inserts the scoped inbox key and commits `Apply` on the same Bun transaction. Exact replay skips the effect; a changed body/digest conflicts. Echo the receipt only after successful commit. `Apply` must validate the event and use only the supplied SQL transaction: external I/O or a second database cannot inherit this guarantee and needs its own outbox/idempotency contract. Sender/receiver receipt tables have no automatic expiry; retain them through every replay/retry/backup horizon before coordinated cleanup.

This path schedules notifications for completed exports explicitly. Automatic atomic export-completion-to-notification publication, recurring cron occurrences, email/SMTP, arbitrary webhook endpoints without this receiver contract and nontransactional external effects remain separate. Existing legacy delivery APIs keep their original behavior. SIGKILL/restart checks establish the tested process-failure boundaries; production identity policy, physical power-loss/failover and capacity acceptance remain deployment work.

### Atomic export completion and webhook publication

`deliverybun.NewCompletionPublisher` closes the separate generation-to-scheduling gap for an opt-in durable replay/scheduler service. Run `deliverybun.MigrateCompletion(ctx, db)` as well as the existing delivery, tracker/replay and queue migrations. Supply a validated delivery service, a `Select(ctx, actor, queuedRecord)` function returning the desired webhook request and mandatory transaction-aware `AuthorizeTransaction`. Use the exact same Bun database and native transactional queue throughout.

Bind the publisher before constructing the scheduler:

```go
publisher, err := deliverybun.NewCompletionPublisher(deliverybun.CompletionConfig{
    Delivery: deliveryService,
    Select: selectWebhookPlan,
    AuthorizeTransaction: authorizeExportInTransaction,
})
if err != nil { return err }
replayConfig, err = publisher.Bind(replayConfig)
if err != nil { return err }
scheduler, err := jobbun.NewScheduler(jobbun.Config{Replay: replayConfig, Queue: queue})
```

The native replay service's optional `OnQueued` hook runs after a new async record is inserted and before its generation message is enqueued. The publisher stores the first canonical scoped plan alongside that record/message/replay identity. Every new record on this composition needs a nonempty stable key and target set from Select; keyed request replay does not reselect the plan, and keyless requests each capture their own plan. Supply per-request choices through trusted application policy/context at acceptance. Pending plans include authorized endpoints/headers and must remain private.

`OnCompleted` runs after durable artifact publication and completed tracking, before SQL commit. The publisher reads the persisted plan, rechecks current transaction/export/Group and destination policy, then commits webhook operation/target intent, delivery messages and its publication marker in that same transaction. It does not send HTTP or open another transaction. Any failure rolls completed tracking and publication back; provisional artifact files remain unavailable through the queued record's download state. An authorized generation retry may regenerate those unacknowledged bytes. An uncertain successful commit is recovered by generation replay: the completed record/artifact is reused, hooks are skipped and acknowledged webhook jobs are not recreated.

Keep using the existing request/generate commands, native generation task and webhook task described above. Reconstruct current trusted initiating identity and Group context in workers; persisted actor/plan claims do not authenticate execution. Transaction authorization must use the supplied transaction or verified host fence, including ownership reads, without opening nested writes or borrowing a second connection. Select, authorization and publication callbacks must not commit or perform network I/O. Bind rejects an existing hook owner or another Bun database; postcommit activity remains outside the outbox guarantee.

The new plan/publication table joins the existing export/replay/queue/target tables in one backup and retention boundary. Existing completed records are reused without automatic hook/backfill execution. Existing queued records missing a retained plan fail publication closed under this composition; explicitly drain them with the prior service or adopt a reviewed plan by calling the publisher's OnQueued hook inside an authorized transaction before generation. Do not silently invent plans from current configuration or rerun published notifications. Endpoint/credential migrations, orphan file reclamation, receiver receipt retention, recurring cron occurrences, nontransactional effects and deployment policy remain explicit operational work. The receiver-side idempotency contract is unchanged.
