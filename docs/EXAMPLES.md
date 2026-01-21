# Examples

## Sync CSV export (callback row source)
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
    rows := []export.Row{{"1", "demo@example.com"}}
    idx := 0
    return exportcallback.NewSource(func(ctx context.Context, spec export.RowSourceSpec) (export.RowIterator, error) {
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

## Sync SQLite export (adapter)
```go
runner := export.NewRunner()
_ = runner.Renderers.Register(export.FormatSQLite, exportsqlite.Renderer{Enabled: true})
_ = runner.Definitions.Register(export.ExportDefinition{
    Name:           "users",
    RowSourceKey:   "callback",
    AllowedFormats: []export.Format{export.FormatSQLite},
    Schema: export.Schema{
        Columns: []export.Column{{Name: "id", Type: "int"}, {Name: "email"}},
    },
})
_ = runner.RowSources.Register("callback", func(req export.ExportRequest, def export.ResolvedDefinition) (export.RowSource, error) {
    rows := []export.Row{{int64(1), "demo@example.com"}}
    idx := 0
    return exportcallback.NewSource(func(ctx context.Context, spec export.RowSourceSpec) (export.RowIterator, error) {
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
    Format:     export.FormatSQLite,
    Output:     buf,
    RenderOptions: export.RenderOptions{
        SQLite: export.SQLiteOptions{TableName: "report_rows"},
    },
})
```

## Async XLSX export to filesystem artifact store
```go
runner := export.NewRunner()
_ = runner.Definitions.Register(export.ExportDefinition{
    Name:         "reports",
    RowSourceKey: "callback",
    Schema: export.Schema{
        Columns: []export.Column{{Name: "id"}, {Name: "total", Type: "number"}},
    },
})
_ = runner.RowSources.Register("callback", func(req export.ExportRequest, def export.ResolvedDefinition) (export.RowSource, error) {
    return exportcallback.NewSource(func(ctx context.Context, spec export.RowSourceSpec) (export.RowIterator, error) {
        rows := []export.Row{{"1", 42.5}}
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

store := storefs.NewStore("/tmp/exports")
tracker := export.NewMemoryTracker()
svc := export.NewService(export.ServiceConfig{
    Runner:  runner,
    Store:   store,
    Tracker: tracker,
})

record, _ := svc.RequestExport(context.Background(), export.Actor{ID: "actor-1"}, export.ExportRequest{
    Definition: "reports",
    Format:     export.FormatXLSX,
    Delivery:   export.DeliveryAsync,
})

// In production, enqueue this via adapters/job; Runner only supports sync delivery.
_, _ = svc.GenerateExport(context.Background(), export.Actor{ID: "actor-1"}, record.ID, export.ExportRequest{
    Definition: "reports",
    Format:     export.FormatXLSX,
})
```

## Row transformation pipeline (map + augment + filter)
```go
runner := export.NewRunner()
_ = runner.Transformers.Register("normalize", func(cfg export.TransformerConfig) (export.RowTransformer, error) {
    return export.NewMapTransformer(func(ctx context.Context, row export.Row) (export.Row, error) {
        _ = ctx
        next := append(export.Row(nil), row...)
        next[1] = strings.TrimSpace(strings.ToLower(next[1].(string)))
        return next, nil
    }), nil
})
_ = runner.Transformers.Register("augment", func(cfg export.TransformerConfig) (export.RowTransformer, error) {
    return export.NewAugmentTransformer([]export.Column{{Name: "domain"}}, func(ctx context.Context, row export.Row) ([]any, error) {
        _ = ctx
        parts := strings.SplitN(row[1].(string), "@", 2)
        domain := ""
        if len(parts) == 2 {
            domain = parts[1]
        }
        return []any{domain}, nil
    }), nil
})
_ = runner.Transformers.Register("filter", func(cfg export.TransformerConfig) (export.RowTransformer, error) {
    return export.NewFilterTransformer(func(ctx context.Context, row export.Row) (bool, error) {
        _ = ctx
        return row[1] != "", nil
    }), nil
})

_ = runner.Definitions.Register(export.ExportDefinition{
    Name:         "users",
    RowSourceKey: "callback",
    Schema: export.Schema{
        Columns: []export.Column{{Name: "id"}, {Name: "email"}},
    },
    Transformers: []export.TransformerConfig{
        {Key: "normalize"},
        {Key: "augment"},
        {Key: "filter"},
    },
})

_ = runner.RowSources.Register("callback", func(req export.ExportRequest, def export.ResolvedDefinition) (export.RowSource, error) {
    rows := []export.Row{{"1", " Alice@Example.com "}, {"2", " "}}
    idx := 0
    return exportcallback.NewSource(func(ctx context.Context, spec export.RowSourceSpec) (export.RowIterator, error) {
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
// CSV headers now include the new "domain" column added by the transformer.
```

Guidance: keep large aggregations in SQL/materialized views; buffered transformers are best for small exports and should be paired with `ExportPolicy.MaxRows/MaxBytes` limits to avoid unbounded memory use.

## go-command wiring
```go
reg := gcmd.NewRegistry()
subs, _ := examples.RegisterExportHandlers(reg, svc)
defer func() {
    for _, sub := range subs {
        sub.Unsubscribe()
    }
}()

record, _ := dispatcher.DispatchWithResult[exportcmd.RequestExport, export.ExportRecord](
    context.Background(),
    exportcmd.RequestExport{
        Actor: actor,
        Request: export.ExportRequest{
            Definition: "users",
            Format:     export.FormatCSV,
        },
    },
)
```

## go-job async wiring
```go
// API side: enqueue exports via a queue-backed scheduler.
storage := qredis.NewStorage(redisClient, qredis.WithQueueName("exports"))
adapter := qredis.NewAdapter(storage)

scheduler := exportjob.NewScheduler(exportjob.Config{
    Service:  svc,
    Enqueuer: adapter,
    Tracker:  tracker,
})

controller := exportapi.NewController(exportapi.Config{
    Service:        svc,
    Runner:         runner,
    Store:          store,
    ActorProvider:  actorProvider,
    AsyncRequester: scheduler,
})

record, _ := scheduler.RequestExport(context.Background(), actor, export.ExportRequest{
    Definition: "reports",
    Format:     export.FormatXLSX,
})

// Worker side: register tasks and consume deliveries.
cancelRegistry := exportjob.NewCancelRegistry()
generateTask := exportjob.NewGenerateTask(exportjob.TaskConfig{
    CancelRegistry: cancelRegistry,
    Store:          store,
})
deliveryTask := exportdelivery.NewTask(exportdelivery.TaskConfig{
    Handler: deliverySvc, // exportdelivery.NewService(...)
})

// CLI/cron scheduled exports: run synchronously via the shared MessageBuilder.
builder := exportjob.NewMessageBuilder(exportjob.MessageBuilderConfig{
    Service: svc,
    Tracker: tracker,
})
executor := exportjob.NewBatchExecutor(generateTask, builder)
scheduledExports := command.NewScheduledExportsCommand(
    scheduler, // async requester (optional when executor is set)
    loader,
    command.WithBatchExecutor(executor),
)

worker := queueworker.NewWorker(adapter, queueworker.WithConcurrency(4))
// TaskCommander retries are disabled by default; GenerateTask owns retries.
_ = worker.Register(generateTask)
_ = worker.Register(deliveryTask)
_ = worker.Start(context.Background())
```

```go
// Scheduled deliveries: choose mode explicitly.
deliveryBuilder := exportdelivery.NewMessageBuilder(exportdelivery.MessageBuilderConfig{})
deliveryExecutor := exportdelivery.NewTaskExecutor(deliveryTask, deliveryBuilder)

scheduleCmd := exportdelivery.NewScheduledDeliveriesCommand(
    scheduler, // enqueue requester
    loader,
    exportdelivery.WithScheduleMode(exportdelivery.ScheduleModeEnqueue), // or ExecuteSync
    exportdelivery.WithScheduleExecutor(deliveryExecutor),              // required for ExecuteSync
)

// Override order: env -> flag -> config (config wins).
// EXPORT_DELIVERY_SCHEDULE_MODE=enqueue|execute_sync
```

## Realtime inbox (examples/web)
The demo app wires `go-notifications` inbox events to a WebSocket hub and REST endpoints.

Run the example with notifications enabled:
```bash
EXPORT_NOTIFY_ENABLED=true \
EXPORT_NOTIFY_CHANNELS=inbox \
go run ./examples/web
```

Connect to the stream (topics: `inbox.created`, `inbox.updated`):
```js
const ws = new WebSocket('ws://localhost:8080/ws/inbox?user_id=demo-user');
ws.onmessage = (event) => console.log(JSON.parse(event.data));
```

Inbox endpoints:
```
GET /api/inbox
GET /api/inbox/badge
PATCH /api/inbox/read
DELETE /api/inbox/{id}
```

Notes:
- The demo uses a static actor (`demo-user`); pass `user_id` as a query param (WS or REST) or `X-User-ID` header (REST) to scope inbox calls.
- If you want email delivery too, include `email` in `EXPORT_NOTIFY_CHANNELS` and configure SMTP settings.

## Named query export (sources/sql)
```go
type reportParams struct {
    From string
    To   string
}

registry := exportsql.NewRegistry()
_ = registry.Register(exportsql.Definition{
    Name:  "billing_rollup",
    Query: "SELECT id, total FROM billing WHERE created_at >= ? AND created_at <= ?",
    Validate: func(params any) error {
        p, ok := params.(reportParams)
        if !ok || p.From == "" || p.To == "" {
            return export.NewError(export.KindValidation, "missing params", nil)
        }
        return nil
    },
})

type queryExecutor struct{}
func (queryExecutor) Query(ctx context.Context, spec exportsql.QuerySpec) (export.RowIterator, error) {
    _ = ctx
    _ = spec
    rows := []export.Row{{"1", 99.9}}
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
}

runner := export.NewRunner()
_ = runner.Definitions.Register(export.ExportDefinition{
    Name:         "billing",
    RowSourceKey: "sql",
    Schema: export.Schema{
        Columns: []export.Column{{Name: "id"}, {Name: "total", Type: "number"}},
    },
})
_ = runner.RowSources.Register("sql", func(req export.ExportRequest, def export.ResolvedDefinition) (export.RowSource, error) {
    return exportsql.NewSource(registry, queryExecutor{}, "billing_rollup"), nil
})

buf := &bytes.Buffer{}
_, _ = runner.Run(context.Background(), export.ExportRequest{
    Definition: "billing",
    Format:     export.FormatCSV,
    Query:      reportParams{From: "2024-01-01", To: "2024-01-31"},
    Output:     buf,
})
```

## go-admin DataGrid export wiring (asset helper + behavior override)
Serve the embedded datagrid helper and use it from a go-admin DataGrid page.

Go server:
```go
handler := exporthttp.NewHandler(exporthttp.Config{Runner: runner})
mux.Handle("/admin/exports", handler)

mux.Handle("/assets/",
  http.StripPrefix("/assets/",
    exporthttp.RuntimeAssetsHandler(),
  ),
)
```

Browser (DataGrid export behavior):
```js
import { buildDatagridExportRequest } from '/assets/datagrid_export_client.js';

class GoExportBehavior {
  constructor(definition) {
    this.definition = definition;
  }

  getEndpoint() {
    return '/admin/exports';
  }

  async export(format, grid) {
    const payload = buildDatagridExportRequest(grid, {
      definition: this.definition,
      format
    });
    const res = await fetch(this.getEndpoint(), {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    if (res.status === 202) {
      return await res.json();
    }
    if (!res.ok) {
      throw new Error(`export failed: ${res.status}`);
    }
    return { id: res.headers.get('X-Export-Id') };
  }
}
```

Notes:
- Export history lives at `GET /admin/exports/history`.
- Querystring exports are supported via `GET /admin/exports?definition=users&format=csv`.
- Status is served at `GET /admin/exports/{id}`; downloads at `GET /admin/exports/{id}/download`.
