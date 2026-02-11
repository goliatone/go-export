# Row Sources Guide

This guide covers implementing and using row sources to stream export data in go-export. Row sources are the primary mechanism for providing data to the export engine.

## Overview

A row source is responsible for:

- **Opening** a data stream based on the export request
- **Iterating** through rows one at a time
- **Closing** resources when complete

The streaming iterator pattern allows go-export to handle datasets of any size without loading everything into memory.

## Core Interfaces

### RowSource Interface

```go
type RowSource interface {
    Open(ctx context.Context, spec RowSourceSpec) (RowIterator, error)
}
```

The `Open` method receives a `RowSourceSpec` containing everything needed to fetch the data:

```go
type RowSourceSpec struct {
    Definition ResolvedDefinition  // The export definition with any variant overrides
    Request    ExportRequest       // The original export request
    Columns    []Column            // Resolved columns to export
    Actor      Actor               // The requesting user/principal
}
```

### RowIterator Interface

```go
type RowIterator interface {
    Next(ctx context.Context) (Row, error)
    Close() error
}
```

The iterator pattern:
- `Next()` returns the next row or `io.EOF` when done
- `Close()` releases any held resources
- Context cancellation should be respected for graceful shutdown

### Row Type

A row is simply a slice of values aligned with the column order:

```go
type Row []any
```

Values should match the order of columns in the schema.

## Built-in Row Sources

go-export provides four row source implementations for common use cases:

| Package | Use Case | Key Feature |
|---------|----------|-------------|
| `sources/callback` | Simple data providers | Function-based, no dependencies |
| `sources/crud` | go-crud datagrids | Filter/sort/pagination support |
| `sources/repo` | Repository pattern | Full request context |
| `sources/sql` | Named SQL queries | Parameter validation, scope injection |

## Callback Source

The callback source is the simplest way to provide data. It wraps a function that creates an iterator.

### Basic Usage

```go
import (
    "context"
    "io"

    "github.com/goliatone/go-export/export"
    exportcallback "github.com/goliatone/go-export/sources/callback"
)

// Create a callback-based row source
source := exportcallback.NewSource(func(ctx context.Context, spec export.RowSourceSpec) (export.RowIterator, error) {
    // Your data - could come from a database, API, file, etc.
    users := [][]any{
        {"1", "alice@example.com", "Alice Johnson", "admin"},
        {"2", "bob@example.com", "Bob Smith", "user"},
        {"3", "carol@example.com", "Carol Williams", "editor"},
    }

    idx := 0
    return &exportcallback.FuncIterator{
        NextFunc: func(ctx context.Context) (export.Row, error) {
            if idx >= len(users) {
                return nil, io.EOF  // Signal end of data
            }
            row := users[idx]
            idx++
            return row, nil
        },
    }, nil
})
```

### FuncIterator

The `FuncIterator` adapts functions to the `RowIterator` interface:

```go
type FuncIterator struct {
    NextFunc  func(ctx context.Context) (export.Row, error)
    CloseFunc func() error  // Optional cleanup
}
```

### With Cleanup

```go
source := exportcallback.NewSource(func(ctx context.Context, spec export.RowSourceSpec) (export.RowIterator, error) {
    // Open a database connection
    db, err := sql.Open("postgres", connectionString)
    if err != nil {
        return nil, err
    }

    rows, err := db.QueryContext(ctx, "SELECT id, email, name FROM users")
    if err != nil {
        db.Close()
        return nil, err
    }

    return &exportcallback.FuncIterator{
        NextFunc: func(ctx context.Context) (export.Row, error) {
            if !rows.Next() {
                if err := rows.Err(); err != nil {
                    return nil, err
                }
                return nil, io.EOF
            }
            var id, email, name string
            if err := rows.Scan(&id, &email, &name); err != nil {
                return nil, err
            }
            return export.Row{id, email, name}, nil
        },
        CloseFunc: func() error {
            rows.Close()
            return db.Close()
        },
    }, nil
})
```

### Accessing Spec Data

Use the `RowSourceSpec` to customize behavior:

```go
source := exportcallback.NewSource(func(ctx context.Context, spec export.RowSourceSpec) (export.RowIterator, error) {
    // Access the actor for tenant filtering
    tenantID := spec.Actor.Scope.TenantID

    // Access requested columns
    columns := make([]string, len(spec.Columns))
    for i, col := range spec.Columns {
        columns[i] = col.Name
    }

    // Access selection criteria
    if spec.Request.Selection.Mode == export.SelectionIDs {
        ids := spec.Request.Selection.IDs
        // Filter by specific IDs
    }

    // Build your iterator based on these inputs
    return buildIterator(tenantID, columns)
})
```

## CRUD Source

The CRUD source integrates with go-crud datagrids, supporting filters, sorting, and pagination.

### Streamer Interface

Implement the `Streamer` interface to provide data:

```go
type Streamer interface {
    Stream(ctx context.Context, spec Spec) (export.RowIterator, error)
}

type Spec struct {
    Query     Query             // Filters, search, sort, pagination
    Selection export.Selection  // Row selection criteria
    Columns   []string          // Column names to include
    Actor     export.Actor      // Requesting principal
    Scope     export.Scope      // Tenant/workspace scope
}

type Query struct {
    Filters []Filter  // Field filters
    Search  string    // Full-text search term
    Sort    []Sort    // Sort directives
    Cursor  string    // Pagination cursor
    Limit   int       // Page size
    Offset  int       // Skip count
}
```

### Implementing a Streamer

```go
import (
    "context"
    "io"

    "github.com/goliatone/go-export/export"
    exportcrud "github.com/goliatone/go-export/sources/crud"
)

type UserStreamer struct {
    db *sql.DB
}

func (s UserStreamer) Stream(ctx context.Context, spec exportcrud.Spec) (export.RowIterator, error) {
    // Build query from spec
    query := "SELECT "
    query += strings.Join(spec.Columns, ", ")
    query += " FROM users WHERE 1=1"

    var args []any

    // Apply tenant scope
    if spec.Scope.TenantID != "" {
        query += " AND tenant_id = ?"
        args = append(args, spec.Scope.TenantID)
    }

    // Apply filters
    for _, filter := range spec.Query.Filters {
        switch filter.Op {
        case "eq":
            query += fmt.Sprintf(" AND %s = ?", filter.Field)
            args = append(args, filter.Value)
        case "contains":
            query += fmt.Sprintf(" AND %s LIKE ?", filter.Field)
            args = append(args, "%"+filter.Value.(string)+"%")
        }
    }

    // Apply selection
    if spec.Selection.Mode == exportcrud.SelectionIDs && len(spec.Selection.IDs) > 0 {
        placeholders := strings.Repeat("?,", len(spec.Selection.IDs))
        placeholders = placeholders[:len(placeholders)-1]
        query += fmt.Sprintf(" AND id IN (%s)", placeholders)
        for _, id := range spec.Selection.IDs {
            args = append(args, id)
        }
    }

    // Apply sorting
    if len(spec.Query.Sort) > 0 {
        query += " ORDER BY "
        for i, sort := range spec.Query.Sort {
            if i > 0 {
                query += ", "
            }
            query += sort.Field
            if sort.Desc {
                query += " DESC"
            }
        }
    }

    rows, err := s.db.QueryContext(ctx, query, args...)
    if err != nil {
        return nil, err
    }

    return &sqlIterator{rows: rows, columns: spec.Columns}, nil
}
```

### Using the CRUD Source

```go
import exportcrud "github.com/goliatone/go-export/sources/crud"

// Create the source
streamer := UserStreamer{db: db}
source := exportcrud.NewSource(streamer, exportcrud.Config{
    PrimaryKey: "id",  // Default sort column
})

// Register with the row source registry
registry.Register("users", func(req export.ExportRequest, def export.ResolvedDefinition) (export.RowSource, error) {
    return source, nil
})
```

### Datagrid Request Contract

The CRUD source expects query data in the `DatagridRequest` format:

```go
type DatagridRequest struct {
    Definition     string              `json:"definition"`
    Resource       string              `json:"resource,omitempty"`
    Format         export.Format       `json:"format,omitempty"`
    Query          *Query              `json:"query,omitempty"`
    Selection      SelectionPayload    `json:"selection,omitempty"`
    Columns        []string            `json:"columns,omitempty"`
    Delivery       export.DeliveryMode `json:"delivery,omitempty"`
    EstimatedRows  int                 `json:"estimated_rows,omitempty"`
    EstimatedBytes int64               `json:"estimated_bytes,omitempty"`
}
```

## Repository Source

The repository source passes the full export context to a repository implementation.

### Repository Interface

```go
type Repository interface {
    Stream(ctx context.Context, spec Spec) (export.RowIterator, error)
}

type Spec struct {
    Request   export.ExportRequest  // Full export request
    Columns   []export.Column       // Resolved columns
    Actor     export.Actor          // Requesting principal
    Scope     export.Scope          // Tenant/workspace scope
    Selection export.Selection      // Row selection
    Query     any                   // Custom query parameters
}
```

### Implementing a Repository

```go
import (
    "context"

    "github.com/goliatone/go-export/export"
    exportrepo "github.com/goliatone/go-export/sources/repo"
)

type OrderRepository struct {
    db *sql.DB
}

func (r *OrderRepository) Stream(ctx context.Context, spec exportrepo.Spec) (export.RowIterator, error) {
    // Access full request context
    format := spec.Request.Format
    locale := spec.Request.Locale
    timezone := spec.Request.Timezone

    // Build query based on selection
    var query string
    var args []any

    switch spec.Selection.Mode {
    case export.SelectionAll:
        query = "SELECT * FROM orders WHERE tenant_id = ?"
        args = []any{spec.Scope.TenantID}

    case export.SelectionIDs:
        query = "SELECT * FROM orders WHERE id = ANY(?)"
        args = []any{spec.Selection.IDs}

    case export.SelectionQuery:
        // Handle named query selection
        query, args = r.buildQueryFromRef(spec.Selection.Query)
    }

    rows, err := r.db.QueryContext(ctx, query, args...)
    if err != nil {
        return nil, err
    }

    return newOrderIterator(rows, spec.Columns), nil
}
```

### Using the Repository Source

```go
import exportrepo "github.com/goliatone/go-export/sources/repo"

repo := &OrderRepository{db: db}
source := exportrepo.NewSource(repo)

registry.Register("orders", func(req export.ExportRequest, def export.ResolvedDefinition) (export.RowSource, error) {
    return source, nil
})
```

## SQL Source

The SQL source executes named, registered queries with parameter validation and scope injection.

### Query Registry

Register named queries with optional validation:

```go
import exportsql "github.com/goliatone/go-export/sources/sql"

// Create a query registry
queryRegistry := exportsql.NewRegistry()

// Register a simple query
queryRegistry.Register(exportsql.Definition{
    Name:  "active-users",
    Query: "SELECT id, email, name, created_at FROM users WHERE status = 'active'",
})

// Register a query with parameters
queryRegistry.Register(exportsql.Definition{
    Name:  "users-by-role",
    Query: "SELECT id, email, name FROM users WHERE role = :role",
    Validate: func(params any) error {
        p, ok := params.(map[string]any)
        if !ok {
            return errors.New("params must be a map")
        }
        if _, ok := p["role"]; !ok {
            return errors.New("role parameter is required")
        }
        return nil
    },
})

// Register a query with scope injection
queryRegistry.Register(exportsql.Definition{
    Name:  "tenant-orders",
    Query: "SELECT id, customer, total FROM orders WHERE tenant_id = :tenant_id",
    ScopeInjector: func(scope export.Scope, params any) (any, error) {
        p, _ := params.(map[string]any)
        if p == nil {
            p = make(map[string]any)
        }
        p["tenant_id"] = scope.TenantID
        return p, nil
    },
})
```

### Executor Interface

Implement the `Executor` to run queries:

```go
type Executor interface {
    Query(ctx context.Context, spec QuerySpec) (export.RowIterator, error)
}

type QuerySpec struct {
    Name    string          // Query name
    Query   string          // SQL query string
    Params  any             // Validated parameters
    Actor   export.Actor    // Requesting principal
    Scope   export.Scope    // Tenant/workspace scope
    Columns []export.Column // Requested columns
}
```

### Implementing an Executor

```go
type SQLExecutor struct {
    db *sql.DB
}

func (e *SQLExecutor) Query(ctx context.Context, spec exportsql.QuerySpec) (export.RowIterator, error) {
    // Convert named params to positional
    query, args := e.prepareQuery(spec.Query, spec.Params)

    rows, err := e.db.QueryContext(ctx, query, args...)
    if err != nil {
        return nil, err
    }

    return &sqlRowIterator{rows: rows}, nil
}
```

### Using the SQL Source

```go
import exportsql "github.com/goliatone/go-export/sources/sql"

executor := &SQLExecutor{db: db}
source := exportsql.NewSource(queryRegistry, executor, "active-users")

registry.Register("active-users", func(req export.ExportRequest, def export.ResolvedDefinition) (export.RowSource, error) {
    return source, nil
})
```

## Row Source Registration

Row sources are registered with a `RowSourceRegistry` using factory functions:

```go
// RowSourceFactory creates a RowSource for a request.
type RowSourceFactory func(req ExportRequest, def ResolvedDefinition) (RowSource, error)
```

### Basic Registration

```go
registry := export.NewRowSourceRegistry()

// Register a static source
registry.Register("users", func(req export.ExportRequest, def export.ResolvedDefinition) (export.RowSource, error) {
    return userSource, nil
})

// Register with request-based logic
registry.Register("orders", func(req export.ExportRequest, def export.ResolvedDefinition) (export.RowSource, error) {
    // Select source based on variant
    if def.Variant == "summary" {
        return orderSummarySource, nil
    }
    return orderDetailSource, nil
})
```

### Dynamic Source Creation

```go
registry.Register("reports", func(req export.ExportRequest, def export.ResolvedDefinition) (export.RowSource, error) {
    // Create source based on request parameters
    if params, ok := req.Query.(map[string]any); ok {
        if reportType, ok := params["type"].(string); ok {
            switch reportType {
            case "sales":
                return salesReportSource, nil
            case "inventory":
                return inventoryReportSource, nil
            }
        }
    }
    return defaultReportSource, nil
})
```

## Implementing Custom Row Sources

### Basic Custom Source

```go
type CustomSource struct {
    dataProvider DataProvider
}

func (s *CustomSource) Open(ctx context.Context, spec export.RowSourceSpec) (export.RowIterator, error) {
    // Validate inputs
    if s.dataProvider == nil {
        return nil, export.NewError(export.KindValidation, "data provider is required", nil)
    }

    // Fetch data based on spec
    data, err := s.dataProvider.Fetch(ctx, spec.Actor.Scope.TenantID)
    if err != nil {
        return nil, export.NewError(export.KindInternal, "failed to fetch data", err)
    }

    // Return an iterator
    return &customIterator{data: data}, nil
}

type customIterator struct {
    data  []Record
    index int
}

func (it *customIterator) Next(ctx context.Context) (export.Row, error) {
    // Check for cancellation
    select {
    case <-ctx.Done():
        return nil, ctx.Err()
    default:
    }

    if it.index >= len(it.data) {
        return nil, io.EOF
    }

    record := it.data[it.index]
    it.index++

    return export.Row{
        record.ID,
        record.Name,
        record.Value,
    }, nil
}

func (it *customIterator) Close() error {
    // Clean up resources if needed
    return nil
}
```

### Streaming Database Iterator

```go
type DBIterator struct {
    rows    *sql.Rows
    columns []string
    closed  bool
}

func (it *DBIterator) Next(ctx context.Context) (export.Row, error) {
    // Check context cancellation
    select {
    case <-ctx.Done():
        return nil, ctx.Err()
    default:
    }

    if it.closed {
        return nil, io.EOF
    }

    if !it.rows.Next() {
        if err := it.rows.Err(); err != nil {
            return nil, export.NewError(export.KindInternal, "row iteration error", err)
        }
        return nil, io.EOF
    }

    // Create value holders
    values := make([]any, len(it.columns))
    valuePtrs := make([]any, len(it.columns))
    for i := range values {
        valuePtrs[i] = &values[i]
    }

    if err := it.rows.Scan(valuePtrs...); err != nil {
        return nil, export.NewError(export.KindInternal, "scan error", err)
    }

    return export.Row(values), nil
}

func (it *DBIterator) Close() error {
    if it.closed {
        return nil
    }
    it.closed = true
    return it.rows.Close()
}
```

## Error Handling

Use the error types from the export package:

```go
import "github.com/goliatone/go-export/export"

// Validation errors
if input == "" {
    return nil, export.NewError(export.KindValidation, "input is required", nil)
}

// Not found errors
if record == nil {
    return nil, export.NewError(export.KindNotFound, "record not found", nil)
}

// Internal errors (wrap underlying errors)
if err != nil {
    return nil, export.NewError(export.KindInternal, "database query failed", err)
}

// Authorization errors
if !authorized {
    return nil, export.NewError(export.KindUnauthorized, "access denied", nil)
}
```

## Context Cancellation

Always respect context cancellation for graceful shutdown:

```go
func (it *Iterator) Next(ctx context.Context) (export.Row, error) {
    // Check at the start of each iteration
    select {
    case <-ctx.Done():
        return nil, ctx.Err()
    default:
    }

    // For long operations, check periodically
    for {
        select {
        case <-ctx.Done():
            return nil, ctx.Err()
        default:
            // Continue processing
        }

        // ... do work ...
        break
    }

    return row, nil
}
```

## Testing Row Sources

### Testing with Callback Source

```go
func TestExportUsers(t *testing.T) {
    // Create test data
    testUsers := [][]any{
        {"1", "test@example.com", "Test User"},
    }

    source := exportcallback.NewSource(func(ctx context.Context, spec export.RowSourceSpec) (export.RowIterator, error) {
        idx := 0
        return &exportcallback.FuncIterator{
            NextFunc: func(ctx context.Context) (export.Row, error) {
                if idx >= len(testUsers) {
                    return nil, io.EOF
                }
                row := testUsers[idx]
                idx++
                return row, nil
            },
        }, nil
    })

    // Test the source
    iter, err := source.Open(context.Background(), export.RowSourceSpec{})
    require.NoError(t, err)
    defer iter.Close()

    row, err := iter.Next(context.Background())
    require.NoError(t, err)
    assert.Equal(t, "1", row[0])
}
```

### Testing Iterator Behavior

```go
func TestIteratorEOF(t *testing.T) {
    source := exportcallback.NewSource(func(ctx context.Context, spec export.RowSourceSpec) (export.RowIterator, error) {
        return &exportcallback.FuncIterator{
            NextFunc: func(ctx context.Context) (export.Row, error) {
                return nil, io.EOF  // Empty result
            },
        }, nil
    })

    iter, err := source.Open(context.Background(), export.RowSourceSpec{})
    require.NoError(t, err)

    _, err = iter.Next(context.Background())
    assert.Equal(t, io.EOF, err)
}

func TestIteratorCancellation(t *testing.T) {
    ctx, cancel := context.WithCancel(context.Background())
    cancel()  // Cancel immediately

    source := exportcallback.NewSource(func(ctx context.Context, spec export.RowSourceSpec) (export.RowIterator, error) {
        return &exportcallback.FuncIterator{
            NextFunc: func(ctx context.Context) (export.Row, error) {
                select {
                case <-ctx.Done():
                    return nil, ctx.Err()
                default:
                    return export.Row{"data"}, nil
                }
            },
        }, nil
    })

    iter, _ := source.Open(ctx, export.RowSourceSpec{})
    _, err := iter.Next(ctx)
    assert.ErrorIs(t, err, context.Canceled)
}
```

## Performance Considerations

### Streaming Large Datasets

1. **Never load all data into memory** - Use cursors or pagination
2. **Use prepared statements** - Avoid query parsing overhead
3. **Batch fetches if needed** - Fetch N rows at a time internally
4. **Close resources promptly** - Implement `Close()` properly

```go
type BatchIterator struct {
    fetcher   func(offset, limit int) ([]Record, error)
    batch     []Record
    batchSize int
    offset    int
    index     int
}

func (it *BatchIterator) Next(ctx context.Context) (export.Row, error) {
    // Fetch next batch when current is exhausted
    if it.index >= len(it.batch) {
        batch, err := it.fetcher(it.offset, it.batchSize)
        if err != nil {
            return nil, err
        }
        if len(batch) == 0 {
            return nil, io.EOF
        }
        it.batch = batch
        it.offset += len(batch)
        it.index = 0
    }

    record := it.batch[it.index]
    it.index++
    return recordToRow(record), nil
}
```

### Connection Pooling

```go
type PooledSource struct {
    pool *sql.DB  // Connection pool, not single connection
}

func (s *PooledSource) Open(ctx context.Context, spec export.RowSourceSpec) (export.RowIterator, error) {
    // Get connection from pool
    conn, err := s.pool.Conn(ctx)
    if err != nil {
        return nil, err
    }

    rows, err := conn.QueryContext(ctx, "SELECT ...")
    if err != nil {
        conn.Close()
        return nil, err
    }

    return &pooledIterator{
        rows: rows,
        conn: conn,  // Return to pool on Close()
    }, nil
}
```

## Next Steps

- [GUIDE_DEFINITIONS.md](GUIDE_DEFINITIONS.md) - Configure export definitions
- [GUIDE_RENDERERS.md](GUIDE_RENDERERS.md) - Output format renderers
- [GUIDE_DELIVERY.md](GUIDE_DELIVERY.md) - Sync and async delivery modes
- [GUIDE_AUTHORIZATION.md](GUIDE_AUTHORIZATION.md) - Implement export guards
