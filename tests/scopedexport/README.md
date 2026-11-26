# Guarded Group export integration fixture

This nested module composes the real export HTTP adapter, SQL row source, Bun progress tracker and current native go-users Groups. Authentication is a synthetic trusted principal callback; this does not repeat INT-02's full login/session verification. Group labels remain independent of tenant/workspace. Host adapters enforce current native grants, SQL predicates before counts/selection/pages/history, safe text projection/redaction and scoped operational activity. The package's service guard and safe public errors are runtime fixes; host policies remain application responsibilities.

Run this nested module separately from the root suite against its published locks:

```sh
export GOWORK=off GOPROXY=https://proxy.golang.org GOSUMDB=sum.golang.org
export GOPRIVATE= GONOPROXY= GONOSUMDB=
export CC=/usr/bin/clang CXX=/usr/bin/clang++
go mod verify
go test -mod=readonly -race -count=1 ./...
```

The module has zero replacements and selects go-export v0.11.3, go-users v0.27.6 and go-router v0.65.3. SQLite is disposable by default. Optional PostgreSQL coverage requires `EXPORT_POSTGRES_DSN` and `EXPORT_POSTGRES_DISPOSABLE=1` pointing only to an owned disposable database. Do not use a shared application database. The SQLite qualification does not establish PostgreSQL qualification or full login/session behavior.
