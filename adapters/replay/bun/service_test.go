package replaybun

import (
	"context"
	"database/sql"
	storefs "github.com/goliatone/go-export/adapters/store/fs"
	trackerbun "github.com/goliatone/go-export/adapters/tracker/bun"
	"github.com/goliatone/go-export/export"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/sqlitedialect"
	"github.com/uptrace/bun/driver/sqliteshim"
	"io"
	"path/filepath"
	"testing"
	"time"
)

type source struct{}

func (source) Open(context.Context, export.RowSourceSpec) (export.RowIterator, error) {
	return &rows{}, nil
}

type rows struct{ done bool }

func (r *rows) Next(context.Context) (export.Row, error) {
	if r.done {
		return nil, io.EOF
	}
	r.done = true
	return export.Row{"one"}, nil
}
func (*rows) Close() error { return nil }
func fixture(t *testing.T) (*Service, *bun.DB, *time.Time) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "replay.sqlite")
	raw, e := sql.Open(sqliteshim.ShimName, path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if e != nil {
		t.Fatal(e)
	}
	db := bun.NewDB(raw, sqlitedialect.New())
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	if _, e = db.NewCreateTable().Model((*replayTestRecord)(nil)).Exec(ctx); e != nil {
		t.Fatal(e)
	}
	if e = Migrate(ctx, db); e != nil {
		t.Fatal(e)
	}
	runner := export.NewRunner()
	e = runner.Definitions.Register(export.ExportDefinition{Name: "records", RowSourceKey: "rows", Schema: export.Schema{Columns: []export.Column{{Name: "id"}}}})
	if e != nil {
		t.Fatal(e)
	}
	e = runner.RowSources.Register("rows", func(export.ExportRequest, export.ResolvedDefinition) (export.RowSource, error) { return source{}, nil })
	if e != nil {
		t.Fatal(e)
	}
	now := time.Now().UTC()
	tr := trackerbun.NewTracker(db)
	s, e := NewService(Config{Service: export.ServiceConfig{Runner: runner, Tracker: tr, Store: storefs.NewDurableStore(t.TempDir()), Now: func() time.Time { return now }}, Tracker: tr, TTL: time.Minute, Namespace: func(context.Context, export.Actor) (string, error) { return "trusted-scope", nil }})
	if e != nil {
		t.Fatal(e)
	}
	return s, db, &now
}
func request() export.ExportRequest {
	return export.ExportRequest{Definition: "records", Format: export.FormatCSV, Delivery: export.DeliveryAsync, IdempotencyKey: "operation"}
}
func count(t *testing.T, db *bun.DB, table string) int {
	t.Helper()
	n, e := db.NewSelect().Table(table).Count(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	return n
}
func TestReplayTransactionRollbackAndTTL(t *testing.T) {
	s, db, clock := fixture(t)
	ctx := context.Background()
	actor := export.Actor{ID: "actor"}
	_, e := db.Exec("CREATE TRIGGER reject_export BEFORE INSERT ON export_records BEGIN SELECT RAISE(ABORT,'injected'); END")
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.RequestExport(ctx, actor, request()); e == nil {
		t.Fatal("injected SQL failure ignored")
	}
	if count(t, db, "export_records") != 0 || count(t, db, "export_replay_keys") != 0 {
		t.Fatal("transaction left partial identity")
	}
	if _, e = db.Exec("DROP TRIGGER reject_export"); e != nil {
		t.Fatal(e)
	}
	first, e := s.RequestExport(ctx, actor, request())
	if e != nil {
		t.Fatal(e)
	}
	same, e := s.RequestExport(ctx, actor, request())
	if e != nil || same.ID != first.ID {
		t.Fatalf("replay: %+v %v", same, e)
	}
	*clock = clock.Add(time.Minute)
	next, e := s.RequestExport(ctx, actor, request())
	if e != nil || next.ID == first.ID {
		t.Fatalf("TTL boundary: %+v %v", next, e)
	}
	if count(t, db, "export_records") != 2 || count(t, db, "export_replay_keys") != 1 {
		t.Fatal("unexpected expiry identity state")
	}
}
func TestReplayPersistsRequestAndReusesCompletedResult(t *testing.T) {
	s, db, _ := fixture(t)
	ctx := context.Background()
	a := export.Actor{ID: "actor"}
	r, e := s.RequestExport(ctx, a, request())
	if e != nil {
		t.Fatal(e)
	}
	persisted, e := s.cfg.Tracker.Status(ctx, r.ID)
	if e != nil || persisted.Request.Definition != "records" || persisted.Request.IdempotencyKey != "" || persisted.Request.Output != nil {
		t.Fatalf("unsafe/missing request: %+v %v", persisted.Request, e)
	}
	first, e := s.GenerateExport(ctx, a, r.ID, request())
	if e != nil {
		t.Fatal(e)
	}
	second, e := s.GenerateExport(ctx, a, r.ID, request())
	if e != nil {
		t.Fatal(e)
	}
	if first.ID != second.ID || first.Rows != second.Rows || first.Bytes != second.Bytes || first.Delivery != second.Delivery || first.Filename != second.Filename {
		t.Fatalf("result changed: %+v %+v", first, second)
	}
	record, e := s.cfg.Tracker.Status(ctx, r.ID)
	if e != nil || record.Counts.Processed != 1 || record.State != export.StateCompleted {
		t.Fatalf("duplicate completion: %+v %v", record, e)
	}
	if count(t, db, "export_records") != 1 {
		t.Fatal("generation duplicated records")
	}
}
