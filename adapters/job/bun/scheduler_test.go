package jobbun

import (
	"context"
	"database/sql"
	"testing"
	"time"

	replaybun "github.com/goliatone/go-export/adapters/replay/bun"
	trackerbun "github.com/goliatone/go-export/adapters/tracker/bun"
	"github.com/goliatone/go-export/export"
	job "github.com/goliatone/go-job"
	"github.com/goliatone/go-job/queue"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/sqlitedialect"
	"github.com/uptrace/bun/driver/sqliteshim"
)

type transactionQueue struct{ db *sql.DB }

func (q transactionQueue) Database() *sql.DB { return q.db }
func (transactionQueue) EnqueueTx(context.Context, *sql.Tx, *job.ExecutionMessage, time.Time) (queue.EnqueueReceipt, error) {
	panic("construction must not enqueue")
}
func TestSchedulerRequiresSharedDatabaseAndExclusiveHook(t *testing.T) {
	raw, e := sql.Open(sqliteshim.ShimName, ":memory:")
	if e != nil {
		t.Fatal(e)
	}
	defer raw.Close()
	other, e := sql.Open(sqliteshim.ShimName, ":memory:")
	if e != nil {
		t.Fatal(e)
	}
	defer other.Close()
	db := bun.NewDB(raw, sqlitedialect.New())
	cfg := Config{Replay: replaybun.Config{Tracker: trackerbun.NewTracker(db), Namespace: func(context.Context, export.Actor) (string, error) { return "trusted", nil }, Service: export.ServiceConfig{Runner: export.NewRunner(), Store: export.NewMemoryStore()}}}
	if _, e := NewScheduler(cfg); e == nil {
		t.Fatal("accepted missing queue")
	}
	cfg.Queue = transactionQueue{other}
	if _, e := NewScheduler(cfg); e == nil {
		t.Fatal("accepted a different queue database")
	}
	cfg.Queue = transactionQueue{raw}
	if _, e := NewScheduler(cfg); e != nil {
		t.Fatal(e)
	}
	cfg.Replay.Enqueue = func(context.Context, bun.Tx, export.Actor, export.ExportRecord) error { return nil }
	if _, e := NewScheduler(cfg); e == nil {
		t.Fatal("silently replaced an existing transactional enqueue hook")
	}
}
