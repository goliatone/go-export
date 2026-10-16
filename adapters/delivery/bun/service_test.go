package deliverybun

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	delivery "github.com/goliatone/go-export/adapters/delivery"
	"github.com/goliatone/go-export/export"
	job "github.com/goliatone/go-job"
	"github.com/goliatone/go-job/queue"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/sqlitedialect"
	"github.com/uptrace/bun/driver/sqliteshim"
)

type testQueue struct {
	db   *bun.DB
	fail bool
}

func (q *testQueue) Database() *sql.DB { return q.db.DB }
func (q *testQueue) EnqueueTx(ctx context.Context, tx *sql.Tx, m *job.ExecutionMessage, at time.Time) (queue.EnqueueReceipt, error) {
	if q.fail {
		return queue.EnqueueReceipt{}, errors.New("queue fault")
	}
	b, e := json.Marshal(m)
	if e != nil {
		return queue.EnqueueReceipt{}, e
	}
	_, e = tx.ExecContext(ctx, "INSERT INTO test_queue (payload) VALUES (?)", string(b))
	return queue.EnqueueReceipt{}, e
}

type testSender struct {
	n    int
	body string
	fail bool
}

func (s *testSender) SendReceipt(_ context.Context, m delivery.WebhookDelivery) error {
	s.n++
	s.body = string(m.Body)
	if s.fail {
		return errors.New("uncertain response")
	}
	return nil
}
func openTestDB(t *testing.T, path string) *bun.DB {
	t.Helper()
	db, e := sql.Open(sqliteshim.ShimName, "file:"+path+"?_pragma=busy_timeout(5000)&_txlock=immediate")
	if e != nil {
		t.Fatal(e)
	}
	db.SetMaxOpenConns(1)
	b := bun.NewDB(db, sqlitedialect.New())
	t.Cleanup(func() { b.Close() })
	return b
}
func testService(t *testing.T) (*Service, *testQueue, *testSender, *bool) {
	t.Helper()
	db := openTestDB(t, filepath.Join(t.TempDir(), "send.sqlite"))
	ctx := context.Background()
	if e := Migrate(ctx, db); e != nil {
		t.Fatal(e)
	}
	if _, e := db.Exec("CREATE TABLE test_queue (payload TEXT)"); e != nil {
		t.Fatal(e)
	}
	q := &testQueue{db: db}
	sender := &testSender{}
	allowed := true
	cfg := Config{DB: db, Queue: q, Sender: sender, AuthorizeTarget: func(context.Context, export.Actor, string, Target) error { return nil }, Namespace: func(context.Context, export.Actor) (string, error) { return "group:A", nil }, Link: func(export.ExportRecord) (string, error) { return "/guarded/exports/id/download", nil }, Authorize: func(ctx context.Context, _ export.Actor, _ string) (context.Context, error) {
		if !allowed {
			return ctx, denied()
		}
		return ctx, nil
	}, WithAuthorizedTransaction: func(ctx context.Context, _ export.Actor, id string, fn func(context.Context, bun.Tx, export.ExportRecord) error) error {
		if !allowed {
			return denied()
		}
		return db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
			return fn(ctx, tx, export.ExportRecord{ID: id, State: export.StateCompleted, Definition: "resources", Format: export.FormatCSV})
		})
	}}
	s, e := NewService(cfg)
	if e != nil {
		t.Fatal(e)
	}
	return s, q, sender, &allowed
}
func scheduleReq() ScheduleRequest {
	return ScheduleRequest{ExportID: "export", Key: "ready.v1", Targets: []Target{{ID: "first", URL: "https://example.com/one"}, {ID: "second", URL: "https://example.com/two"}}, Metadata: map[string]any{"label": "original"}}
}
func TestDurableWebhookAtomicReplayAndScope(t *testing.T) {
	s, q, sender, allowed := testService(t)
	ctx := context.Background()
	a := export.Actor{ID: "actor"}
	req := scheduleReq()
	q.fail = true
	if _, e := s.Schedule(ctx, a, req); e == nil {
		t.Fatal("queue failure acknowledged")
	}
	q.fail = false
	for _, table := range []string{"export_webhook_operations", "export_webhook_targets", "test_queue"} {
		n, _ := s.cfg.DB.NewSelect().Table(table).Count(ctx)
		if n != 0 {
			t.Fatal("partial commit", table, n)
		}
	}
	r, e := s.Schedule(ctx, a, req)
	if e != nil {
		t.Fatal(e)
	}
	// Replay with order changed must retain original IDs/body and queue count.
	req.Targets[0], req.Targets[1] = req.Targets[1], req.Targets[0]
	again, e := s.Schedule(ctx, a, req)
	if e != nil || again[0].ID != r[0].ID {
		t.Fatal("replay", again, e)
	}
	n, _ := s.cfg.DB.NewSelect().Table("test_queue").Count(ctx)
	if n != 2 {
		t.Fatal("reenqueued", n)
	}
	req.Metadata["label"] = "changed"
	if _, e = s.Schedule(ctx, a, req); e == nil {
		t.Fatal("content conflict accepted")
	}
	if e = s.DeliverTarget(ctx, a, "export", r[0].ID); e != nil {
		t.Fatal(e)
	}
	if e = s.DeliverTarget(ctx, a, "export", r[0].ID); e != nil || sender.n != 1 {
		t.Fatal("completed target resent", e, sender.n)
	}
	*allowed = false
	if _, e = s.Status(ctx, a, "export", r[0].ID); e == nil {
		t.Fatal("revoked receipt read")
	}
	if e = s.DeliverTarget(ctx, a, "export", r[1].ID); e == nil || sender.n != 1 {
		t.Fatal("revoked send")
	}
	if _, e = s.Schedule(ctx, a, scheduleReq()); e == nil {
		t.Fatal("revoked replay")
	}
	*allowed = true
	if e = s.DeliverTarget(ctx, export.Actor{ID: "other"}, "export", r[1].ID); e == nil {
		t.Fatal("cross actor")
	}
	if e = s.DeliverTarget(ctx, export.Actor{ID: "actor", Scope: export.Scope{TenantID: "other"}}, "export", r[1].ID); e == nil {
		t.Fatal("cross tenant")
	}
	s.cfg.Namespace = func(context.Context, export.Actor) (string, error) { return "group:B", nil }
	if e = s.DeliverTarget(ctx, a, "export", r[1].ID); e == nil {
		t.Fatal("cross group")
	}
}
func TestDurableWebhookUncertainReceiptAndCorruption(t *testing.T) {
	s, _, sender, _ := testService(t)
	ctx := context.Background()
	a := export.Actor{ID: "actor"}
	req := scheduleReq()
	req.Metadata["large_integer"] = int64(9007199254740993)
	r, e := s.Schedule(ctx, a, req)
	if e != nil {
		t.Fatal(e)
	}
	sender.fail = true
	if e = s.DeliverTarget(ctx, a, "export", r[0].ID); e == nil {
		t.Fatal("uncertain ack accepted")
	}
	body := sender.body
	if !strings.Contains(body, `"large_integer":9007199254740993`) {
		t.Fatal("metadata integer lost precision", body)
	}
	status, e := s.Status(ctx, a, "export", r[0].ID)
	if e != nil || !status.DeliveredAt.IsZero() {
		t.Fatal("false receipt", status, e)
	}
	// Rebuild service (as after restart); persisted bytes remain authoritative.
	s, e = NewService(s.cfg)
	if e != nil {
		t.Fatal(e)
	}
	sender.fail = false
	if e = s.DeliverTarget(ctx, a, "export", r[0].ID); e != nil || sender.body != body {
		t.Fatal("retry changed bytes", e)
	}
	_, e = s.cfg.DB.Exec("UPDATE export_webhook_targets SET body='{}' WHERE id=?", r[1].ID)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.DeliverTarget(ctx, a, "export", r[1].ID); e == nil || sender.n != 2 {
		t.Fatal("corrupt body sent")
	}
}
func TestReceiverAtomicEffectConflictAndConcurrency(t *testing.T) {
	db := openTestDB(t, filepath.Join(t.TempDir(), "receiver.sqlite"))
	ctx := context.Background()
	if e := MigrateReceiver(ctx, db); e != nil {
		t.Fatal(e)
	}
	if _, e := db.Exec("CREATE TABLE effects (key TEXT)"); e != nil {
		t.Fatal(e)
	}
	r := &Receiver{DB: db}
	body := json.RawMessage(`{"export_id":"export"}`)
	key := strings.Repeat("a", 64)
	digest := delivery.PayloadDigest(body)
	apply := func(ctx context.Context, tx bun.Tx, _ json.RawMessage) error {
		_, e := tx.ExecContext(ctx, "INSERT INTO effects VALUES (?)", key)
		return e
	}
	fault := errors.New("rollback effect")
	if e := r.Receive(ctx, "trusted-A", key, digest, body, func(ctx context.Context, tx bun.Tx, b json.RawMessage) error {
		if e := apply(ctx, tx, b); e != nil {
			return e
		}
		return fault
	}); !errors.Is(e, fault) {
		t.Fatal(e)
	}
	for _, table := range []string{"export_webhook_inbox", "effects"} {
		n, _ := db.NewSelect().Table(table).Count(ctx)
		if n != 0 {
			t.Fatal("partial receiver effect", table)
		}
	}
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- r.Receive(ctx, "trusted-A", key, digest, body, apply) }()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	n, _ := db.NewSelect().Table("effects").Count(ctx)
	if n != 1 {
		t.Fatal("duplicate receiver effect", n)
	}
	changed := json.RawMessage(`{"export_id":"other"}`)
	if e := r.Receive(ctx, "trusted-A", key, delivery.PayloadDigest(changed), changed, apply); e == nil {
		t.Fatal("key/body conflict accepted")
	}
	if e := r.Receive(ctx, "trusted-A", key, digest, changed, apply); e == nil {
		t.Fatal("forged digest")
	}
	if e := r.Receive(ctx, "trusted-B", key, digest, body, apply); e != nil {
		t.Fatal(e)
	}
	n, _ = db.NewSelect().Table("effects").Count(ctx)
	if n != 2 {
		t.Fatal("receiver namespace conflation")
	}
}
