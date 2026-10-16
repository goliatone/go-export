package deliverybun

import (
	"context"
	"errors"
	"github.com/goliatone/go-export/export"
	"github.com/uptrace/bun"
	"strings"
	"testing"
)

func TestCompletionPublisherFreezesPlanAndRollsBackPublication(t *testing.T) {
	s, q, _, _ := testService(t)
	ctx := context.Background()
	if e := MigrateCompletion(ctx, s.cfg.DB); e != nil {
		t.Fatal(e)
	}
	a := export.Actor{ID: "actor"}
	rec := export.ExportRecord{ID: "export", State: export.StateQueued, Definition: "resources", Format: export.FormatCSV}
	selected := 0
	p, e := NewCompletionPublisher(CompletionConfig{Delivery: s, Select: func(context.Context, export.Actor, export.ExportRecord) (ScheduleRequest, error) {
		selected++
		r := scheduleReq()
		r.Metadata["integer"] = int64(9007199254740993)
		return r, nil
	}, AuthorizeTransaction: func(context.Context, bun.Tx, export.Actor, string) error { return nil }})
	if e != nil {
		t.Fatal(e)
	}
	txrun := func(fn func(context.Context, bun.Tx, export.Actor, export.ExportRecord) error) error {
		return s.cfg.DB.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error { return fn(ctx, tx, a, rec) })
	}
	if e = txrun(p.OnQueued); e != nil {
		t.Fatal(e)
	}
	p.cfg.Select = func(context.Context, export.Actor, export.ExportRecord) (ScheduleRequest, error) {
		return ScheduleRequest{}, errors.New("current configuration must not refresh saved plan")
	}
	if e = txrun(p.OnQueued); e != nil || selected != 1 {
		t.Fatal("saved plan not reused", e)
	}
	rec.State = export.StateCompleted
	q.fail = true
	if e = txrun(p.OnCompleted); e == nil {
		t.Fatal("queue failure acknowledged")
	}
	q.fail = false
	for _, table := range []string{"export_webhook_operations", "export_webhook_targets", "test_queue"} {
		n, _ := s.cfg.DB.NewSelect().Table(table).Count(ctx)
		if n != 0 {
			t.Fatal("partial publication", table)
		}
	}
	if _, e = s.cfg.DB.Exec("CREATE TRIGGER fail_marker BEFORE UPDATE ON export_webhook_completion_plans BEGIN SELECT RAISE(ABORT,'marker-fault'); END"); e != nil {
		t.Fatal(e)
	}
	if e = txrun(p.OnCompleted); e == nil {
		t.Fatal("marker failure acknowledged")
	}
	if _, e = s.cfg.DB.Exec("DROP TRIGGER fail_marker"); e != nil {
		t.Fatal(e)
	}
	if e = txrun(p.OnCompleted); e != nil {
		t.Fatal(e)
	}
	if e = txrun(p.OnCompleted); e != nil {
		t.Fatal(e)
	}
	var row completionPlan
	if e = s.cfg.DB.NewSelect().Model(&row).Scan(ctx); e != nil || row.PublishedAt.IsZero() {
		t.Fatal("publication marker missing", e)
	}
	if !strings.Contains(row.Request, `"integer":9007199254740993`) {
		t.Fatal("plan precision lost")
	}
	n, _ := s.cfg.DB.NewSelect().Table("test_queue").Count(ctx)
	if n != 2 {
		t.Fatal("duplicate/lost jobs", n)
	}
	var body string
	if e = s.cfg.DB.QueryRow("SELECT body FROM export_webhook_targets LIMIT 1").Scan(&body); e != nil || !strings.Contains(body, `"integer":9007199254740993`) {
		t.Fatal("publication precision lost", e)
	}
}
func TestCompletionPublisherMissingPlanAndAuthorization(t *testing.T) {
	s, _, _, _ := testService(t)
	ctx := context.Background()
	if e := MigrateCompletion(ctx, s.cfg.DB); e != nil {
		t.Fatal(e)
	}
	allow := false
	p, e := NewCompletionPublisher(CompletionConfig{Delivery: s, Select: func(context.Context, export.Actor, export.ExportRecord) (ScheduleRequest, error) {
		return scheduleReq(), nil
	}, AuthorizeTransaction: func(context.Context, bun.Tx, export.Actor, string) error {
		if !allow {
			return denied()
		}
		return nil
	}})
	if e != nil {
		t.Fatal(e)
	}
	r := export.ExportRecord{ID: "export", State: export.StateCompleted}
	a := export.Actor{ID: "actor"}
	run := func() error {
		return s.cfg.DB.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error { return p.OnCompleted(ctx, tx, a, r) })
	}
	if e = run(); e == nil {
		t.Fatal("missing authorization accepted")
	}
	allow = true
	if e = run(); e == nil {
		t.Fatal("missing plan silently completed")
	}
	n, _ := s.cfg.DB.NewSelect().Table("test_queue").Count(ctx)
	if n != 0 {
		t.Fatal("missing plan queued")
	}
}
