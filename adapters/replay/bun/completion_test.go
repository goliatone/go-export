package replaybun

import (
	"context"
	"errors"
	"github.com/goliatone/go-export/export"
	"github.com/uptrace/bun"
	"testing"
)

func TestCompletionHooksShareAcceptanceAndGenerationTransaction(t *testing.T) {
	s, db, _ := fixture(t)
	db.SetMaxOpenConns(1)
	ctx := context.Background()
	a := export.Actor{ID: "actor"}
	if _, e := db.Exec("CREATE TABLE hook_events (export_id TEXT, phase TEXT)"); e != nil {
		t.Fatal(e)
	}
	fault := errors.New("publication failure")
	failQueued, failCompleted := true, true
	completedCalls := 0
	s.cfg.OnQueued = func(ctx context.Context, tx bun.Tx, a export.Actor, r export.ExportRecord) error {
		if _, e := tx.ExecContext(ctx, "INSERT INTO hook_events VALUES (?,?)", r.ID, "queued"); e != nil {
			return e
		}
		if failQueued {
			return fault
		}
		return nil
	}
	s.cfg.OnCompleted = func(ctx context.Context, tx bun.Tx, a export.Actor, r export.ExportRecord) error {
		completedCalls++
		if r.State != export.StateCompleted || r.Artifact.Key == "" || r.Counts.Processed != 1 {
			t.Fatal("callback preceded artifact/completion", r)
		}
		if _, e := tx.ExecContext(ctx, "INSERT INTO hook_events VALUES (?,?)", r.ID, "completed"); e != nil {
			return e
		}
		if failCompleted {
			return fault
		}
		return nil
	}
	if _, e := s.RequestExport(ctx, a, request()); e == nil {
		t.Fatal("queued hook failure ignored")
	}
	for _, table := range []string{"hook_events", "export_records", "export_replay_keys"} {
		if count(t, db, table) != 0 {
			t.Fatal("partial acceptance", table)
		}
	}
	failQueued = false
	r, e := s.RequestExport(ctx, a, request())
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.GenerateExport(ctx, a, r.ID, request()); e == nil {
		t.Fatal("completion publication failure ignored")
	}
	rec, e := s.cfg.Tracker.Status(ctx, r.ID)
	if e != nil || rec.State != export.StateQueued || count(t, db, "hook_events") != 1 {
		t.Fatal("completion escaped rollback", rec, e)
	}
	if _, e = s.DownloadMetadata(ctx, a, r.ID); e == nil {
		t.Fatal("provisional artifact downloadable")
	}
	failCompleted = false
	if _, e = s.GenerateExport(ctx, a, r.ID, request()); e != nil {
		t.Fatal(e)
	}
	if _, e = s.GenerateExport(ctx, a, r.ID, request()); e != nil {
		t.Fatal(e)
	}
	if completedCalls != 2 || count(t, db, "hook_events") != 2 {
		t.Fatal("completed replay repeated callback", completedCalls)
	}
	// Keyless async acceptance must capture a plan too.
	req := request()
	req.IdempotencyKey = ""
	if _, e = s.RequestExport(ctx, a, req); e != nil {
		t.Fatal(e)
	}
	if count(t, db, "hook_events") != 3 {
		t.Fatal("keyless callback skipped")
	}
}
