package replaybun

import (
	"context"
	"errors"
	"github.com/goliatone/go-export/export"
	"github.com/uptrace/bun"
	"testing"
)

func TestTransactionProgressFlushAndFailure(t *testing.T) {
	svc, db, _ := fixture(t)
	ctx := context.Background()
	a := export.Actor{ID: "actor"}
	rec, e := svc.RequestExport(ctx, a, request())
	if e != nil {
		t.Fatal(e)
	}
	fail := errors.New("renderer failure")
	e = db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		tr := &transactionTracker{Tracker: svc.cfg.Tracker.WithTx(tx)}
		if e := tr.SetState(ctx, rec.ID, export.StateRunning, nil); e != nil {
			return e
		}
		if e := tr.Advance(ctx, rec.ID, export.ProgressDelta{Rows: 2, Bytes: 17}, nil); e != nil {
			return e
		}
		if e := tr.SetState(ctx, rec.ID, export.StatePublishing, nil); e != nil {
			return e
		}
		before, e := tr.Status(ctx, rec.ID)
		if e != nil {
			return e
		}
		if before.Counts.Processed != 0 || before.BytesWritten != 0 || before.State != export.StateRunning || before.StartedAt.IsZero() {
			t.Fatal("progress escaped buffer or started timestamp lost")
		}
		if e := tr.Fail(ctx, rec.ID, fail, nil); e != nil {
			return e
		}
		if !errors.Is(tr.Complete(ctx, rec.ID, nil), fail) {
			t.Fatal("failed generation was completed")
		}
		after, e := tr.Status(ctx, rec.ID)
		if e != nil {
			return e
		}
		if after.Counts.Processed != 0 || after.State != export.StateRunning {
			t.Fatal("failure wrote SQL before cursor close")
		}
		return fail
	})
	if !errors.Is(e, fail) {
		t.Fatal(e)
	}
	e = db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		tr := &transactionTracker{Tracker: svc.cfg.Tracker.WithTx(tx)}
		for range 2 {
			if e := tr.Advance(ctx, rec.ID, export.ProgressDelta{Rows: 1, Bytes: 8}, nil); e != nil {
				return e
			}
		}
		if e := tr.SetState(ctx, rec.ID, export.StatePublishing, nil); e != nil {
			return e
		}
		return tr.Complete(ctx, rec.ID, map[string]any{"rows": int64(2), "bytes": int64(16)})
	})
	if e != nil {
		t.Fatal(e)
	}
	after, e := svc.Status(ctx, a, rec.ID)
	if e != nil {
		t.Fatal(e)
	}
	if after.Counts.Processed != 2 || after.BytesWritten != 16 || after.State != export.StateCompleted {
		t.Fatalf("bad completion %+v", after)
	}
}
