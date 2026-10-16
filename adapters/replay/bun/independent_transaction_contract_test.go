package replaybun

import (
	"context"
	"errors"
	"testing"

	"github.com/goliatone/go-export/export"
	"github.com/uptrace/bun"
)

func TestIndependentAsyncSubmissionHonorsTransaction(t *testing.T) {
	for _, mode := range []string{"deny", "rollback", "commit"} {
		t.Run(mode, func(t *testing.T) {
			for _, keyed := range []bool{true, false} {
				name := "unkeyed"
				if keyed {
					name = "keyed"
				}
				t.Run(name, func(t *testing.T) {
					svc, db, _ := fixture(t)
					calls := 0
					rejected := errors.New("authorized transaction rejected")
					svc.cfg.WithTransaction = func(ctx context.Context, a export.Actor, fn func(context.Context, bun.Tx) error) error {
						calls++
						if mode == "deny" {
							return rejected
						}
						return db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
							if err := fn(ctx, tx); err != nil {
								return err
							}
							if mode == "rollback" {
								return rejected
							}
							return nil
						})
					}
					req := request()
					if !keyed {
						req.IdempotencyKey = ""
					}
					rec, err := svc.RequestExport(context.Background(), export.Actor{ID: "actor"}, req)
					want := 0
					if mode == "commit" {
						want = 1
					}
					if calls != 1 {
						t.Errorf("configured authorized transaction called %d times; want1 (keyed=%v)", calls, keyed)
					}
					if mode == "commit" {
						if err != nil || rec.ID == "" {
							t.Fatalf("authorized commit failed: %+v %v", rec, err)
						}
					} else if err == nil || rec.ID != "" {
						t.Errorf("rejected transaction returned accepted export: %+v err=%v", rec, err)
					}
					if n := count(t, db, "export_records"); n != want {
						t.Errorf("queued records=%d want%d after %s", n, want, mode)
					}
					keyWant := 0
					if keyed && mode == "commit" {
						keyWant = 1
					}
					if n := count(t, db, "export_replay_keys"); n != keyWant {
						t.Errorf("replay reservations=%d want%d", n, keyWant)
					}
				})
			}
		})
	}
}
