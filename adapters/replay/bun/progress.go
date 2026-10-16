package replaybun

import (
	"context"
	trackerbun "github.com/goliatone/go-export/adapters/tracker/bun"
	"github.com/goliatone/go-export/export"
)

// transactionTracker keeps per-row progress off the SQL connection while a
// transaction-aware row source may have an active cursor on that connection.
// Progress is private until the enclosing generation transaction commits, so
// coalescing these deltas preserves externally visible semantics and constant
// memory use. Complete runs after the native runner closes its iterator and the
// artifact store acknowledges publication. Errors still roll back everything.
type transactionTracker struct {
	*trackerbun.Tracker
	id         string
	delta      export.ProgressDelta
	publishing bool
	terminal   error
}

func (t *transactionTracker) check(id string) error {
	if id == "" || (t.id != "" && t.id != id) {
		return export.NewError(export.KindValidation, "transaction tracker requires one export ID", nil)
	}
	t.id = id
	return nil
}
func (t *transactionTracker) Advance(ctx context.Context, id string, d export.ProgressDelta, _ map[string]any) error {
	if e := ctx.Err(); e != nil {
		return e
	}
	if e := t.check(id); e != nil {
		return e
	}
	t.delta.Rows += d.Rows
	t.delta.Bytes += d.Bytes
	return nil
}
func (t *transactionTracker) SetState(ctx context.Context, id string, state export.ExportState, meta map[string]any) error {
	if e := t.check(id); e != nil {
		return e
	}
	if state == export.StateRunning {
		return t.Tracker.SetState(ctx, id, state, meta)
	}
	if state == export.StateCanceled {
		t.terminal = context.Canceled
		return nil
	}
	if state == export.StateFailed {
		t.terminal = export.NewError(export.KindInternal, "generation failed", nil)
		return nil
	}
	if state != export.StatePublishing {
		return export.NewError(export.KindValidation, "unexpected generation state", nil)
	}
	if e := ctx.Err(); e != nil {
		return e
	}
	if e := t.check(id); e != nil {
		return e
	}
	t.publishing = true
	return nil
}
func (t *transactionTracker) Complete(ctx context.Context, id string, meta map[string]any) error {
	if e := t.check(id); e != nil {
		return e
	}
	if t.terminal != nil {
		return t.terminal
	}
	if t.delta.Rows != 0 || t.delta.Bytes != 0 {
		if e := t.Tracker.Advance(ctx, id, t.delta, nil); e != nil {
			return e
		}
		t.delta = export.ProgressDelta{}
	}
	if t.publishing {
		if e := t.Tracker.SetState(ctx, id, export.StatePublishing, nil); e != nil {
			return e
		}
		t.publishing = false
	}
	return t.Tracker.Complete(ctx, id, meta)
}

// A failed generation is rolled back by Service.GenerateExport. Do not issue a
// SQL failure write while its source cursor may still be open.
func (t *transactionTracker) Fail(ctx context.Context, id string, err error, _ map[string]any) error {
	if e := t.check(id); e != nil {
		return e
	}
	if err == nil {
		err = export.NewError(export.KindInternal, "generation failed", nil)
	}
	t.terminal = err
	return ctx.Err()
}
