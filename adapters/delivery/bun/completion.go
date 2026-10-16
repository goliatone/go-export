package deliverybun

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	delivery "github.com/goliatone/go-export/adapters/delivery"
	replaybun "github.com/goliatone/go-export/adapters/replay/bun"
	"github.com/goliatone/go-export/export"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

// CompletionConfig selects the first notification plan inside export acceptance.
// Every new async record on this opt-in service requires a plan. Neither policy
// selection nor transactional authorization may perform network I/O or commit.
type CompletionConfig struct {
	Delivery *Service
	Select   func(context.Context, export.Actor, export.ExportRecord) (ScheduleRequest, error)
	// AuthorizeTransaction verifies current identity/export scope using the
	// supplied transaction or its trusted host fence, without opening another tx.
	AuthorizeTransaction func(context.Context, bun.Tx, export.Actor, string) error
}
type CompletionPublisher struct{ cfg CompletionConfig }
type completionPlan struct {
	bun.BaseModel `bun:"table:export_webhook_completion_plans"`
	ExportID      string    `bun:",pk"`
	Namespace     string    `bun:",notnull"`
	Owner         string    `bun:",notnull"`
	Request       string    `bun:",notnull"`
	Digest        string    `bun:",notnull"`
	PublishedAt   time.Time `bun:",nullzero"`
}

// MigrateCompletion adds immutable plan/publication markers. Run Migrate too.
func MigrateCompletion(ctx context.Context, db *bun.DB) error {
	if db == nil {
		return invalid("completion database required")
	}
	_, e := db.NewCreateTable().Model((*completionPlan)(nil)).IfNotExists().Exec(ctx)
	return e
}
func NewCompletionPublisher(cfg CompletionConfig) (*CompletionPublisher, error) {
	if cfg.Delivery == nil || cfg.Delivery.cfg.DB == nil || cfg.Select == nil || cfg.AuthorizeTransaction == nil {
		return nil, invalid("completion publication requires delivery, plan selector and transaction authorization")
	}
	return &CompletionPublisher{cfg: cfg}, nil
}

// Bind installs exclusive hooks on the delivery database. Bind before composing
// jobbun.NewScheduler, which supplies the generation enqueue callback itself.
func (p *CompletionPublisher) Bind(cfg replaybun.Config) (replaybun.Config, error) {
	if p == nil || p.cfg.Delivery == nil || cfg.Tracker == nil || cfg.Tracker.DB != p.cfg.Delivery.cfg.DB || cfg.OnQueued != nil || cfg.OnCompleted != nil {
		return cfg, invalid("completion hooks require the same database and exclusive ownership")
	}
	cfg.OnQueued = p.OnQueued
	cfg.OnCompleted = p.OnCompleted
	return cfg, nil
}
func (p *CompletionPublisher) scope(ctx context.Context, tx bun.Tx, a export.Actor, rec export.ExportRecord) (string, error) {
	if p == nil || p.cfg.Delivery == nil || p.cfg.AuthorizeTransaction == nil || a.ID == "" || rec.ID == "" {
		return "", denied()
	}
	if e := p.cfg.AuthorizeTransaction(ctx, tx, a, rec.ID); e != nil {
		return "", e
	}
	return p.cfg.Delivery.namespace(ctx, a)
}
func (p *CompletionPublisher) load(ctx context.Context, tx bun.Tx, a export.Actor, ns, id string) (completionPlan, error) {
	row := completionPlan{ExportID: id}
	q := tx.NewSelect().Model(&row).WherePK()
	if p.cfg.Delivery.cfg.DB.Dialect().Name() == dialect.PG {
		q = q.For("UPDATE")
	}
	if e := q.Scan(ctx); e != nil {
		return row, e
	}
	if row.Namespace != ns || row.Owner != owner(a) {
		return completionPlan{}, denied()
	}
	if row.Digest != deliveryDigest(row.Request) {
		return completionPlan{}, invalid("stored completion plan is invalid")
	}
	return row, nil
}
func deliveryDigest(body string) string { return delivery.PayloadDigest([]byte(body)) }
func decodePlan(body string, req *ScheduleRequest) error {
	d := json.NewDecoder(bytes.NewReader([]byte(body)))
	d.UseNumber()
	if e := d.Decode(req); e != nil {
		return invalid("stored completion plan is invalid")
	}
	return nil
}

// OnQueued freezes the first plan with the queued record and generation message.
// The native service invokes it only for newly accepted records, including
// keyless requests. Authorized explicit adoption can use the same hook.
func (p *CompletionPublisher) OnQueued(ctx context.Context, tx bun.Tx, a export.Actor, rec export.ExportRecord) error {
	ns, e := p.scope(ctx, tx, a, rec)
	if e != nil {
		return e
	}
	if rec.State != export.StateQueued {
		return invalid("queued export required for completion plan")
	}
	if _, e = p.load(ctx, tx, a, ns, rec.ID); e == nil {
		return nil
	} else if !errors.Is(e, sql.ErrNoRows) {
		return e
	}
	req, e := p.cfg.Select(ctx, a, rec)
	if e != nil {
		return e
	}
	if req.ExportID != "" && req.ExportID != rec.ID {
		return invalid("completion plan export does not match")
	}
	req.ExportID = rec.ID
	req, e = prepareSchedule(req)
	if e != nil {
		return e
	}
	for _, target := range req.Targets {
		if e = p.cfg.Delivery.cfg.AuthorizeTarget(ctx, a, rec.ID, target); e != nil {
			return e
		}
	}
	body, e := json.Marshal(req)
	if e != nil {
		return invalid("completion plan cannot be persisted")
	}
	row := completionPlan{ExportID: rec.ID, Namespace: ns, Owner: owner(a), Request: string(body), Digest: deliveryDigest(string(body))}
	if _, e = tx.NewInsert().Model(&row).On("CONFLICT (export_id) DO NOTHING").Exec(ctx); e != nil {
		return e
	}
	_, e = p.load(ctx, tx, a, ns, rec.ID)
	return e
}

// OnCompleted publishes only the persisted plan in the caller's completion
// transaction. It never opens nested writes or sends HTTP. A publication failure
// rolls back completed tracking; a provisional artifact stays guarded as queued.
func (p *CompletionPublisher) OnCompleted(ctx context.Context, tx bun.Tx, a export.Actor, rec export.ExportRecord) error {
	ns, e := p.scope(ctx, tx, a, rec)
	if e != nil {
		return e
	}
	if rec.State != export.StateCompleted {
		return invalid("completed export required for publication")
	}
	row, e := p.load(ctx, tx, a, ns, rec.ID)
	if errors.Is(e, sql.ErrNoRows) {
		return invalid("persisted completion plan required")
	}
	if e != nil {
		return e
	}
	if !row.PublishedAt.IsZero() {
		return nil
	}
	// Preserve number precision and all canonical intent fields through the
	// existing scheduler, which normalizes and reauthorizes destination policy.
	var req ScheduleRequest
	if e = decodePlan(row.Request, &req); e != nil {
		return e
	}
	if req.ExportID != rec.ID {
		return invalid("completion plan export does not match")
	}
	bound := *p.cfg.Delivery
	bound.cfg.WithAuthorizedTransaction = func(ctx context.Context, _ export.Actor, _ string, fn func(context.Context, bun.Tx, export.ExportRecord) error) error {
		return fn(ctx, tx, rec)
	}
	if _, e = bound.Schedule(ctx, a, req); e != nil {
		return e
	}
	at := p.cfg.Delivery.cfg.Now()
	if at.IsZero() {
		return invalid("invalid completion publication clock")
	}
	result, e := tx.NewUpdate().Model((*completionPlan)(nil)).Set("published_at=?", at).Where("export_id=?", rec.ID).Exec(ctx)
	if e != nil {
		return e
	}
	n, e := result.RowsAffected()
	if e != nil {
		return e
	}
	if n != 1 {
		return invalid("completion publication marker not committed")
	}
	return nil
}
