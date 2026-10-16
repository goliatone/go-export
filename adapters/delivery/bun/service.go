// Package deliverybun coordinates durable webhook intent, queue acceptance and receipts.
package deliverybun

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"

	delivery "github.com/goliatone/go-export/adapters/delivery"
	exportjob "github.com/goliatone/go-export/adapters/job"
	jobbun "github.com/goliatone/go-export/adapters/job/bun"
	cmd "github.com/goliatone/go-export/command"
	"github.com/goliatone/go-export/export"
	job "github.com/goliatone/go-job"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

type Target = delivery.WebhookTargetSpec
type ScheduleRequest = delivery.ScheduleWebhooksRequest
type Config struct {
	DB     *bun.DB
	Queue  jobbun.TransactionalQueue
	Sender delivery.ReceiptWebhookSender
	// Namespace must derive from trusted scope. It is independent of Actor.Scope.
	Namespace func(context.Context, export.Actor) (string, error)
	// WithAuthorizedTransaction must check current identity, grants, export
	// ownership and completed artifact metadata in tx, then invoke fn with that
	// same DB transaction and trusted context. It owns rollback/commit, as for
	// replay/Bun. No callback may commit or perform external I/O.
	WithAuthorizedTransaction func(context.Context, export.Actor, string, func(context.Context, bun.Tx, export.ExportRecord) error) error
	// Authorize rechecks current trusted policy before receipt reads or sending.
	Authorize func(context.Context, export.Actor, string) (context.Context, error)
	// AuthorizeTarget checks destination/credential policy at acceptance and send.
	AuthorizeTarget func(context.Context, export.Actor, string, Target) error
	// Link returns a stable guarded download route; expiring bearer URLs cannot
	// be refreshed without changing the immutable delivery body.
	Link            func(export.ExportRecord) (string, error)
	ExecutionConfig job.Config
	Now             func() time.Time
}
type Service struct{ cfg Config }
type operation struct {
	bun.BaseModel `bun:"table:export_webhook_operations"`
	ID            string `bun:",pk"`
	Signature     string `bun:",notnull"`
}
type targetRecord struct {
	bun.BaseModel `bun:"table:export_webhook_targets"`
	ID            string    `bun:",pk"`
	OperationID   string    `bun:",notnull"`
	ExportID      string    `bun:",notnull"`
	TargetID      string    `bun:",notnull"`
	Namespace     string    `bun:",notnull"`
	Owner         string    `bun:",notnull"`
	URL           string    `bun:",notnull"`
	Headers       string    `bun:",notnull"`
	Body          string    `bun:",notnull"`
	Digest        string    `bun:",notnull"`
	DeliveredAt   time.Time `bun:",nullzero"`
}

func Migrate(ctx context.Context, db *bun.DB) error {
	if db == nil {
		return invalid("webhook database required")
	}
	return db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		for _, m := range []any{(*operation)(nil), (*targetRecord)(nil)} {
			if _, e := tx.NewCreateTable().Model(m).IfNotExists().Exec(ctx); e != nil {
				return e
			}
		}
		_, e := tx.NewCreateIndex().Model((*targetRecord)(nil)).Index("export_webhook_targets_operation_idx").Column("operation_id").IfNotExists().Exec(ctx)
		return e
	})
}
func NewService(cfg Config) (*Service, error) {
	if cfg.DB == nil || cfg.Queue == nil || cfg.Queue.Database() != cfg.DB.DB || cfg.Sender == nil || cfg.Namespace == nil || cfg.WithAuthorizedTransaction == nil || cfg.Authorize == nil || cfg.AuthorizeTarget == nil || cfg.Link == nil {
		return nil, invalid("durable webhooks require one database, queue, sender and trusted authorization/link policies")
	}
	if cfg.Now == nil {
		cfg.Now = func() time.Time { return time.Now().UTC() }
	}
	return &Service{cfg: cfg}, nil
}
func invalid(s string) error { return export.NewError(export.KindValidation, s, nil) }
func denied() error          { return export.NewError(export.KindAuthz, "webhook delivery not authorized", nil) }
func digestJSON(v any) (string, error) {
	b, e := json.Marshal(v)
	if e != nil {
		return "", invalid("webhook request cannot be persisted")
	}
	return delivery.PayloadDigest(b), nil
}
func owner(a export.Actor) string {
	h, _ := digestJSON(struct {
		ID    string
		Scope export.Scope
	}{a.ID, a.Scope})
	return h
}
func (s *Service) namespace(ctx context.Context, a export.Actor) (string, error) {
	if a.ID == "" {
		return "", denied()
	}
	ns, e := s.cfg.Namespace(ctx, a)
	if e != nil || ns == "" || len(ns) > 512 {
		return "", denied()
	}
	return ns, nil
}
func receipt(r targetRecord) delivery.WebhookReceipt {
	return delivery.WebhookReceipt{ID: r.ID, ExportID: r.ExportID, TargetID: r.TargetID, DeliveredAt: r.DeliveredAt}
}

func prepareSchedule(r ScheduleRequest) (ScheduleRequest, error) {
	if r.ExportID == "" || strings.TrimSpace(r.Key) == "" || len(r.Key) > 512 || len(r.Targets) == 0 || len(r.Targets) > delivery.DefaultMaxTargets {
		return ScheduleRequest{}, invalid("webhook export, key and bounded targets required")
	}
	// Round-trip input before entering the callback: callers retain ownership of maps/slices.
	b, e := json.Marshal(r)
	if e != nil {
		return ScheduleRequest{}, invalid("webhook request cannot be persisted")
	}
	if len(b) > 1<<20 {
		return ScheduleRequest{}, invalid("webhook request too large")
	}
	var req ScheduleRequest
	decoder := json.NewDecoder(bytes.NewReader(b))
	decoder.UseNumber()
	if e = decoder.Decode(&req); e != nil {
		return ScheduleRequest{}, e
	}
	sort.Slice(req.Targets, func(i, j int) bool { return req.Targets[i].ID < req.Targets[j].ID })
	for i, t := range req.Targets {
		if strings.TrimSpace(t.ID) == "" || len(t.ID) > 512 || (i > 0 && req.Targets[i-1].ID == t.ID) {
			return ScheduleRequest{}, invalid("unique webhook target identities required")
		}
		// The final body/key are validated too, after construction.
		if e = delivery.ValidateWebhookDelivery(delivery.WebhookDelivery{ID: strings.Repeat("0", 64), URL: t.URL, Headers: t.Headers, Body: json.RawMessage(`{}`), SHA256: delivery.PayloadDigest([]byte(`{}`))}); e != nil {
			return ScheduleRequest{}, e
		}
	}
	return req, nil
}

// Schedule atomically accepts the entire target set and its queue messages. The
// same scoped key/export/actor must replay the same intent; target order is free.
// The export is already completed; automatic generation-to-notification recovery
// is a separate host workflow. Keys/receipts have no implicit expiration.
func (s *Service) Schedule(ctx context.Context, a export.Actor, r ScheduleRequest) ([]delivery.WebhookReceipt, error) {
	if s == nil {
		return nil, invalid("webhook service required")
	}
	if a.ID == "" {
		return nil, denied()
	}
	req, e := prepareSchedule(r)
	if e != nil {
		return nil, e
	}
	sig, e := digestJSON(req)
	if e != nil {
		return nil, e
	}
	var receipts []delivery.WebhookReceipt
	called := false
	e = s.cfg.WithAuthorizedTransaction(ctx, a, req.ExportID, func(ctx context.Context, tx bun.Tx, rec export.ExportRecord) error {
		if called {
			return invalid("webhook transaction callback repeated")
		}
		called = true
		if rec.ID != req.ExportID || rec.State != export.StateCompleted {
			return invalid("completed export required for webhook delivery")
		}
		ns, e := s.namespace(ctx, a)
		if e != nil {
			return e
		}
		for _, target := range req.Targets {
			if e = s.cfg.AuthorizeTarget(ctx, a, req.ExportID, target); e != nil {
				return e
			}
		}
		opID, e := digestJSON([]string{"export.webhook.v1", ns, owner(a), req.ExportID, req.Key})
		if e != nil {
			return e
		}
		op := operation{ID: opID, Signature: sig}
		res, e := tx.NewInsert().Model(&op).On("CONFLICT (id) DO NOTHING").Exec(ctx)
		if e != nil {
			return e
		}
		n, e := res.RowsAffected()
		if e != nil {
			return e
		}
		q := tx.NewSelect().Model(&op).WherePK()
		if s.cfg.DB.Dialect().Name() == dialect.PG {
			q = q.For("UPDATE")
		}
		if e = q.Scan(ctx); e != nil {
			return e
		}
		if op.Signature != sig {
			return invalid("webhook idempotency conflict")
		}
		if n == 0 {
			var rows []targetRecord
			if e = tx.NewSelect().Model(&rows).Where("operation_id=?", op.ID).Order("target_id").Scan(ctx); e != nil {
				return e
			}
			if len(rows) != len(req.Targets) {
				return invalid("webhook receipt set incomplete")
			}
			for _, row := range rows {
				receipts = append(receipts, receipt(row))
			}
			return nil
		}
		link, e := s.cfg.Link(rec)
		if e != nil {
			return e
		}
		if strings.TrimSpace(link) == "" {
			return invalid("guarded webhook download link required")
		}
		at := s.cfg.Now()
		payload, e := json.Marshal(delivery.WebhookPayload{ExportID: rec.ID, Definition: rec.Definition, Format: rec.Format, Filename: rec.Artifact.Meta.Filename, Mode: delivery.DeliveryLink, Link: link, Metadata: req.Metadata, Actor: export.Actor{ID: a.ID, Scope: a.Scope}, SentAt: at})
		if e != nil {
			return invalid("webhook body cannot be persisted")
		}
		if len(payload) > 1<<20 || at.IsZero() {
			return invalid("invalid webhook body size or clock")
		}
		for _, target := range req.Targets {
			id, e := digestJSON([]string{op.ID, target.ID})
			if e != nil {
				return e
			}
			headers, e := json.Marshal(target.Headers)
			if e != nil {
				return e
			}
			row := targetRecord{ID: id, OperationID: op.ID, ExportID: rec.ID, TargetID: target.ID, Namespace: ns, Owner: owner(a), URL: target.URL, Headers: string(headers), Body: string(payload), Digest: delivery.PayloadDigest(payload)}
			if _, e = tx.NewInsert().Model(&row).Exec(ctx); e != nil {
				return e
			}
			m, e := json.Marshal(cmd.DeliverWebhook{Actor: export.Actor{ID: a.ID, Scope: a.Scope}, ExportID: rec.ID, ReceiptID: id})
			if e != nil {
				return e
			}
			if _, e = s.cfg.Queue.EnqueueTx(ctx, tx.Tx, &job.ExecutionMessage{JobID: exportjob.DefaultWebhookTaskID, ScriptPath: exportjob.DefaultWebhookTaskID, Config: s.cfg.ExecutionConfig, IdempotencyKey: id, Parameters: map[string]any{"payload": json.RawMessage(m)}}, at); e != nil {
				return e
			}
			receipts = append(receipts, receipt(row))
		}
		return nil
	})
	if e != nil {
		return nil, e
	}
	if !called {
		return nil, invalid("webhook transaction callback not invoked")
	}
	return receipts, nil
}
func (s *Service) load(ctx context.Context, a export.Actor, exportID, id string) (context.Context, targetRecord, error) {
	if a.ID == "" || exportID == "" || id == "" {
		return ctx, targetRecord{}, denied()
	}
	ctx, e := s.cfg.Authorize(ctx, a, exportID)
	if e != nil {
		return ctx, targetRecord{}, e
	}
	if ctx == nil {
		return ctx, targetRecord{}, denied()
	}
	ns, e := s.namespace(ctx, a)
	if e != nil {
		return ctx, targetRecord{}, e
	}
	r := targetRecord{ID: id}
	e = s.cfg.DB.NewSelect().Model(&r).Where("id=? AND export_id=? AND namespace=? AND owner=?", id, exportID, ns, owner(a)).Scan(ctx)
	if e != nil {
		if !errors.Is(e, sql.ErrNoRows) {
			return ctx, targetRecord{}, export.NewError(export.KindInternal, "webhook receipt lookup failed", nil)
		}
		return ctx, targetRecord{}, export.NewError(export.KindNotFound, "webhook receipt unavailable", nil)
	}
	return ctx, r, nil
}
func (s *Service) Status(ctx context.Context, a export.Actor, exportID, id string) (delivery.WebhookReceipt, error) {
	if s == nil {
		return delivery.WebhookReceipt{}, invalid("webhook service required")
	}
	_, r, e := s.load(ctx, a, exportID, id)
	if e != nil {
		return delivery.WebhookReceipt{}, e
	}
	return receipt(r), nil
}

// DeliverTarget invokes the provider outside SQL transactions. Concurrent attempts
// can send duplicate requests, so receiver idempotency is mandatory. A successful
// receipt is durable before the native worker acknowledges its queue lease.
func (s *Service) DeliverTarget(ctx context.Context, a export.Actor, exportID, id string) error {
	if s == nil {
		return invalid("webhook service required")
	}
	ctx, r, e := s.load(ctx, a, exportID, id)
	if e != nil {
		return e
	}
	if !r.DeliveredAt.IsZero() {
		return nil
	}
	var headers map[string]string
	if e = json.Unmarshal([]byte(r.Headers), &headers); e != nil {
		return invalid("stored webhook headers invalid")
	}
	if e = s.cfg.AuthorizeTarget(ctx, a, exportID, Target{ID: r.TargetID, URL: r.URL, Headers: headers}); e != nil {
		return e
	}
	m := delivery.WebhookDelivery{ID: r.ID, URL: r.URL, Headers: headers, Body: json.RawMessage(r.Body), SHA256: r.Digest}
	if e = delivery.ValidateWebhookDelivery(m); e != nil {
		return e
	}
	if e = s.cfg.Sender.SendReceipt(ctx, m); e != nil {
		return e
	}
	at := s.cfg.Now()
	if at.IsZero() {
		return export.NewError(export.KindInternal, "invalid webhook receipt clock", nil)
	}
	result, e := s.cfg.DB.NewUpdate().Model((*targetRecord)(nil)).Set("delivered_at=?", at).Where("id=? AND delivered_at IS NULL", r.ID).Exec(ctx)
	if e != nil {
		return e
	}
	n, e := result.RowsAffected()
	if e != nil {
		return e
	}
	if n == 0 {
		var committed targetRecord
		if e = s.cfg.DB.NewSelect().Model(&committed).Where("id=?", r.ID).Scan(ctx); e != nil || committed.DeliveredAt.IsZero() {
			return export.NewError(export.KindInternal, "webhook receipt not committed", nil)
		}
	}
	return nil
}

var _ cmd.WebhookDeliveryService = (*Service)(nil)
