// Package replaybun coordinates durable replay and generation with Bun tracking.
package replaybun

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"time"

	trackerbun "github.com/goliatone/go-export/adapters/tracker/bun"
	"github.com/goliatone/go-export/export"
	"github.com/google/uuid"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

// Config supplies a native service and its SQL tracker. Namespace must derive
// from trusted application scope, independently of tenant/workspace conventions.
// WithTransaction may supply an authorized transaction (e.g. a native Group
// fence). Guards and row sources must reuse that transaction or the fence proof;
// starting another write transaction inside it is unsupported.
type Config struct {
	Service         export.ServiceConfig
	Tracker         *trackerbun.Tracker
	Namespace       func(context.Context, export.Actor) (string, error)
	TTL             time.Duration
	WithTransaction func(context.Context, export.Actor, func(context.Context, bun.Tx) error) error
	// Enqueue persists a new async record's executable delivery in this same
	// transaction. It must not perform external I/O or commit the transaction.
	// Replays do not enqueue again; any error rolls back record and replay key.
	Enqueue func(context.Context, bun.Tx, export.Actor, export.ExportRecord) error
	// OnQueued captures transaction-local work for a new async record before
	// Enqueue. Replayed submissions do not invoke it. Errors roll back acceptance.
	OnQueued func(context.Context, bun.Tx, export.Actor, export.ExportRecord) error
	// OnCompleted publishes transaction-local work after durable artifact and
	// completed tracking, before commit. Completed generation replay skips it.
	// Hooks must use this same transaction, never commit or perform network I/O.
	OnCompleted func(context.Context, bun.Tx, export.Actor, export.ExportRecord) error
}

// Service retains the ordinary service API; only async replay and generation
// need transactional coordination. The SQL record is the durable queue; an
// external scheduler and external effects require a separate delivery contract.
type Service struct {
	export.Service
	cfg Config
}

type reservation struct {
	bun.BaseModel `bun:"table:export_replay_keys"`
	Namespace     string    `bun:",pk"`
	Signature     string    `bun:",pk"`
	ExportID      string    `bun:",notnull"`
	ExpiresAt     time.Time `bun:",notnull"`
}

// Migrate creates only the additive replay-key table. Native tracker migrations
// remain application-owned, as with the ordinary Bun tracker.
func Migrate(ctx context.Context, db *bun.DB) error {
	if db == nil {
		return export.NewError(export.KindValidation, "replay database is required", nil)
	}
	_, e := db.NewCreateTable().Model((*reservation)(nil)).IfNotExists().Exec(ctx)
	return e
}
func NewService(cfg Config) (*Service, error) {
	if cfg.Tracker == nil || cfg.Tracker.DB == nil || cfg.Service.Runner == nil || cfg.Service.Store == nil || cfg.Namespace == nil {
		return nil, export.NewError(export.KindValidation, "durable replay requires database, runner, store and namespace", nil)
	}
	if cfg.TTL < 0 {
		return nil, export.NewError(export.KindValidation, "replay TTL cannot be negative", nil)
	}
	if cfg.TTL == 0 {
		cfg.TTL = 24 * time.Hour
	}
	if cfg.Service.Tracker == nil {
		cfg.Service.Tracker = cfg.Tracker
	}
	return &Service{Service: export.NewService(cfg.Service), cfg: cfg}, nil
}
func (s *Service) now() time.Time {
	if s.cfg.Service.Now != nil {
		return s.cfg.Service.Now()
	}
	return time.Now().UTC()
}
func (s *Service) transact(ctx context.Context, a export.Actor, fn func(context.Context, bun.Tx) error) error {
	if s.cfg.WithTransaction != nil {
		return s.cfg.WithTransaction(ctx, a, fn)
	}
	return s.cfg.Tracker.DB.RunInTx(ctx, nil, fn)
}
func (s *Service) preflight(ctx context.Context, a export.Actor, req export.ExportRequest) (export.ResolvedExport, error) {
	d, e := s.cfg.Service.Runner.Definitions.Resolve(req)
	if e != nil {
		return export.ResolvedExport{}, e
	}
	r, e := export.ResolveExport(req, d, s.now())
	if e != nil {
		return r, e
	}
	for _, g := range []export.Guard{s.cfg.Service.Guard, s.cfg.Service.Runner.Guard} {
		if g != nil {
			if e = g.AuthorizeExport(ctx, a, r.Request, r.Definition); e != nil {
				return r, export.NewError(export.KindAuthz, "export not authorized", e)
			}
		}
	}
	return r, nil
}
func replaySignature(a export.Actor, req export.ExportRequest) (string, error) {
	key := req.IdempotencyKey
	req.IdempotencyKey = ""
	req.Output = nil
	req.Delivery = export.DeliveryAsync
	req.EstimatedRows = 0
	req.EstimatedBytes = 0
	req.EstimatedDuration = 0
	// This timestamp is output metadata resolved anew by the HTTP controller;
	// retries retain the first queued request's timestamp and filename.
	req.RenderOptions.Template.GeneratedAt = time.Time{}
	req.Selection.IDs = append([]string(nil), req.Selection.IDs...)
	sort.Strings(req.Selection.IDs)
	b, e := json.Marshal(struct {
		Key, Actor string
		Scope      export.Scope
		Request    export.ExportRequest
	}{key, a.ID, a.Scope, req})
	if e != nil {
		return "", export.NewError(export.KindValidation, "request cannot be persisted", e)
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:]), nil
}

type eventBuffer struct{ events []export.ChangeEvent }

func (b *eventBuffer) Emit(_ context.Context, e export.ChangeEvent) error {
	b.events = append(b.events, e)
	return nil
}
func (s *Service) bound(tx bun.Tx, b *eventBuffer, id string) export.Service {
	cfg := s.cfg.Service
	r := *cfg.Runner
	r.Tracker = &transactionTracker{Tracker: s.cfg.Tracker.WithTx(tx)}
	r.Emitter = b
	cfg.Runner = &r
	cfg.Tracker = r.Tracker
	if id != "" {
		cfg.IDGenerator = func() string { return id }
	}
	return export.NewService(cfg)
}
func (s *Service) emit(ctx context.Context, b *eventBuffer) {
	if e := s.cfg.Service.Runner.Emitter; e != nil {
		for _, event := range b.events {
			if err := e.Emit(ctx, event); err != nil && s.cfg.Service.Runner.Logger != nil {
				s.cfg.Service.Runner.Logger.Error("postcommit export activity failed", "error", err)
			}
		}
	}
}

// RequestExport commits the replay identity and native queued request together.
// A unique SQL key serializes independent processes; a lost response or aborted
// transaction cannot create an acknowledged key pointing at an absent record.
func (s *Service) RequestExport(ctx context.Context, a export.Actor, req export.ExportRequest) (export.ExportRecord, error) {
	r, e := s.preflight(ctx, a, req)
	if e != nil {
		return export.ExportRecord{}, export.AsGoError(e)
	}
	if export.SelectDelivery(r.Request, r.Definition, s.cfg.Service.DeliveryPolicy) != export.DeliveryAsync {
		return s.Service.RequestExport(ctx, a, req)
	}
	var ns, sig string
	if req.IdempotencyKey != "" {
		ns, e = s.cfg.Namespace(ctx, a)
		if e != nil || ns == "" {
			return export.ExportRecord{}, export.AsGoError(export.NewError(export.KindAuthz, "replay scope unavailable", e))
		}
		sig, e = replaySignature(a, r.Request)
		if e != nil {
			return export.ExportRecord{}, export.AsGoError(e)
		}
	}
	var result export.ExportRecord
	b := new(eventBuffer)
	e = s.transact(ctx, a, func(ctx context.Context, tx bun.Tx) error {
		queueNew := func(id string) error {
			canonical := r.Request
			canonical.Delivery = export.DeliveryAsync
			var err error
			result, err = s.bound(tx, b, id).RequestExport(ctx, a, canonical)
			if err == nil && s.cfg.OnQueued != nil {
				err = s.cfg.OnQueued(ctx, tx, a, result)
			}
			if err == nil && s.cfg.Enqueue != nil {
				err = s.cfg.Enqueue(ctx, tx, a, result)
			}
			return err
		}
		if req.IdempotencyKey == "" {
			return queueNew("")
		}
		key := reservation{Namespace: ns, Signature: sig, ExportID: uuid.NewString(), ExpiresAt: s.now().Add(s.cfg.TTL)}
		res, e := tx.NewInsert().Model(&key).On("CONFLICT (namespace, signature) DO NOTHING").Exec(ctx)
		if e != nil {
			return e
		}
		inserted, e := res.RowsAffected()
		if e != nil {
			return e
		}
		q := tx.NewSelect().Model(&key).Where("namespace=? AND signature=?", ns, sig)
		if s.cfg.Tracker.DB.Dialect().Name() == dialect.PG {
			q = q.For("UPDATE")
		}
		if e = q.Scan(ctx); e != nil {
			return e
		}
		if inserted == 0 && !s.now().Before(key.ExpiresAt) {
			key.ExportID = uuid.NewString()
			key.ExpiresAt = s.now().Add(s.cfg.TTL)
			if _, e = tx.NewUpdate().Model(&key).Column("export_id", "expires_at").WherePK().Exec(ctx); e != nil {
				return e
			}
			inserted = 1
		}
		if inserted == 0 {
			result, e = s.bound(tx, b, "").Status(ctx, a, key.ExportID)
			if e != nil {
				return e
			}
			if result.State == export.StateDeleted || result.State == export.StateCanceled {
				return export.NewError(export.KindValidation, "export request is no longer reusable", nil)
			}
			return nil
		}
		return queueNew(key.ExportID)
	})
	if e != nil {
		return export.ExportRecord{}, export.AsGoError(e)
	}
	s.emit(ctx, b)
	return result, nil
}

// GenerateExport serializes generation under the record's SQL lock. An
// acknowledged completed artifact is reused without reading its data source.
// An interrupted transaction stays queued; its provisional file is unavailable
// to DownloadMetadata until an authorized retry commits completed state.
func (s *Service) GenerateExport(ctx context.Context, a export.Actor, id string, req export.ExportRequest) (export.ExportResult, error) {
	if _, e := s.preflight(ctx, a, req); e != nil {
		return export.ExportResult{}, export.AsGoError(e)
	}
	if _, e := s.Service.Status(ctx, a, id); e != nil {
		return export.ExportResult{}, e
	}
	var result export.ExportResult
	b := new(eventBuffer)
	e := s.transact(ctx, a, func(ctx context.Context, tx bun.Tx) error {
		var locked string
		q := tx.NewSelect().Table("export_records").Column("id").Where("id=?", id)
		if s.cfg.Tracker.DB.Dialect().Name() == dialect.PG {
			q = q.For("UPDATE")
		}
		if e := q.Scan(ctx, &locked); e != nil {
			return e
		}
		tr := s.cfg.Tracker.WithTx(tx)
		rec, e := tr.Status(ctx, id)
		if e != nil {
			return e
		}
		if rec.Request.Definition != "" {
			if _, e = s.preflight(ctx, a, rec.Request); e != nil {
				return e
			}
		}
		if rec.State == export.StateCompleted {
			info, e := s.bound(tx, b, "").DownloadMetadata(ctx, a, id)
			if e != nil {
				return e
			}
			result = export.ExportResult{ID: id, Delivery: export.DeliveryAsync, Format: rec.Format, Rows: rec.Counts.Processed, Bytes: rec.BytesWritten, Filename: info.Artifact.Meta.Filename, Artifact: &info.Artifact}
			return nil
		}
		if rec.State == export.StateDeleted || rec.State == export.StateCanceled {
			return export.NewError(export.KindValidation, "export cannot be generated", nil)
		}
		if rec.Request.Definition == "" {
			return export.NewError(export.KindValidation, "persisted export request is required", nil)
		}
		rec.State = export.StateQueued
		rec.Counts = export.ExportCounts{}
		rec.BytesWritten = 0
		rec.StartedAt = time.Time{}
		rec.CompletedAt = time.Time{}
		if e = tr.Update(ctx, rec); e != nil {
			return e
		}
		result, e = s.bound(tx, b, "").GenerateExport(ctx, a, id, rec.Request)
		if e == nil && s.cfg.OnCompleted != nil {
			completed, err := tr.Status(ctx, id)
			if err != nil {
				return err
			}
			if completed.State != export.StateCompleted {
				return export.NewError(export.KindInternal, "completed export required for publication", nil)
			}
			e = s.cfg.OnCompleted(ctx, tx, a, completed)
		}
		return e
	})
	if e != nil {
		return export.ExportResult{}, export.AsGoError(e)
	}
	result.Delivery = export.DeliveryAsync
	s.emit(ctx, b)
	return result, nil
}

var _ export.Service = (*Service)(nil)
