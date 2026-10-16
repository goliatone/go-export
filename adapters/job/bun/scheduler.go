// Package jobbun composes transactional export scheduling with the native SQL queue.
package jobbun

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	exportjob "github.com/goliatone/go-export/adapters/job"
	replaybun "github.com/goliatone/go-export/adapters/replay/bun"
	"github.com/goliatone/go-export/export"
	job "github.com/goliatone/go-job"
	"github.com/goliatone/go-job/queue"
	"github.com/uptrace/bun"
)

// TransactionalQueue accepts messages in the caller's transaction. Database
// binds queue acceptance to the replay tracker database, preventing split writes.
// The current go-job SQL storage implements this contract additively.
type TransactionalQueue interface {
	Database() *sql.DB
	EnqueueTx(context.Context, *sql.Tx, *job.ExecutionMessage, time.Time) (queue.EnqueueReceipt, error)
}

// Config selects a replay service and transaction-aware queue on the same
// database. Migrate replay/tracker and queue schemas before serving requests.
type Config struct {
	Replay          replaybun.Config
	Queue           TransactionalQueue
	TaskID          string
	TaskPath        string
	ExecutionConfig job.Config
	// AvailableAt optionally schedules a new record. Replays retain its first time.
	AvailableAt func(context.Context, export.ExportRecord) (time.Time, error)
}

// Scheduler exposes the native service/typed-command contract and its durable
// queue. HTTP and commands must use this service directly, without the legacy
// enqueue-after-request scheduler or a second process-local replay cache.
type Scheduler struct {
	*replaybun.Service
	Queue TransactionalQueue
}

func NewScheduler(cfg Config) (*Scheduler, error) {
	if cfg.Replay.Tracker == nil || cfg.Replay.Tracker.DB == nil || cfg.Replay.Enqueue != nil {
		return nil, export.NewError(export.KindValidation, "scheduler requires SQL tracker and exclusive enqueue hook", nil)
	}
	if cfg.Queue == nil || cfg.Queue.Database() != cfg.Replay.Tracker.DB.DB {
		return nil, export.NewError(export.KindValidation, "queue and replay require the same SQL database", nil)
	}
	q := cfg.Queue
	id, path := cfg.TaskID, cfg.TaskPath
	if id == "" {
		id = exportjob.DefaultGenerateTaskID
	}
	if path == "" {
		path = exportjob.DefaultGenerateTaskPath
	}
	cfg.Replay.Enqueue = func(ctx context.Context, tx bun.Tx, a export.Actor, r export.ExportRecord) error {
		if r.Request.Definition == "" {
			return export.NewError(export.KindValidation, "queued request missing", nil)
		}
		payload, err := json.Marshal(exportjob.Payload{ExportID: r.ID, Actor: a, Request: r.Request})
		if err != nil {
			return err
		}
		at := time.Now().UTC()
		if cfg.AvailableAt != nil {
			at, err = cfg.AvailableAt(ctx, r)
			if err != nil {
				return err
			}
		}
		_, err = q.EnqueueTx(ctx, tx.Tx, &job.ExecutionMessage{JobID: id, ScriptPath: path, Config: cfg.ExecutionConfig, IdempotencyKey: r.ID, Parameters: map[string]any{"payload": json.RawMessage(payload)}}, at)
		return err
	}
	svc, err := replaybun.NewService(cfg.Replay)
	if err != nil {
		return nil, err
	}
	return &Scheduler{Service: svc, Queue: q}, nil
}

// RequestExport forces async semantics and commits record/replay/message together.
func (s *Scheduler) RequestExport(ctx context.Context, a export.Actor, r export.ExportRequest) (export.ExportRecord, error) {
	r.Delivery = export.DeliveryAsync
	r.Output = nil
	return s.Service.RequestExport(ctx, a, r)
}

var _ export.Service = (*Scheduler)(nil)
