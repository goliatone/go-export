package trackerbun

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/goliatone/go-export/export"
	"github.com/uptrace/bun"
)

// Tracker stores export progress in a Bun-backed database.
type Tracker struct {
	DB          *bun.DB
	Now         func() time.Time
	IDGenerator func() string
}

// NewTracker creates a Bun-backed tracker.
func NewTracker(db *bun.DB) *Tracker {
	return &Tracker{DB: db, Now: time.Now, IDGenerator: defaultIDGenerator()}
}

// Start creates a new export record.
func (t *Tracker) Start(ctx context.Context, record export.ExportRecord) (string, error) {
	if t == nil || t.DB == nil {
		return "", export.NewError(export.KindNotImpl, "tracker database not configured", nil)
	}
	if record.ID == "" {
		record.ID = t.nextID()
	}
	if record.State == "" {
		record.State = export.StateQueued
	}
	if record.CreatedAt.IsZero() {
		record.CreatedAt = t.now()
	}

	model, err := modelFromRecord(record)
	if err != nil {
		return "", err
	}
	_, err = t.DB.NewInsert().Model(&model).Exec(ctx)
	if err != nil {
		return "", err
	}
	return record.ID, nil
}

// Advance updates counts for an export.
func (t *Tracker) Advance(ctx context.Context, id string, delta export.ProgressDelta, meta map[string]any) error {
	_ = meta
	if t == nil || t.DB == nil {
		return export.NewError(export.KindNotImpl, "tracker database not configured", nil)
	}
	if id == "" {
		return export.NewError(export.KindValidation, "export ID is required", nil)
	}

	query := t.DB.NewUpdate().Model((*recordModel)(nil)).
		Set("counts_processed = counts_processed + ?", delta.Rows).
		Set("bytes_written = bytes_written + ?", delta.Bytes).
		Where("id = ?", id)
	res, err := query.Exec(ctx)
	if err != nil {
		return err
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return export.NewError(export.KindNotFound, fmt.Sprintf("export %q not found", id), nil)
	}
	return nil
}

// SetState updates the export state.
func (t *Tracker) SetState(ctx context.Context, id string, state export.ExportState, meta map[string]any) error {
	_ = meta
	if t == nil || t.DB == nil {
		return export.NewError(export.KindNotImpl, "tracker database not configured", nil)
	}
	if id == "" {
		return export.NewError(export.KindValidation, "export ID is required", nil)
	}

	query := t.DB.NewUpdate().Model((*recordModel)(nil)).
		Set("state = ?", state).
		Where("id = ?", id)
	if state == export.StateRunning {
		query = query.Set("started_at = COALESCE(started_at, ?)", t.now())
	}
	if state == export.StateCompleted {
		query = query.Set("completed_at = COALESCE(completed_at, ?)", t.now())
	}

	res, err := query.Exec(ctx)
	if err != nil {
		return err
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return export.NewError(export.KindNotFound, fmt.Sprintf("export %q not found", id), nil)
	}
	return nil
}

// Fail marks the export as failed.
func (t *Tracker) Fail(ctx context.Context, id string, err error, meta map[string]any) error {
	_ = err
	_ = meta
	if t == nil || t.DB == nil {
		return export.NewError(export.KindNotImpl, "tracker database not configured", nil)
	}
	if id == "" {
		return export.NewError(export.KindValidation, "export ID is required", nil)
	}

	res, err := t.DB.NewUpdate().Model((*recordModel)(nil)).
		Set("state = ?", export.StateFailed).
		Set("completed_at = COALESCE(completed_at, ?)", t.now()).
		Where("id = ?", id).
		Exec(ctx)
	if err != nil {
		return err
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return export.NewError(export.KindNotFound, fmt.Sprintf("export %q not found", id), nil)
	}
	return nil
}

// Complete marks the export as completed.
func (t *Tracker) Complete(ctx context.Context, id string, meta map[string]any) error {
	if t == nil || t.DB == nil {
		return export.NewError(export.KindNotImpl, "tracker database not configured", nil)
	}
	if id == "" {
		return export.NewError(export.KindValidation, "export ID is required", nil)
	}

	query := t.DB.NewUpdate().Model((*recordModel)(nil)).
		Set("state = ?", export.StateCompleted).
		Set("completed_at = COALESCE(completed_at, ?)", t.now()).
		Where("id = ?", id)
	if rows, ok := meta["rows"].(int64); ok {
		query = query.Set("counts_processed = ?", rows)
	}
	if bytes, ok := meta["bytes"].(int64); ok {
		query = query.Set("bytes_written = ?", bytes)
	}

	res, err := query.Exec(ctx)
	if err != nil {
		return err
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return export.NewError(export.KindNotFound, fmt.Sprintf("export %q not found", id), nil)
	}
	return nil
}

// Status returns a record by ID.
func (t *Tracker) Status(ctx context.Context, id string) (export.ExportRecord, error) {
	if t == nil || t.DB == nil {
		return export.ExportRecord{}, export.NewError(export.KindNotImpl, "tracker database not configured", nil)
	}
	if id == "" {
		return export.ExportRecord{}, export.NewError(export.KindValidation, "export ID is required", nil)
	}

	model := new(recordModel)
	err := t.DB.NewSelect().Model(model).Where("id = ?", id).Limit(1).Scan(ctx)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return export.ExportRecord{}, export.NewError(export.KindNotFound, fmt.Sprintf("export %q not found", id), nil)
		}
		return export.ExportRecord{}, err
	}
	return model.toRecord()
}

// List returns records matching a filter.
func (t *Tracker) List(ctx context.Context, filter export.ProgressFilter) ([]export.ExportRecord, error) {
	if t == nil || t.DB == nil {
		return nil, export.NewError(export.KindNotImpl, "tracker database not configured", nil)
	}

	models := make([]recordModel, 0)
	query := t.DB.NewSelect().Model(&models)
	if filter.Definition != "" {
		query = query.Where("definition = ?", filter.Definition)
	}
	if filter.State != "" {
		query = query.Where("state = ?", filter.State)
	}
	if !filter.Since.IsZero() {
		query = query.Where("created_at >= ?", filter.Since)
	}
	if !filter.Until.IsZero() {
		query = query.Where("created_at <= ?", filter.Until)
	}
	query = query.Order("created_at DESC")

	if err := query.Scan(ctx); err != nil {
		return nil, err
	}

	records := make([]export.ExportRecord, 0, len(models))
	for _, model := range models {
		record, err := model.toRecord()
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, nil
}

// SetArtifact updates the artifact metadata for a record.
func (t *Tracker) SetArtifact(ctx context.Context, id string, ref export.ArtifactRef) error {
	if t == nil || t.DB == nil {
		return export.NewError(export.KindNotImpl, "tracker database not configured", nil)
	}
	if id == "" {
		return export.NewError(export.KindValidation, "export ID is required", nil)
	}

	meta, err := json.Marshal(ref.Meta)
	if err != nil {
		return err
	}
	query := t.DB.NewUpdate().Model((*recordModel)(nil)).
		Set("artifact_key = ?", ref.Key).
		Set("artifact_meta = ?", meta).
		Where("id = ?", id)
	if !ref.Meta.ExpiresAt.IsZero() {
		query = query.Set("expires_at = ?", ref.Meta.ExpiresAt)
	}
	res, err := query.Exec(ctx)
	if err != nil {
		return err
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return export.NewError(export.KindNotFound, fmt.Sprintf("export %q not found", id), nil)
	}
	return nil
}

// Update replaces an export record.
func (t *Tracker) Update(ctx context.Context, record export.ExportRecord) error {
	if t == nil || t.DB == nil {
		return export.NewError(export.KindNotImpl, "tracker database not configured", nil)
	}
	if record.ID == "" {
		return export.NewError(export.KindValidation, "export ID is required", nil)
	}

	model, err := modelFromRecord(record)
	if err != nil {
		return err
	}

	res, err := t.DB.NewUpdate().Model(&model).Where("id = ?", record.ID).Exec(ctx)
	if err != nil {
		return err
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return export.NewError(export.KindNotFound, fmt.Sprintf("export %q not found", record.ID), nil)
	}
	return nil
}

// Delete removes a record from the tracker.
func (t *Tracker) Delete(ctx context.Context, id string) error {
	if t == nil || t.DB == nil {
		return export.NewError(export.KindNotImpl, "tracker database not configured", nil)
	}
	if id == "" {
		return export.NewError(export.KindValidation, "export ID is required", nil)
	}

	res, err := t.DB.NewDelete().Model((*recordModel)(nil)).Where("id = ?", id).Exec(ctx)
	if err != nil {
		return err
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return export.NewError(export.KindNotFound, fmt.Sprintf("export %q not found", id), nil)
	}
	return nil
}

type recordModel struct {
	bun.BaseModel `bun:"table:export_records,alias:export_records"`

	ID                     string    `bun:",pk"`
	Definition             string    `bun:",notnull"`
	Format                 string    `bun:",notnull"`
	State                  string    `bun:",notnull"`
	RequestedByID          string    `bun:"requested_by_id"`
	RequestedByTenantID    string    `bun:"requested_by_tenant_id"`
	RequestedByWorkspaceID string    `bun:"requested_by_workspace_id"`
	RequestedByRoles       []byte    `bun:"requested_by_roles"`
	RequestedByDetails     []byte    `bun:"requested_by_details"`
	ScopeTenantID          string    `bun:"scope_tenant_id"`
	ScopeWorkspaceID       string    `bun:"scope_workspace_id"`
	CountsProcessed        int64     `bun:"counts_processed"`
	CountsTotal            int64     `bun:"counts_total"`
	CountsErrors           int64     `bun:"counts_errors"`
	BytesWritten           int64     `bun:"bytes_written"`
	ArtifactKey            string    `bun:"artifact_key"`
	ArtifactMeta           []byte    `bun:"artifact_meta"`
	CreatedAt              time.Time `bun:"created_at"`
	StartedAt              time.Time `bun:"started_at,nullzero"`
	CompletedAt            time.Time `bun:"completed_at,nullzero"`
	ExpiresAt              time.Time `bun:"expires_at,nullzero"`
}

func modelFromRecord(record export.ExportRecord) (recordModel, error) {
	roles, err := json.Marshal(record.RequestedBy.Roles)
	if err != nil {
		return recordModel{}, err
	}
	details, err := json.Marshal(record.RequestedBy.Details)
	if err != nil {
		return recordModel{}, err
	}
	meta, err := json.Marshal(record.Artifact.Meta)
	if err != nil {
		return recordModel{}, err
	}

	return recordModel{
		ID:                     record.ID,
		Definition:             record.Definition,
		Format:                 string(record.Format),
		State:                  string(record.State),
		RequestedByID:          record.RequestedBy.ID,
		RequestedByTenantID:    record.RequestedBy.Scope.TenantID,
		RequestedByWorkspaceID: record.RequestedBy.Scope.WorkspaceID,
		RequestedByRoles:       roles,
		RequestedByDetails:     details,
		ScopeTenantID:          record.Scope.TenantID,
		ScopeWorkspaceID:       record.Scope.WorkspaceID,
		CountsProcessed:        record.Counts.Processed,
		CountsTotal:            record.Counts.Total,
		CountsErrors:           record.Counts.Errors,
		BytesWritten:           record.BytesWritten,
		ArtifactKey:            record.Artifact.Key,
		ArtifactMeta:           meta,
		CreatedAt:              record.CreatedAt,
		StartedAt:              record.StartedAt,
		CompletedAt:            record.CompletedAt,
		ExpiresAt:              record.ExpiresAt,
	}, nil
}

func (m recordModel) toRecord() (export.ExportRecord, error) {
	record := export.ExportRecord{
		ID:         m.ID,
		Definition: m.Definition,
		Format:     export.Format(m.Format),
		State:      export.ExportState(m.State),
		RequestedBy: export.Actor{
			ID: m.RequestedByID,
			Scope: export.Scope{
				TenantID:    m.RequestedByTenantID,
				WorkspaceID: m.RequestedByWorkspaceID,
			},
		},
		Scope: export.Scope{
			TenantID:    m.ScopeTenantID,
			WorkspaceID: m.ScopeWorkspaceID,
		},
		Counts: export.ExportCounts{
			Processed: m.CountsProcessed,
			Total:     m.CountsTotal,
			Errors:    m.CountsErrors,
		},
		BytesWritten: m.BytesWritten,
		Artifact: export.ArtifactRef{
			Key: m.ArtifactKey,
		},
		CreatedAt:   m.CreatedAt,
		StartedAt:   m.StartedAt,
		CompletedAt: m.CompletedAt,
		ExpiresAt:   m.ExpiresAt,
	}

	if len(m.RequestedByRoles) > 0 {
		if err := json.Unmarshal(m.RequestedByRoles, &record.RequestedBy.Roles); err != nil {
			return export.ExportRecord{}, err
		}
	}
	if len(m.RequestedByDetails) > 0 {
		if err := json.Unmarshal(m.RequestedByDetails, &record.RequestedBy.Details); err != nil {
			return export.ExportRecord{}, err
		}
	}
	if len(m.ArtifactMeta) > 0 {
		if err := json.Unmarshal(m.ArtifactMeta, &record.Artifact.Meta); err != nil {
			return export.ExportRecord{}, err
		}
	}

	return record, nil
}

func (t *Tracker) now() time.Time {
	if t.Now != nil {
		return t.Now()
	}
	return time.Now()
}

func (t *Tracker) nextID() string {
	if t.IDGenerator != nil {
		return t.IDGenerator()
	}
	return defaultIDGenerator()()
}

func defaultIDGenerator() func() string {
	var counter uint64
	return func() string {
		id := atomic.AddUint64(&counter, 1)
		return fmt.Sprintf("exp-%d", id)
	}
}
