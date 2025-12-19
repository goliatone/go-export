package trackerbun

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/goliatone/go-export/export"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/sqlitedialect"
	"github.com/uptrace/bun/driver/sqliteshim"
)

func TestTracker_StartStatusList(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tracker := NewTracker(db)

	recordID, err := tracker.Start(ctx, export.ExportRecord{
		Definition: "users",
		Format:     export.FormatCSV,
		State:      export.StateQueued,
		RequestedBy: export.Actor{
			ID:    "user-1",
			Roles: []string{"admin"},
			Scope: export.Scope{TenantID: "t1", WorkspaceID: "w1"},
		},
		Scope: export.Scope{TenantID: "t1", WorkspaceID: "w1"},
		Counts: export.ExportCounts{
			Processed: 1,
			Total:     2,
		},
		Artifact: export.ArtifactRef{
			Key: "exports/1.csv",
			Meta: export.ArtifactMeta{
				Filename:    "users.csv",
				ContentType: "text/csv",
			},
		},
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if recordID == "" {
		t.Fatalf("expected record id")
	}

	got, err := tracker.Status(ctx, recordID)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if got.Definition != "users" {
		t.Fatalf("expected definition, got %q", got.Definition)
	}
	if got.RequestedBy.ID != "user-1" {
		t.Fatalf("expected actor, got %q", got.RequestedBy.ID)
	}

	list, err := tracker.List(ctx, export.ProgressFilter{Definition: "users"})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("expected 1 record, got %d", len(list))
	}
}

func TestTracker_StateTransitions(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tracker := NewTracker(db)

	recordID, err := tracker.Start(ctx, export.ExportRecord{
		ID:         "exp-1",
		Definition: "orders",
		Format:     export.FormatJSON,
		State:      export.StateQueued,
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	if err := tracker.Advance(ctx, recordID, export.ProgressDelta{Rows: 3, Bytes: 100}, nil); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if err := tracker.SetState(ctx, recordID, export.StateRunning, nil); err != nil {
		t.Fatalf("set state: %v", err)
	}
	if err := tracker.Complete(ctx, recordID, map[string]any{"rows": int64(3), "bytes": int64(100)}); err != nil {
		t.Fatalf("complete: %v", err)
	}

	got, err := tracker.Status(ctx, recordID)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if got.State != export.StateCompleted {
		t.Fatalf("expected completed state, got %s", got.State)
	}
	if got.BytesWritten != 100 {
		t.Fatalf("expected bytes written, got %d", got.BytesWritten)
	}
}

func TestTracker_ArtifactUpdateDelete(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tracker := NewTracker(db)

	recordID, err := tracker.Start(ctx, export.ExportRecord{
		ID:         "exp-2",
		Definition: "reports",
		Format:     export.FormatCSV,
		State:      export.StateQueued,
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	if err := tracker.SetArtifact(ctx, recordID, export.ArtifactRef{
		Key: "exports/exp-2.csv",
		Meta: export.ArtifactMeta{
			Filename:    "reports.csv",
			ContentType: "text/csv",
			ExpiresAt:   time.Now().Add(time.Hour),
		},
	}); err != nil {
		t.Fatalf("set artifact: %v", err)
	}

	got, err := tracker.Status(ctx, recordID)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if got.Artifact.Key != "exports/exp-2.csv" {
		t.Fatalf("expected artifact key, got %q", got.Artifact.Key)
	}

	got.State = export.StateDeleted
	if err := tracker.Update(ctx, got); err != nil {
		t.Fatalf("update: %v", err)
	}

	if err := tracker.Delete(ctx, recordID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := tracker.Status(ctx, recordID); err == nil {
		t.Fatalf("expected not found after delete")
	}
}

func newTestDB(t *testing.T) *bun.DB {
	t.Helper()
	sqldb, err := sql.Open(sqliteshim.ShimName, "file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	db := bun.NewDB(sqldb, sqlitedialect.New())
	t.Cleanup(func() {
		_ = db.Close()
	})

	if _, err := db.NewCreateTable().Model((*recordModel)(nil)).IfNotExists().Exec(context.Background()); err != nil {
		t.Fatalf("create table: %v", err)
	}
	return db
}
