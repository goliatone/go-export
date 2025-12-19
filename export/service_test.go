package export

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"
)

func TestService_DeleteExport_TombstoneStrategy(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2024, 2, 3, 4, 5, 6, 0, time.UTC)

	tracker := NewMemoryTracker()
	store := NewMemoryStore()
	key := "exports/exp-1.csv"

	if _, err := store.Put(ctx, key, bytes.NewBufferString("data"), ArtifactMeta{
		Filename:  "users.csv",
		CreatedAt: now,
	}); err != nil {
		t.Fatalf("store put: %v", err)
	}

	record := ExportRecord{
		ID:         "exp-1",
		Definition: "users",
		Format:     FormatCSV,
		State:      StateCompleted,
		Artifact: ArtifactRef{
			Key: key,
			Meta: ArtifactMeta{
				Filename:  "users.csv",
				CreatedAt: now,
			},
		},
	}
	if _, err := tracker.Start(ctx, record); err != nil {
		t.Fatalf("tracker start: %v", err)
	}

	svc := NewService(ServiceConfig{
		Runner:         NewRunner(),
		Tracker:        tracker,
		Store:          store,
		Now:            func() time.Time { return now },
		DeleteStrategy: TombstoneDeleteStrategy{TTL: time.Hour},
	})

	if err := svc.DeleteExport(ctx, Actor{ID: "actor-1"}, "exp-1"); err != nil {
		t.Fatalf("delete export: %v", err)
	}

	updated, err := tracker.Status(ctx, "exp-1")
	if err != nil {
		t.Fatalf("tracker status: %v", err)
	}
	if updated.State != StateDeleted {
		t.Fatalf("expected deleted state, got %s", updated.State)
	}
	wantExpiry := now.Add(time.Hour)
	if !updated.ExpiresAt.Equal(wantExpiry) {
		t.Fatalf("expected expires at %v, got %v", wantExpiry, updated.ExpiresAt)
	}
	if !updated.Artifact.Meta.ExpiresAt.Equal(wantExpiry) {
		t.Fatalf("expected artifact expires at %v, got %v", wantExpiry, updated.Artifact.Meta.ExpiresAt)
	}

	_, _, err = store.Open(ctx, key)
	if err == nil {
		t.Fatalf("expected artifact deletion")
	}
	var exportErr *ExportError
	if !errors.As(err, &exportErr) || exportErr.Kind != KindNotFound {
		t.Fatalf("expected not found error, got %v", err)
	}
}
