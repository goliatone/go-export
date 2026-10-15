package export

import (
	"context"
	"errors"
	"io"
	"testing"
)

type publicationStore struct {
	*MemoryStore
	tracker   *MemoryTracker
	premature bool
}

func (s *publicationStore) Put(ctx context.Context, k string, r io.Reader, m ArtifactMeta) (ArtifactRef, error) {
	_, e := io.Copy(io.Discard, r)
	if e != nil {
		return ArtifactRef{}, e
	}
	rec, _ := s.tracker.Status(ctx, "publication-id")
	s.premature = rec.State == StateCompleted
	return ArtifactRef{}, errors.New("injected artifact publication failure")
}
func TestGenerationDoesNotCompleteBeforeArtifactPublication(t *testing.T) {
	runner, _, _ := guardFixture(t)
	tr := NewMemoryTracker()
	st := &publicationStore{MemoryStore: NewMemoryStore(), tracker: tr}
	runner.Tracker = tr
	svc := NewService(ServiceConfig{Runner: runner, Tracker: tr, Store: st, IDGenerator: func() string { return "publication-id" }})
	rec, e := svc.RequestExport(context.Background(), Actor{ID: "actor-1"}, ExportRequest{Definition: "resources", Format: FormatCSV, Delivery: DeliveryAsync})
	if e != nil {
		t.Fatal(e)
	}
	_, e = svc.GenerateExport(context.Background(), Actor{ID: "actor-1"}, rec.ID, ExportRequest{Definition: "resources", Format: FormatCSV})
	if e == nil {
		t.Fatal("publication failure ignored")
	}
	if st.premature {
		t.Fatal("completed record visible before artifact publication")
	}
}

type progressOnlyTracker struct{ ProgressTracker }

type artifactFailureTracker struct{ *MemoryTracker }

func (t artifactFailureTracker) SetArtifact(context.Context, string, ArtifactRef) error {
	return errors.New("injected artifact metadata persistence failure")
}
func TestGenerationReturnsArtifactMetadataFailure(t *testing.T) {
	runner, _, _ := guardFixture(t)
	tracker := artifactFailureTracker{NewMemoryTracker()}
	svc := NewService(ServiceConfig{Runner: runner, Tracker: tracker, Store: NewMemoryStore()})
	req := ExportRequest{Definition: "resources", Format: FormatCSV, Delivery: DeliveryAsync}
	record, e := svc.RequestExport(context.Background(), Actor{ID: "actor"}, req)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = svc.GenerateExport(context.Background(), Actor{ID: "actor"}, record.ID, req); e == nil {
		t.Fatal("artifact metadata failure ignored")
	}
	record, e = tracker.Status(context.Background(), record.ID)
	if e != nil {
		t.Fatal(e)
	}
	if record.State == StateCompleted {
		t.Fatal("metadata failure marked completed")
	}
}

func TestGenerationRetainsProgressOnlyTrackerCompatibility(t *testing.T) {
	runner, _, _ := guardFixture(t)
	tracker := progressOnlyTracker{NewMemoryTracker()}
	svc := NewService(ServiceConfig{Runner: runner, Tracker: tracker, Store: NewMemoryStore()})
	req := ExportRequest{Definition: "resources", Format: FormatCSV, Delivery: DeliveryAsync}
	record, e := svc.RequestExport(context.Background(), Actor{ID: "actor"}, req)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = svc.GenerateExport(context.Background(), Actor{ID: "actor"}, record.ID, req); e != nil {
		t.Fatal(e)
	}
	if _, e = svc.DownloadMetadata(context.Background(), Actor{ID: "actor"}, record.ID); e != nil {
		t.Fatal(e)
	}
}
