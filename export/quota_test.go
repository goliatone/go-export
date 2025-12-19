package export

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"
)

type recordingQuota struct {
	called bool
	actor  Actor
}

func (q *recordingQuota) Allow(_ context.Context, actor Actor, _ ExportRequest, _ ResolvedDefinition) error {
	q.called = true
	q.actor = actor
	return nil
}

func TestRateLimiter_AllowsByActorScope(t *testing.T) {
	now := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	limiter := &RateLimiter{
		Max:    1,
		Window: time.Minute,
		Now:    func() time.Time { return now },
	}
	def := ResolvedDefinition{ExportDefinition: ExportDefinition{Name: "users"}}
	actorA := Actor{
		ID: "actor-a",
		Scope: Scope{
			TenantID:    "tenant",
			WorkspaceID: "workspace",
		},
	}
	actorB := Actor{
		ID: "actor-b",
		Scope: Scope{
			TenantID:    "tenant",
			WorkspaceID: "workspace",
		},
	}

	if err := limiter.Allow(context.Background(), actorA, ExportRequest{}, def); err != nil {
		t.Fatalf("expected first allow, got %v", err)
	}
	err := limiter.Allow(context.Background(), actorA, ExportRequest{}, def)
	if err == nil {
		t.Fatalf("expected rate limit error")
	}
	var expErr *ExportError
	if !errors.As(err, &expErr) || expErr.Kind != KindValidation {
		t.Fatalf("expected validation error, got %v", err)
	}
	if err := limiter.Allow(context.Background(), actorB, ExportRequest{}, def); err != nil {
		t.Fatalf("expected separate actor allowance, got %v", err)
	}
}

func TestRunner_QuotaHookReceivesActor(t *testing.T) {
	quota := &recordingQuota{}
	actor := Actor{
		ID: "actor-1",
		Scope: Scope{
			TenantID:    "tenant",
			WorkspaceID: "workspace",
		},
	}

	runner := NewRunner()
	runner.QuotaHook = quota
	runner.ActorProvider = stubActorProvider{actor: actor}

	if err := runner.Definitions.Register(ExportDefinition{
		Name:         "users",
		RowSourceKey: "stub",
		Schema:       Schema{Columns: []Column{{Name: "id"}}},
	}); err != nil {
		t.Fatalf("register definition: %v", err)
	}
	if err := runner.RowSources.Register("stub", func(req ExportRequest, def ResolvedDefinition) (RowSource, error) {
		_ = req
		_ = def
		return &stubSource{iter: &stubIterator{rows: []Row{{"1"}}}}, nil
	}); err != nil {
		t.Fatalf("register source: %v", err)
	}

	_, err := runner.Run(context.Background(), ExportRequest{
		Definition: "users",
		Format:     FormatCSV,
		Output:     &bytes.Buffer{},
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !quota.called {
		t.Fatalf("expected quota hook to be called")
	}
	if quota.actor.ID != actor.ID {
		t.Fatalf("expected actor %q, got %q", actor.ID, quota.actor.ID)
	}
}
