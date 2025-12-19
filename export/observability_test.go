package export

import (
	"bytes"
	"context"
	"testing"
	"time"
)

type recordingEmitter struct {
	events []ChangeEvent
}

func (r *recordingEmitter) Emit(_ context.Context, evt ChangeEvent) error {
	r.events = append(r.events, evt)
	return nil
}

type recordingMetrics struct {
	events []MetricsEvent
}

func (r *recordingMetrics) Emit(_ context.Context, evt MetricsEvent) error {
	r.events = append(r.events, evt)
	return nil
}

type stubActorProvider struct {
	actor Actor
	err   error
}

func (s stubActorProvider) FromContext(_ context.Context) (Actor, error) {
	return s.actor, s.err
}

type errorIterator struct {
	err error
}

func (it *errorIterator) Next(ctx context.Context) (Row, error) {
	_ = ctx
	return nil, it.err
}

func (it *errorIterator) Close() error {
	return nil
}

func TestRunner_EmitsEventsAndMetrics(t *testing.T) {
	emitter := &recordingEmitter{}
	metrics := &recordingMetrics{}
	actor := Actor{
		ID: "actor-1",
		Scope: Scope{
			TenantID:    "tenant-1",
			WorkspaceID: "workspace-1",
		},
	}

	runner := NewRunner()
	runner.Emitter = emitter
	runner.Metrics = metrics
	runner.ActorProvider = stubActorProvider{actor: actor}

	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	calls := 0
	runner.Now = func() time.Time {
		calls++
		return base.Add(time.Duration(calls) * time.Second)
	}

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

	if len(emitter.events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(emitter.events))
	}
	if emitter.events[0].Name != "export.requested" {
		t.Fatalf("expected requested event, got %q", emitter.events[0].Name)
	}
	if emitter.events[2].Name != "export.completed" {
		t.Fatalf("expected completed event, got %q", emitter.events[2].Name)
	}
	if emitter.events[0].Actor.ID != actor.ID {
		t.Fatalf("expected actor ID %q, got %q", actor.ID, emitter.events[0].Actor.ID)
	}
	columns, ok := emitter.events[0].Metadata["columns"].([]string)
	if !ok || len(columns) != 1 || columns[0] != "id" {
		t.Fatalf("expected columns metadata, got %#v", emitter.events[0].Metadata["columns"])
	}
	rows, ok := emitter.events[2].Metadata["rows"].(int64)
	if !ok || rows != 1 {
		t.Fatalf("expected rows=1, got %#v", emitter.events[2].Metadata["rows"])
	}

	if len(metrics.events) != 2 {
		t.Fatalf("expected 2 metrics events, got %d", len(metrics.events))
	}
	if metrics.events[0].Name != "export.requested" {
		t.Fatalf("expected requested metrics, got %q", metrics.events[0].Name)
	}
	if metrics.events[1].Name != "export.completed" {
		t.Fatalf("expected completed metrics, got %q", metrics.events[1].Name)
	}
	if metrics.events[1].Rows != 1 {
		t.Fatalf("expected metrics rows=1, got %d", metrics.events[1].Rows)
	}
	if metrics.events[1].Bytes == 0 {
		t.Fatalf("expected metrics bytes > 0")
	}
	if metrics.events[1].Duration <= 0 {
		t.Fatalf("expected metrics duration > 0")
	}
	if metrics.events[1].ErrorKind != "" {
		t.Fatalf("expected no error kind, got %q", metrics.events[1].ErrorKind)
	}
}

func TestRunner_EmitsFailureMetrics(t *testing.T) {
	metrics := &recordingMetrics{}

	runner := NewRunner()
	runner.Metrics = metrics

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
		return &stubSource{iter: &errorIterator{err: NewError(KindValidation, "boom", nil)}}, nil
	}); err != nil {
		t.Fatalf("register source: %v", err)
	}

	_, err := runner.Run(context.Background(), ExportRequest{
		Definition: "users",
		Format:     FormatCSV,
		Output:     &bytes.Buffer{},
	})
	if err == nil {
		t.Fatalf("expected error")
	}

	if len(metrics.events) != 2 {
		t.Fatalf("expected 2 metrics events, got %d", len(metrics.events))
	}
	last := metrics.events[len(metrics.events)-1]
	if last.Name != "export.failed" {
		t.Fatalf("expected failed metrics, got %q", last.Name)
	}
	if last.ErrorKind != KindValidation {
		t.Fatalf("expected validation error kind, got %q", last.ErrorKind)
	}
}
