package examples

import (
	"bytes"
	"context"
	"io"
	"testing"

	gcmd "github.com/goliatone/go-command"
	"github.com/goliatone/go-command/dispatcher"
	exportcmd "github.com/goliatone/go-export/command"
	"github.com/goliatone/go-export/export"
	exportqry "github.com/goliatone/go-export/query"
)

type allowGuard struct {
	exportCalls   int
	downloadCalls int
}

func (g *allowGuard) AuthorizeExport(ctx context.Context, actor export.Actor, req export.ExportRequest, def export.ResolvedDefinition) error {
	_ = ctx
	_ = actor
	_ = req
	_ = def
	g.exportCalls++
	return nil
}

func (g *allowGuard) AuthorizeDownload(ctx context.Context, actor export.Actor, exportID string) error {
	_ = ctx
	_ = actor
	_ = exportID
	g.downloadCalls++
	return nil
}

type stubSource struct {
	iter export.RowIterator
}

func (s *stubSource) Open(ctx context.Context, spec export.RowSourceSpec) (export.RowIterator, error) {
	_ = ctx
	_ = spec
	return s.iter, nil
}

type stubIterator struct {
	rows  []export.Row
	index int
}

func (it *stubIterator) Next(ctx context.Context) (export.Row, error) {
	_ = ctx
	if it.index >= len(it.rows) {
		return nil, io.EOF
	}
	row := it.rows[it.index]
	it.index++
	return row, nil
}

func (it *stubIterator) Close() error { return nil }

func TestCommandQueryWiring(t *testing.T) {
	runner := export.NewRunner()
	if err := runner.Definitions.Register(export.ExportDefinition{
		Name:         "users",
		RowSourceKey: "stub",
		Schema: export.Schema{
			Columns: []export.Column{{Name: "id"}, {Name: "name"}},
		},
	}); err != nil {
		t.Fatalf("register definition: %v", err)
	}
	if err := runner.RowSources.Register("stub", func(req export.ExportRequest, def export.ResolvedDefinition) (export.RowSource, error) {
		_ = req
		_ = def
		return &stubSource{
			iter: &stubIterator{rows: []export.Row{{"1", "alice"}}},
		}, nil
	}); err != nil {
		t.Fatalf("register source: %v", err)
	}

	guard := &allowGuard{}
	tracker := export.NewMemoryTracker()
	store := export.NewMemoryStore()

	service := export.NewService(export.ServiceConfig{
		Runner:  runner,
		Guard:   guard,
		Tracker: tracker,
		Store:   store,
	})

	reg := gcmd.NewRegistry()
	subs, err := RegisterExportHandlers(reg, service)
	if err != nil {
		t.Fatalf("register handlers: %v", err)
	}
	defer func() {
		for _, sub := range subs {
			sub.Unsubscribe()
		}
	}()

	buf := &bytes.Buffer{}
	actor := export.Actor{ID: "actor-1", Scope: export.Scope{TenantID: "tenant-1"}}

	record, err := dispatcher.DispatchWithResult[exportcmd.RequestExport, export.ExportRecord](
		context.Background(),
		exportcmd.RequestExport{
			Actor: actor,
			Request: export.ExportRequest{
				Definition: "users",
				Format:     export.FormatCSV,
				Output:     buf,
			},
		},
	)
	if err != nil {
		t.Fatalf("dispatch request export: %v", err)
	}
	if record.ID == "" {
		t.Fatalf("expected export ID")
	}
	if buf.Len() == 0 {
		t.Fatalf("expected export output")
	}

	status, err := dispatcher.Query[exportqry.ExportStatus, export.ExportRecord](
		context.Background(),
		exportqry.ExportStatus{
			Actor:    actor,
			ExportID: record.ID,
		},
	)
	if err != nil {
		t.Fatalf("query status: %v", err)
	}
	if status.ID != record.ID {
		t.Fatalf("unexpected record ID: %q", status.ID)
	}
	if guard.exportCalls == 0 || guard.downloadCalls == 0 {
		t.Fatalf("expected guard calls to be recorded")
	}
}
