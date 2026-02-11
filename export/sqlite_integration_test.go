package export_test

import (
	"bytes"
	"context"
	"database/sql"
	"io"
	"os"
	"testing"

	exportsqlite "github.com/goliatone/go-export/adapters/sqlite"
	"github.com/goliatone/go-export/export"
)

func TestRunner_SQLiteExport(t *testing.T) {
	runner := export.NewRunner()
	if err := runner.Renderers.Register(export.FormatSQLite, exportsqlite.Renderer{Enabled: true}); err != nil {
		t.Fatalf("register renderer: %v", err)
	}
	if err := runner.Definitions.Register(export.ExportDefinition{
		Name:           "users",
		RowSourceKey:   "stub",
		AllowedFormats: []export.Format{export.FormatSQLite},
		Schema: export.Schema{
			Columns: []export.Column{{Name: "id", Type: "int"}, {Name: "name"}},
		},
	}); err != nil {
		t.Fatalf("register definition: %v", err)
	}

	iter := &stubIterator{rows: []export.Row{{int64(1), "alice"}, {int64(2), "bob"}}}
	if err := runner.RowSources.Register("stub", func(req export.ExportRequest, def export.ResolvedDefinition) (export.RowSource, error) {
		_ = req
		_ = def
		return &stubSource{iter: iter}, nil
	}); err != nil {
		t.Fatalf("register source: %v", err)
	}

	buf := &bytes.Buffer{}
	result, err := runner.Run(context.Background(), export.ExportRequest{
		Definition: "users",
		Format:     export.FormatSQLite,
		Output:     buf,
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if result.Rows != 2 {
		t.Fatalf("expected 2 rows, got %d", result.Rows)
	}
	if buf.Len() == 0 {
		t.Fatalf("expected sqlite output")
	}

	path := writeTempSQLite(t, buf.Bytes())
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM "data"`).Scan(&count); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 rows in db, got %d", count)
	}
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

func (it *stubIterator) Close() error {
	return nil
}

func writeTempSQLite(t *testing.T, data []byte) string {
	t.Helper()

	file, err := os.CreateTemp("", "sqlite-test-*.sqlite")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		_ = os.Remove(file.Name())
		t.Fatalf("write temp file: %v", err)
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(file.Name())
		t.Fatalf("close temp file: %v", err)
	}
	t.Cleanup(func() {
		_ = os.Remove(file.Name())
	})
	return file.Name()
}
