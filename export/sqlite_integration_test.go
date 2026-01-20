package export

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"testing"

	exportsqlite "github.com/goliatone/go-export/adapters/sqlite"
)

func TestRunner_SQLiteExport(t *testing.T) {
	runner := NewRunner()
	if err := runner.Renderers.Register(FormatSQLite, exportsqlite.Renderer{Enabled: true}); err != nil {
		t.Fatalf("register renderer: %v", err)
	}
	if err := runner.Definitions.Register(ExportDefinition{
		Name:           "users",
		RowSourceKey:   "stub",
		AllowedFormats: []Format{FormatSQLite},
		Schema: Schema{
			Columns: []Column{{Name: "id", Type: "int"}, {Name: "name"}},
		},
	}); err != nil {
		t.Fatalf("register definition: %v", err)
	}

	iter := &stubIterator{rows: []Row{{int64(1), "alice"}, {int64(2), "bob"}}}
	if err := runner.RowSources.Register("stub", func(req ExportRequest, def ResolvedDefinition) (RowSource, error) {
		_ = req
		_ = def
		return &stubSource{iter: iter}, nil
	}); err != nil {
		t.Fatalf("register source: %v", err)
	}

	buf := &bytes.Buffer{}
	result, err := runner.Run(context.Background(), ExportRequest{
		Definition: "users",
		Format:     FormatSQLite,
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
