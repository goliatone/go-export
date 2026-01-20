package exportsqlite

import (
	"bytes"
	"context"
	"database/sql"
	"io"
	"math"
	"os"
	"testing"
	"time"

	"github.com/goliatone/go-export/export"
)

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

func TestRenderer_RendersSQLite(t *testing.T) {
	renderer := Renderer{Enabled: true}
	createdAt := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	schema := export.Schema{
		Columns: []export.Column{
			{Name: "id", Type: "int"},
			{Name: "name", Type: "string"},
			{Name: "active", Type: "bool"},
			{Name: "score", Type: "float"},
			{Name: "created_at", Type: "datetime"},
		},
	}
	iter := &stubIterator{rows: []export.Row{
		{int64(1), "alice", true, 9.5, createdAt},
		{int64(2), "bob", false, 7.25, createdAt.Add(2 * time.Hour)},
	}}

	buf := &bytes.Buffer{}
	stats, err := renderer.Render(context.Background(), schema, iter, buf, export.RenderOptions{
		SQLite: export.SQLiteOptions{TableName: "report_rows"},
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if stats.Rows != 2 {
		t.Fatalf("expected 2 rows, got %d", stats.Rows)
	}
	if stats.Bytes == 0 {
		t.Fatalf("expected bytes written")
	}

	path := writeTempSQLite(t, buf.Bytes())
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	infoRows, err := db.Query(`PRAGMA table_info("report_rows")`)
	if err != nil {
		t.Fatalf("table info: %v", err)
	}
	defer infoRows.Close()

	var names []string
	var types []string
	for infoRows.Next() {
		var cid int
		var name, colType string
		var notNull int
		var defaultValue sql.NullString
		var pk int
		if err := infoRows.Scan(&cid, &name, &colType, &notNull, &defaultValue, &pk); err != nil {
			t.Fatalf("scan table info: %v", err)
		}
		names = append(names, name)
		types = append(types, colType)
	}
	if err := infoRows.Err(); err != nil {
		t.Fatalf("table info rows: %v", err)
	}

	wantNames := []string{"id", "name", "active", "score", "created_at"}
	wantTypes := []string{"INTEGER", "TEXT", "INTEGER", "REAL", "TEXT"}
	if len(names) != len(wantNames) {
		t.Fatalf("expected %d columns, got %d", len(wantNames), len(names))
	}
	for i := range wantNames {
		if names[i] != wantNames[i] {
			t.Fatalf("column %d name: expected %q, got %q", i, wantNames[i], names[i])
		}
		if types[i] != wantTypes[i] {
			t.Fatalf("column %d type: expected %q, got %q", i, wantTypes[i], types[i])
		}
	}

	rows, err := db.Query(`SELECT id, name, active, score, created_at FROM "report_rows" ORDER BY id`)
	if err != nil {
		t.Fatalf("select rows: %v", err)
	}
	defer rows.Close()

	type rowData struct {
		id        int64
		name      string
		active    int64
		score     float64
		createdAt string
	}
	var results []rowData
	for rows.Next() {
		var row rowData
		if err := rows.Scan(&row.id, &row.name, &row.active, &row.score, &row.createdAt); err != nil {
			t.Fatalf("scan row: %v", err)
		}
		results = append(results, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows error: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 result rows, got %d", len(results))
	}
	if results[0].id != 1 || results[0].name != "alice" || results[0].active != 1 {
		t.Fatalf("unexpected first row: %+v", results[0])
	}
	if math.Abs(results[0].score-9.5) > 0.0001 {
		t.Fatalf("unexpected first row score: %v", results[0].score)
	}
	if results[0].createdAt != createdAt.Format(time.RFC3339) {
		t.Fatalf("unexpected first row created_at: %q", results[0].createdAt)
	}
	if results[1].id != 2 || results[1].name != "bob" || results[1].active != 0 {
		t.Fatalf("unexpected second row: %+v", results[1])
	}
	if results[1].createdAt != createdAt.Add(2*time.Hour).Format(time.RFC3339) {
		t.Fatalf("unexpected second row created_at: %q", results[1].createdAt)
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
