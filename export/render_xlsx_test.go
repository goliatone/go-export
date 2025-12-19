package export

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/xuri/excelize/v2"
)

func TestXLSXRenderer_WritesRows(t *testing.T) {
	buf := &bytes.Buffer{}
	iter := &stubIterator{rows: []Row{
		{int64(1), "alice", 12.5, time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC), true},
	}}

	renderer := XLSXRenderer{}
	schema := Schema{Columns: []Column{
		{Name: "id", Type: "int"},
		{Name: "name", Label: "Full Name", Type: "string"},
		{Name: "amount", Type: "number"},
		{Name: "created_at", Type: "datetime"},
		{Name: "active", Type: "bool"},
	}}

	stats, err := renderer.Render(context.Background(), schema, iter, buf, RenderOptions{
		XLSX: XLSXOptions{
			IncludeHeaders: true,
			HeadersSet:     true,
		},
		Format: FormatOptions{Timezone: "UTC"},
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if stats.Rows != 1 {
		t.Fatalf("expected 1 row, got %d", stats.Rows)
	}
	if stats.Bytes == 0 {
		t.Fatalf("expected non-zero bytes")
	}

	file, err := excelize.OpenReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("open xlsx: %v", err)
	}

	sheet := file.GetSheetName(0)
	rows, err := file.GetRows(sheet)
	if err != nil {
		t.Fatalf("get rows: %v", err)
	}
	if len(rows) < 2 {
		t.Fatalf("expected header + data rows, got %d", len(rows))
	}
	if len(rows[0]) < 2 || rows[0][1] != "Full Name" {
		t.Fatalf("expected header label, got %v", rows[0])
	}
	if len(rows[1]) < 2 || rows[1][1] != "alice" {
		t.Fatalf("expected data row, got %v", rows[1])
	}
}

func TestXLSXRenderer_MaxRows(t *testing.T) {
	buf := &bytes.Buffer{}
	iter := &stubIterator{rows: []Row{{"a"}, {"b"}}}
	renderer := XLSXRenderer{}

	_, err := renderer.Render(context.Background(), Schema{Columns: []Column{{Name: "name"}}}, iter, buf, RenderOptions{
		XLSX: XLSXOptions{
			MaxRows:    1,
			HeadersSet: true,
		},
	})
	if err == nil {
		t.Fatalf("expected max rows error")
	}
	if exportErr, ok := err.(*ExportError); !ok || exportErr.Kind != KindValidation {
		t.Fatalf("expected validation error, got %v", err)
	}
}

func TestXLSXRenderer_InvalidType(t *testing.T) {
	buf := &bytes.Buffer{}
	iter := &stubIterator{rows: []Row{{"not-an-int"}}}
	renderer := XLSXRenderer{}

	_, err := renderer.Render(context.Background(), Schema{Columns: []Column{{Name: "id", Type: "int"}}}, iter, buf, RenderOptions{
		XLSX: XLSXOptions{HeadersSet: true},
	})
	if err == nil {
		t.Fatalf("expected type validation error")
	}
	if exportErr, ok := err.(*ExportError); !ok || exportErr.Kind != KindValidation {
		t.Fatalf("expected validation error, got %v", err)
	}
}
