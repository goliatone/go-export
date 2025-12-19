package export

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"
)

func TestCSVRenderer_TimezoneFormatting(t *testing.T) {
	buf := &bytes.Buffer{}
	iter := &stubIterator{rows: []Row{
		{time.Date(2024, 1, 2, 15, 4, 5, 0, time.UTC)},
	}}

	renderer := CSVRenderer{}
	schema := Schema{Columns: []Column{{Name: "created_at", Type: "datetime"}}}

	_, err := renderer.Render(context.Background(), schema, iter, buf, RenderOptions{
		CSV: CSVOptions{
			IncludeHeaders: false,
			HeadersSet:     true,
		},
		Format: FormatOptions{Timezone: "America/New_York"},
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}

	output := strings.TrimSpace(buf.String())
	if !strings.Contains(output, "-05:00") {
		t.Fatalf("expected timezone offset in output, got %q", output)
	}
}
