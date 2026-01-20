package export

import "testing"

func TestContentTypeForFormat_SQLite(t *testing.T) {
	if got := contentTypeForFormat(FormatSQLite); got != "application/vnd.sqlite3" {
		t.Fatalf("expected sqlite content type, got %q", got)
	}
}
