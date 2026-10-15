package exporthttp

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/goliatone/go-export/export"
)

type failingSource struct{ err error }

func (s failingSource) Open(context.Context, export.RowSourceSpec) (export.RowIterator, error) {
	return nil, s.err
}
func TestHandlerPublicErrorDisclosure(t *testing.T) {
	private := "Group-B-private /storage/private.db secret-value"
	for _, tc := range []struct {
		name   string
		err    error
		status int
		code   string
	}{
		{"internal", errors.New(private), 500, "internal"},
		{"validation", export.NewError(export.KindValidation, private, nil), 400, "validation"},
		{"authorization", export.NewError(export.KindAuthz, private, nil), 403, "authz"},
		{"not-found", export.NewError(export.KindNotFound, private, nil), 404, "not_found"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runner := export.NewRunner()
			if err := runner.Definitions.Register(export.ExportDefinition{Name: "resources", RowSourceKey: "failure", Schema: export.Schema{Columns: []export.Column{{Name: "id"}}}}); err != nil {
				t.Fatal(err)
			}
			if err := runner.RowSources.Register("failure", func(export.ExportRequest, export.ResolvedDefinition) (export.RowSource, error) {
				return failingSource{tc.err}, nil
			}); err != nil {
				t.Fatal(err)
			}
			h := NewHandler(Config{Runner: runner})
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest("POST", "/admin/exports", strings.NewReader(`{"definition":"resources","format":"csv"}`)))
			var got struct {
				Error struct{ Message, Code string }
			}
			if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
				t.Fatal(err)
			}
			safe, err := export.AsGoError(tc.err).ToPublicErrorResponse()
			if err != nil {
				t.Fatal(err)
			}
			if w.Code != tc.status || got.Error.Code != tc.code || got.Error.Message != safe.Error.Message || strings.Contains(w.Body.String(), private) || w.Header().Get("Content-Disposition") != "" {
				t.Fatalf("unsafe error response: %d %s", w.Code, w.Body.String())
			}
		})
	}
}
