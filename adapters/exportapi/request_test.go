package exportapi

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/goliatone/go-export/export"
)

type stubRequest struct {
	body io.ReadCloser
}

func (s stubRequest) Context() context.Context { return context.Background() }
func (s stubRequest) Method() string           { return "POST" }
func (s stubRequest) Path() string             { return "/admin/exports" }
func (s stubRequest) Header(string) string     { return "" }
func (s stubRequest) Query(string) string      { return "" }
func (s stubRequest) Body() io.ReadCloser      { return s.body }

func TestJSONRequestDecoder_FormatAlias(t *testing.T) {
	payload := `{"definition":"users","format":"excel"}`
	decoder := JSONRequestDecoder{}
	req, err := decoder.Decode(stubRequest{body: io.NopCloser(strings.NewReader(payload))})
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if req.Format != export.FormatXLSX {
		t.Fatalf("expected xlsx, got %q", req.Format)
	}
}

func TestJSONRequestDecoder_FormatLowercase(t *testing.T) {
	payload := `{"definition":"users","format":"CSV"}`
	decoder := JSONRequestDecoder{}
	req, err := decoder.Decode(stubRequest{body: io.NopCloser(strings.NewReader(payload))})
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if req.Format != export.FormatCSV {
		t.Fatalf("expected csv, got %q", req.Format)
	}
}

func TestJSONRequestDecoder_SelectionQuery(t *testing.T) {
	payload := `{"definition":"users","selection":{"mode":"query","query":{"name":"active_users","params":{"status":"active"}}}}`
	decoder := JSONRequestDecoder{}
	req, err := decoder.Decode(stubRequest{body: io.NopCloser(strings.NewReader(payload))})
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if req.Selection.Mode != export.SelectionQuery {
		t.Fatalf("expected query mode, got %q", req.Selection.Mode)
	}
	if req.Selection.Query.Name != "active_users" {
		t.Fatalf("expected selection query name, got %q", req.Selection.Query.Name)
	}
	params, ok := req.Selection.Query.Params.(map[string]any)
	if !ok {
		t.Fatalf("expected selection query params map, got %T", req.Selection.Query.Params)
	}
	if params["status"] != "active" {
		t.Fatalf("expected selection query params, got %v", params["status"])
	}
}
