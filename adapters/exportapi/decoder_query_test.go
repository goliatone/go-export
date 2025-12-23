package exportapi

import (
	"context"
	"io"
	"net/url"
	"testing"

	exportcrud "github.com/goliatone/go-export/sources/crud"

	"github.com/goliatone/go-export/export"
)

type stubQueryRequest struct {
	parsed *url.URL
}

func (s stubQueryRequest) Context() context.Context { return context.Background() }
func (s stubQueryRequest) Method() string           { return "GET" }
func (s stubQueryRequest) Path() string             { return "/admin/exports" }
func (s stubQueryRequest) URL() *url.URL            { return s.parsed }
func (s stubQueryRequest) Header(string) string     { return "" }
func (s stubQueryRequest) Query(name string) string {
	if s.parsed == nil {
		return ""
	}
	return s.parsed.Query().Get(name)
}
func (s stubQueryRequest) Body() io.ReadCloser { return nil }

func TestQueryRequestDecoder_Mapping(t *testing.T) {
	raw := "/admin/exports?definition=users&format=xls&delivery=sync&columns=id,name&search=alice&order=-name&status__eq=active&limit=25&offset=50"
	parsed, err := url.ParseRequestURI(raw)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	decoder := QueryRequestDecoder{}
	req, err := decoder.Decode(stubQueryRequest{parsed: parsed})
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if req.Definition != "users" {
		t.Fatalf("expected definition users, got %q", req.Definition)
	}
	if req.Format != export.FormatXLSX {
		t.Fatalf("expected xlsx, got %q", req.Format)
	}
	if req.Delivery != export.DeliverySync {
		t.Fatalf("expected sync delivery, got %q", req.Delivery)
	}
	if len(req.Columns) != 2 || req.Columns[0] != "id" || req.Columns[1] != "name" {
		t.Fatalf("expected columns, got %v", req.Columns)
	}
	query, ok := req.Query.(*exportcrud.Query)
	if !ok || query == nil {
		t.Fatalf("expected crud query, got %T", req.Query)
	}
	if query.Search != "alice" {
		t.Fatalf("expected search alice, got %q", query.Search)
	}
	if query.Limit != 25 || query.Offset != 50 {
		t.Fatalf("expected limit/offset, got %d/%d", query.Limit, query.Offset)
	}
	if len(query.Sort) != 1 || query.Sort[0].Field != "name" || !query.Sort[0].Desc {
		t.Fatalf("expected sort by name desc, got %+v", query.Sort)
	}
	if len(query.Filters) != 1 || query.Filters[0].Field != "status" || query.Filters[0].Op != "eq" || query.Filters[0].Value != "active" {
		t.Fatalf("expected status filter, got %+v", query.Filters)
	}
}

func TestQueryRequestDecoder_DefaultFormat(t *testing.T) {
	raw := "/admin/exports?definition=users"
	parsed, err := url.ParseRequestURI(raw)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	decoder := QueryRequestDecoder{}
	req, err := decoder.Decode(stubQueryRequest{parsed: parsed})
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if req.Format != export.FormatCSV {
		t.Fatalf("expected csv default, got %q", req.Format)
	}
}
