package exportcrud

import (
	"net/url"
	"reflect"
	"testing"
)

func TestQueryFromValues(t *testing.T) {
	values := url.Values{
		"order":         []string{"name asc,created_at desc"},
		"email__ilike":  []string{"%demo%"},
		"role":          []string{"admin"},
		"limit":         []string{"25"},
		"offset":        []string{"50"},
		"search":        []string{"quick"},
		"ignored_param": []string{""},
	}

	query, err := QueryFromValues(values)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if query.Search != "quick" {
		t.Fatalf("expected search to be set, got %q", query.Search)
	}
	if query.Limit != 25 || query.Offset != 50 {
		t.Fatalf("expected paging 25/50, got %d/%d", query.Limit, query.Offset)
	}
	if len(query.Sort) != 2 {
		t.Fatalf("expected 2 sorts, got %d", len(query.Sort))
	}
	if query.Sort[0].Field != "name" || query.Sort[0].Desc {
		t.Fatalf("unexpected first sort: %+v", query.Sort[0])
	}
	if query.Sort[1].Field != "created_at" || !query.Sort[1].Desc {
		t.Fatalf("unexpected second sort: %+v", query.Sort[1])
	}
	if len(query.Filters) != 2 {
		t.Fatalf("expected 2 filters, got %d", len(query.Filters))
	}

	got := map[string]Filter{}
	for _, filter := range query.Filters {
		got[filter.Field] = filter
	}
	if filter, ok := got["email"]; !ok || filter.Op != "ilike" || !reflect.DeepEqual(filter.Value, "%demo%") {
		t.Fatalf("unexpected email filter: %+v", filter)
	}
	if filter, ok := got["role"]; !ok || filter.Op != "eq" || !reflect.DeepEqual(filter.Value, "admin") {
		t.Fatalf("unexpected role filter: %+v", filter)
	}
}

func TestQueryFromValues_SplitInValues(t *testing.T) {
	values := url.Values{
		"status__in": []string{"active,pending"},
	}
	query, err := QueryFromValues(values)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(query.Filters) != 1 {
		t.Fatalf("expected 1 filter, got %d", len(query.Filters))
	}
	filter := query.Filters[0]
	if filter.Field != "status" || filter.Op != "in" {
		t.Fatalf("unexpected filter metadata: %+v", filter)
	}
	value, ok := filter.Value.([]string)
	if !ok || len(value) != 2 || value[0] != "active" || value[1] != "pending" {
		t.Fatalf("unexpected filter value: %#v", filter.Value)
	}
}

func TestQueryFromValues_InvalidLimit(t *testing.T) {
	values := url.Values{
		"limit": []string{"nope"},
	}
	if _, err := QueryFromValues(values); err == nil {
		t.Fatalf("expected error")
	}
}
