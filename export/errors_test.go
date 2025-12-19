package export

import (
	"context"
	"testing"

	errorslib "github.com/goliatone/go-errors"
)

func TestAsGoErrorMapping(t *testing.T) {
	cases := []struct {
		err      error
		category errorslib.Category
		code     string
	}{
		{NewError(KindValidation, "bad input", nil), errorslib.CategoryValidation, "validation"},
		{NewError(KindAuthz, "nope", nil), errorslib.CategoryAuthz, "authz"},
		{NewError(KindNotFound, "missing", nil), errorslib.CategoryNotFound, "not_found"},
		{context.DeadlineExceeded, errorslib.CategoryOperation, "timeout"},
		{context.Canceled, errorslib.CategoryOperation, "canceled"},
		{NewError(KindInternal, "boom", nil), errorslib.CategoryInternal, "internal"},
	}

	for _, tc := range cases {
		mapped := AsGoError(tc.err)
		if mapped == nil {
			t.Fatalf("expected mapping for %v", tc.err)
		}
		if mapped.Category != tc.category {
			t.Fatalf("expected category %s, got %s", tc.category, mapped.Category)
		}
		if mapped.TextCode != tc.code {
			t.Fatalf("expected text code %s, got %s", tc.code, mapped.TextCode)
		}
	}
}
