package exportcrud

import (
	"net/url"
	"strconv"
	"strings"

	"github.com/goliatone/go-export/export"
)

// QueryFromValues converts URL query params into a crud Query.
func QueryFromValues(values url.Values) (Query, error) {
	if len(values) == 0 {
		return Query{}, nil
	}

	query := Query{}
	if search := strings.TrimSpace(values.Get("search")); search != "" {
		query.Search = search
	} else if search := strings.TrimSpace(values.Get("q")); search != "" {
		query.Search = search
	}
	filters := make([]Filter, 0)
	for key, vals := range values {
		if len(vals) == 0 {
			continue
		}
		if !hasNonEmptyValue(vals) {
			continue
		}
		switch key {
		case "order", "sort":
			query.Sort = append(query.Sort, parseSortOrder(vals[0])...)
			continue
		case "q", "search":
			continue
		case "limit":
			limit, err := parseInt(vals[0])
			if err != nil {
				return Query{}, export.NewError(export.KindValidation, "invalid limit", err)
			}
			query.Limit = limit
			continue
		case "offset":
			offset, err := parseInt(vals[0])
			if err != nil {
				return Query{}, export.NewError(export.KindValidation, "invalid offset", err)
			}
			query.Offset = offset
			continue
		case "cursor":
			query.Cursor = vals[0]
			continue
		}

		field, op := splitFilterKey(key)
		if field == "" {
			continue
		}
		filters = append(filters, Filter{
			Field: field,
			Op:    op,
			Value: parseFilterValues(op, vals),
		})
	}

	query.Filters = filters
	return query, nil
}

func hasNonEmptyValue(values []string) bool {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return true
		}
	}
	return false
}

func splitFilterKey(key string) (string, string) {
	parts := strings.SplitN(key, "__", 2)
	field := strings.TrimSpace(parts[0])
	op := "eq"
	if len(parts) == 2 {
		if candidate := strings.TrimSpace(parts[1]); candidate != "" {
			op = candidate
		}
	}
	return field, op
}

func parseFilterValues(op string, values []string) any {
	if len(values) == 0 {
		return ""
	}
	if len(values) == 1 {
		value := values[0]
		if (op == "in" || op == "ilike") && strings.Contains(value, ",") {
			parts := strings.Split(value, ",")
			for i := range parts {
				parts[i] = strings.TrimSpace(parts[i])
			}
			return parts
		}
		return value
	}
	return values
}

func parseSortOrder(raw string) []Sort {
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	sorts := make([]Sort, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		desc := false
		field := ""
		if strings.HasPrefix(trimmed, "-") || strings.HasPrefix(trimmed, "+") {
			desc = strings.HasPrefix(trimmed, "-")
			field = strings.TrimSpace(trimmed[1:])
		} else {
			fields := strings.Fields(trimmed)
			if len(fields) > 0 {
				field = fields[0]
			}
			if len(fields) > 1 {
				desc = strings.EqualFold(fields[1], "desc")
			}
		}
		if field == "" {
			continue
		}
		sorts = append(sorts, Sort{Field: field, Desc: desc})
	}
	return sorts
}

func parseInt(raw string) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, err
	}
	return value, nil
}
