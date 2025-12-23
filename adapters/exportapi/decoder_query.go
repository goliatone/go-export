package exportapi

import (
	"encoding/json"
	"net/url"
	"strings"

	exportcrud "github.com/goliatone/go-export/sources/crud"

	"github.com/goliatone/go-export/export"
)

// QueryRequestDecoder decodes querystring payloads into export requests.
type QueryRequestDecoder struct{}

// Decode parses query params into an export request.
func (d QueryRequestDecoder) Decode(req Request) (export.ExportRequest, error) {
	if req == nil {
		return export.ExportRequest{}, export.NewError(export.KindInternal, "request is nil", nil)
	}

	values := url.Values{}
	if parsed := req.URL(); parsed != nil {
		values = parsed.Query()
	}

	datagrid, err := datagridRequestFromValues(values)
	if err != nil {
		return export.ExportRequest{}, err
	}

	return datagrid.ExportRequest(), nil
}

func datagridRequestFromValues(values url.Values) (exportcrud.DatagridRequest, error) {
	definition := strings.TrimSpace(values.Get("definition"))
	resource := strings.TrimSpace(values.Get("resource"))
	format := export.NormalizeFormat(export.Format(values.Get("format")))
	delivery := export.DeliveryMode(strings.ToLower(strings.TrimSpace(values.Get("delivery"))))

	columns := splitCSVValues(values["columns"])

	selectionMode := export.SelectionMode(strings.ToLower(strings.TrimSpace(values.Get("selection_mode"))))
	selectionIDs := splitCSVValues(values["selection_ids"])
	selectionQuery := strings.TrimSpace(values.Get("selection_query"))
	selectionParams, err := parseSelectionParams(values.Get("selection_params"))
	if err != nil {
		return exportcrud.DatagridRequest{}, err
	}

	if selectionMode == "" {
		if len(selectionIDs) > 0 {
			selectionMode = export.SelectionIDs
		} else if selectionQuery != "" {
			selectionMode = export.SelectionQuery
		}
	}

	selection := exportcrud.SelectionPayload{
		Mode: selectionMode,
		IDs:  selectionIDs,
	}
	if selectionQuery != "" || len(selectionParams) > 0 {
		selection.Query = &exportcrud.SelectionQueryPayload{
			Name:   selectionQuery,
			Params: selectionParams,
		}
	}

	queryValues := stripReservedValues(values)
	query, err := exportcrud.QueryFromValues(queryValues)
	if err != nil {
		return exportcrud.DatagridRequest{}, err
	}
	var queryPtr *exportcrud.Query
	if !isEmptyQuery(query) {
		queryPtr = &query
	}

	return exportcrud.DatagridRequest{
		Definition: definition,
		Resource:   resource,
		Format:     format,
		Query:      queryPtr,
		Selection:  selection,
		Columns:    columns,
		Delivery:   delivery,
	}, nil
}

func parseSelectionParams(raw string) (map[string]any, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	params := make(map[string]any)
	if err := json.Unmarshal([]byte(raw), &params); err != nil {
		return nil, export.NewError(export.KindValidation, "invalid selection params", err)
	}
	return params, nil
}

func splitCSVValues(values []string) []string {
	parts := make([]string, 0, len(values))
	for _, value := range values {
		for _, part := range strings.Split(value, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			parts = append(parts, part)
		}
	}
	return parts
}

func stripReservedValues(values url.Values) url.Values {
	if len(values) == 0 {
		return nil
	}
	filtered := url.Values{}
	for key, vals := range values {
		if isReservedKey(key) {
			continue
		}
		filtered[key] = append([]string(nil), vals...)
	}
	return filtered
}

func isReservedKey(key string) bool {
	switch strings.ToLower(strings.TrimSpace(key)) {
	case "definition", "resource", "format", "delivery", "columns",
		"selection_mode", "selection_ids", "selection_query", "selection_params":
		return true
	default:
		return false
	}
}

func isEmptyQuery(query exportcrud.Query) bool {
	return len(query.Filters) == 0 &&
		len(query.Sort) == 0 &&
		query.Search == "" &&
		query.Cursor == "" &&
		query.Limit == 0 &&
		query.Offset == 0
}
