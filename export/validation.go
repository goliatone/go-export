package export

import (
	"fmt"
	"strings"
	"time"
)

// ResolvedExport contains validated inputs for a run.
type ResolvedExport struct {
	Request       ExportRequest
	Definition    ResolvedDefinition
	Columns       []Column
	ColumnNames   []string
	RedactIndices map[int]any
	Filename      string
}

// ResolveExport validates and resolves a request against a definition.
func ResolveExport(req ExportRequest, def ResolvedDefinition, now time.Time) (ResolvedExport, error) {
	req = normalizeRequest(req)

	if !formatAllowed(req.Format, def.AllowedFormats) {
		return ResolvedExport{}, NewError(KindValidation, fmt.Sprintf("format %q not allowed", req.Format), nil)
	}

	if req.Definition == "" {
		return ResolvedExport{}, NewError(KindValidation, "definition is required", nil)
	}

	columns, columnNames, redactions, err := resolveColumns(def.Schema.Columns, req.Columns, def.Policy)
	if err != nil {
		return ResolvedExport{}, err
	}

	if def.Policy.MaxRows > 0 && req.EstimatedRows > def.Policy.MaxRows {
		return ResolvedExport{}, NewError(KindValidation, "estimated rows exceed max rows", nil)
	}
	if def.Policy.MaxBytes > 0 && req.EstimatedBytes > def.Policy.MaxBytes {
		return ResolvedExport{}, NewError(KindValidation, "estimated bytes exceed max bytes", nil)
	}
	if def.Policy.MaxDuration > 0 && req.EstimatedDuration > def.Policy.MaxDuration {
		return ResolvedExport{}, NewError(KindValidation, "estimated duration exceeds max duration", nil)
	}

	filename, err := renderFilename(def, req, now)
	if err != nil {
		return ResolvedExport{}, NewError(KindValidation, "invalid filename template", err)
	}

	return ResolvedExport{
		Request:       req,
		Definition:    def,
		Columns:       columns,
		ColumnNames:   columnNames,
		RedactIndices: redactions,
		Filename:      filename,
	}, nil
}

func normalizeRequest(req ExportRequest) ExportRequest {
	if req.Format == "" {
		req.Format = FormatCSV
	}
	if req.Delivery == "" {
		req.Delivery = DeliveryAuto
	}
	if req.Selection.Mode == "" {
		req.Selection.Mode = SelectionAll
	}
	if req.RenderOptions.CSV.Delimiter == 0 {
		req.RenderOptions.CSV.Delimiter = ','
	}
	if !req.RenderOptions.CSV.HeadersSet {
		req.RenderOptions.CSV.IncludeHeaders = true
	}
	if !req.RenderOptions.XLSX.HeadersSet {
		req.RenderOptions.XLSX.IncludeHeaders = true
	}
	if req.RenderOptions.JSON.Mode == "" && req.Format == FormatNDJSON {
		req.RenderOptions.JSON.Mode = JSONModeLines
	}
	if req.RenderOptions.JSON.Mode == "" {
		req.RenderOptions.JSON.Mode = JSONModeArray
	}
	if req.RenderOptions.Format.Locale == "" {
		req.RenderOptions.Format.Locale = req.Locale
	}
	if req.RenderOptions.Format.Timezone == "" {
		req.RenderOptions.Format.Timezone = req.Timezone
	}
	return req
}

func formatAllowed(format Format, allowed []Format) bool {
	for _, f := range allowed {
		if f == format {
			return true
		}
	}
	return false
}

func resolveColumns(schema []Column, requested []string, policy ExportPolicy) ([]Column, []string, map[int]any, error) {
	if len(schema) == 0 {
		return nil, nil, nil, NewError(KindValidation, "schema has no columns", nil)
	}

	allowed := policy.AllowedColumns
	allowedSet := make(map[string]struct{})
	if len(allowed) > 0 {
		for _, name := range allowed {
			allowedSet[name] = struct{}{}
		}
	}

	schemaSet := make(map[string]Column)
	for _, col := range schema {
		schemaSet[col.Name] = col
	}

	projection := requested
	if len(projection) == 0 {
		if len(allowed) > 0 {
			projection = allowed
		} else {
			for _, col := range schema {
				projection = append(projection, col.Name)
			}
		}
	}

	columns := make([]Column, 0, len(projection))
	columnNames := make([]string, 0, len(projection))
	seen := make(map[string]struct{})
	for _, name := range projection {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}

		col, ok := schemaSet[name]
		if !ok {
			return nil, nil, nil, NewError(KindValidation, fmt.Sprintf("unknown column %q", name), nil)
		}
		if len(allowedSet) > 0 {
			if _, ok := allowedSet[name]; !ok {
				return nil, nil, nil, NewError(KindValidation, fmt.Sprintf("column %q not allowed", name), nil)
			}
		}
		columns = append(columns, col)
		columnNames = append(columnNames, col.Name)
	}

	redactions := make(map[int]any)
	if len(policy.RedactColumns) > 0 {
		redactionValue := policy.RedactionValue
		if redactionValue == nil {
			redactionValue = "[redacted]"
		}
		for idx, col := range columns {
			for _, name := range policy.RedactColumns {
				if name == col.Name {
					redactions[idx] = redactionValue
				}
			}
		}
	}

	if len(columns) == 0 {
		return nil, nil, nil, NewError(KindValidation, "no columns selected", nil)
	}

	return columns, columnNames, redactions, nil
}

func mergePolicy(base ExportPolicy, override ExportPolicy) ExportPolicy {
	merged := base
	if len(override.AllowedColumns) > 0 {
		merged.AllowedColumns = override.AllowedColumns
	}
	if len(override.RedactColumns) > 0 {
		merged.RedactColumns = override.RedactColumns
	}
	if override.RedactionValue != nil {
		merged.RedactionValue = override.RedactionValue
	}
	if override.MaxRows > 0 {
		merged.MaxRows = override.MaxRows
	}
	if override.MaxBytes > 0 {
		merged.MaxBytes = override.MaxBytes
	}
	if override.MaxDuration > 0 {
		merged.MaxDuration = override.MaxDuration
	}
	return merged
}
