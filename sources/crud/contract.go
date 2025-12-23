package exportcrud

import "github.com/goliatone/go-export/export"

// SelectionPayload captures datagrid row selection state.
type SelectionPayload struct {
	Mode  export.SelectionMode   `json:"mode,omitempty"`
	IDs   []string               `json:"ids,omitempty"`
	Query *SelectionQueryPayload `json:"query,omitempty"`
}

func (p SelectionPayload) toSelection() export.Selection {
	selection := export.Selection{Mode: p.Mode, IDs: p.IDs}
	if p.Query != nil {
		selection.Query = export.SelectionQueryRef{
			Name:   p.Query.Name,
			Params: p.Query.Params,
		}
	}
	return selection
}

// SelectionQueryPayload captures named selection query inputs.
type SelectionQueryPayload struct {
	Name   string         `json:"name"`
	Params map[string]any `json:"params,omitempty"`
}

// DatagridRequest captures the datagrid export contract payload.
type DatagridRequest struct {
	Definition     string              `json:"definition"`
	Resource       string              `json:"resource,omitempty"`
	Format         export.Format       `json:"format,omitempty"`
	Query          *Query              `json:"query,omitempty"`
	Selection      SelectionPayload    `json:"selection,omitempty"`
	Columns        []string            `json:"columns,omitempty"`
	Delivery       export.DeliveryMode `json:"delivery,omitempty"`
	EstimatedRows  int                 `json:"estimated_rows,omitempty"`
	EstimatedBytes int64               `json:"estimated_bytes,omitempty"`
}

// ExportRequest converts the datagrid payload into a core export request.
func (r DatagridRequest) ExportRequest() export.ExportRequest {
	delivery := r.Delivery
	if delivery == "" {
		delivery = export.DeliveryAuto
	}
	return export.ExportRequest{
		Definition:     r.Definition,
		Resource:       r.Resource,
		Format:         r.Format,
		Query:          r.Query,
		Selection:      r.Selection.toSelection(),
		Columns:        r.Columns,
		Delivery:       delivery,
		EstimatedRows:  r.EstimatedRows,
		EstimatedBytes: r.EstimatedBytes,
	}
}
