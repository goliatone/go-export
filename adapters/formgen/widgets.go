package exportformgen

import (
	"fmt"
	"strings"
)

// Field defines a form field for formgen-style UIs.
type Field struct {
	Name     string   `json:"name"`
	Label    string   `json:"label"`
	Type     string   `json:"type"`
	Required bool     `json:"required,omitempty"`
	Options  []string `json:"options,omitempty"`
	Hint     string   `json:"hint,omitempty"`
}

// Form defines the export request form widget.
type Form struct {
	ID          string   `json:"id"`
	Title       string   `json:"title"`
	Action      string   `json:"action"`
	Method      string   `json:"method"`
	SubmitLabel string   `json:"submit_label"`
	Fields      []Field  `json:"fields"`
	Headers     []string `json:"headers,omitempty"`
}

// TableColumn defines a column in the history table.
type TableColumn struct {
	Key   string `json:"key"`
	Label string `json:"label"`
}

// TableAction defines a table action that maps to an HTTP endpoint.
type TableAction struct {
	Label       string `json:"label"`
	Method      string `json:"method"`
	URLTemplate string `json:"url_template"`
}

// Table defines a history widget.
type Table struct {
	ID      string        `json:"id"`
	Title   string        `json:"title"`
	DataURL string        `json:"data_url"`
	Columns []TableColumn `json:"columns"`
	Actions []TableAction `json:"actions,omitempty"`
	Headers []string      `json:"headers,omitempty"`
}

// Theme captures optional theme tokens for admin UI styling.
type Theme struct {
	Name   string            `json:"name"`
	Tokens map[string]string `json:"tokens"`
}

// UI bundles the widgets needed for export request and history views.
type UI struct {
	RequestForm Form  `json:"request_form"`
	History     Table `json:"history"`
	Theme       Theme `json:"theme"`
}

// DefaultUI returns a minimal formgen-style UI contract for exports.
func DefaultUI(basePath string) UI {
	basePath = strings.TrimRight(basePath, "/")
	if basePath == "" {
		basePath = "/admin/exports"
	}
	return UI{
		RequestForm: ExportRequestForm(basePath),
		History:     ExportHistoryTable(basePath),
		Theme:       DefaultTheme(),
	}
}

// ExportRequestForm builds a form definition for requesting exports.
func ExportRequestForm(basePath string) Form {
	return Form{
		ID:          "export-request",
		Title:       "Request Export",
		Action:      basePath,
		Method:      "POST",
		SubmitLabel: "Start Export",
		Fields: []Field{
			{Name: "definition", Label: "Definition", Type: "select", Required: true},
			{Name: "format", Label: "Format", Type: "select", Options: []string{"csv", "json", "ndjson", "xlsx"}},
			{Name: "query.search", Label: "Search", Type: "text", Hint: "Matches datagrid search"},
			{Name: "query.filters", Label: "Filters", Type: "json", Hint: "Datagrid filter payload"},
			{Name: "query.sort", Label: "Sort", Type: "json", Hint: "Datagrid sort payload"},
			{Name: "columns", Label: "Columns", Type: "multi-select"},
			{Name: "selection.mode", Label: "Selection", Type: "select", Options: []string{"all", "ids"}},
			{Name: "selection.ids", Label: "Selected IDs", Type: "text"},
			{Name: "estimated_rows", Label: "Estimated Rows", Type: "number"},
			{Name: "delivery", Label: "Delivery", Type: "select", Options: []string{"auto", "sync", "async"}},
		},
		Headers: DefaultAuthHeaders(),
	}
}

// ExportHistoryTable builds a table definition for export history.
func ExportHistoryTable(basePath string) Table {
	return Table{
		ID:      "export-history",
		Title:   "Export History",
		DataURL: basePath,
		Columns: []TableColumn{
			{Key: "ID", Label: "ID"},
			{Key: "Definition", Label: "Definition"},
			{Key: "Format", Label: "Format"},
			{Key: "State", Label: "Status"},
			{Key: "Counts.Processed", Label: "Rows"},
			{Key: "CreatedAt", Label: "Created"},
		},
		Actions: []TableAction{
			{Label: "Download", Method: "GET", URLTemplate: fmt.Sprintf("%s/{id}/download", basePath)},
			{Label: "Delete", Method: "DELETE", URLTemplate: fmt.Sprintf("%s/{id}", basePath)},
		},
		Headers: DefaultAuthHeaders(),
	}
}

// DefaultTheme provides a small theme token set for admin widgets.
func DefaultTheme() Theme {
	return Theme{
		Name: "go-export",
		Tokens: map[string]string{
			"primary":   "#2563eb",
			"surface":   "#ffffff",
			"text":      "#1f2937",
			"muted":     "#6b7280",
			"accent":    "#1d4ed8",
			"danger":    "#b91c1c",
			"success":   "#15803d",
			"border":    "#e5e7eb",
			"highlight": "#dbeafe",
		},
	}
}

// DefaultAuthHeaders lists headers commonly forwarded for x-auth propagation.
func DefaultAuthHeaders() []string {
	return []string{"X-Auth-User", "X-Auth-Roles", "X-Auth-Tenant", "X-Auth-Workspace"}
}
