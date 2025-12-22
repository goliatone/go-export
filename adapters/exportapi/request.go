package exportapi

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"time"

	"github.com/goliatone/go-export/export"
)

// Request provides minimal request access for transport adapters.
type Request interface {
	Context() context.Context
	Method() string
	Path() string
	Header(name string) string
	Query(name string) string
	Body() io.ReadCloser
}

// RequestDecoder parses an HTTP request into an export request.
type RequestDecoder interface {
	Decode(req Request) (export.ExportRequest, error)
}

// QueryDecoder converts raw JSON query payloads into typed values.
type QueryDecoder func(definition, variant string, raw json.RawMessage) (any, error)

// JSONRequestDecoder decodes JSON into export requests.
type JSONRequestDecoder struct {
	QueryDecoder QueryDecoder
}

// Decode decodes a JSON request body into an export request.
func (d JSONRequestDecoder) Decode(req Request) (export.ExportRequest, error) {
	if req == nil {
		return export.ExportRequest{}, export.NewError(export.KindInternal, "request is nil", nil)
	}
	body := req.Body()
	if body == nil {
		return export.ExportRequest{}, export.NewError(export.KindValidation, "request body is required", nil)
	}
	defer body.Close()

	payload, err := decodePayload(body)
	if err != nil {
		return export.ExportRequest{}, err
	}

	query := any(nil)
	if len(payload.Query) > 0 {
		if d.QueryDecoder != nil {
			query, err = d.QueryDecoder(payload.Definition, payload.SourceVariant, payload.Query)
			if err != nil {
				return export.ExportRequest{}, err
			}
		} else {
			if err := json.Unmarshal(payload.Query, &query); err != nil {
				return export.ExportRequest{}, export.NewError(export.KindValidation, "invalid query", err)
			}
		}
	}

	reqModel := export.ExportRequest{
		Definition:        payload.Definition,
		SourceVariant:     payload.SourceVariant,
		Format:            normalizeFormat(payload.Format),
		Query:             query,
		Selection:         payload.Selection.toSelection(),
		Columns:           payload.Columns,
		Locale:            payload.Locale,
		Timezone:          payload.Timezone,
		Delivery:          payload.Delivery,
		IdempotencyKey:    payload.IdempotencyKey,
		EstimatedRows:     payload.EstimatedRows,
		EstimatedBytes:    payload.EstimatedBytes,
		EstimatedDuration: payload.EstimatedDuration.Duration,
		RenderOptions:     payload.RenderOptions.toRenderOptions(),
	}

	return reqModel, nil
}

func normalizeFormat(format export.Format) export.Format {
	normalized := strings.ToLower(strings.TrimSpace(string(format)))
	switch normalized {
	case "excel", "xls":
		return export.FormatXLSX
	default:
		return export.Format(normalized)
	}
}

type requestPayload struct {
	Definition        string               `json:"definition"`
	SourceVariant     string               `json:"source_variant,omitempty"`
	Format            export.Format        `json:"format,omitempty"`
	Query             json.RawMessage      `json:"query,omitempty"`
	Selection         selectionPayload     `json:"selection,omitempty"`
	Columns           []string             `json:"columns,omitempty"`
	Locale            string               `json:"locale,omitempty"`
	Timezone          string               `json:"timezone,omitempty"`
	Delivery          export.DeliveryMode  `json:"delivery,omitempty"`
	IdempotencyKey    string               `json:"idempotency_key,omitempty"`
	EstimatedRows     int                  `json:"estimated_rows,omitempty"`
	EstimatedBytes    int64                `json:"estimated_bytes,omitempty"`
	EstimatedDuration durationValue        `json:"estimated_duration,omitempty"`
	RenderOptions     renderOptionsPayload `json:"render_options,omitempty"`
}

type selectionPayload struct {
	Mode  export.SelectionMode   `json:"mode,omitempty"`
	IDs   []string               `json:"ids,omitempty"`
	Query *selectionQueryPayload `json:"query,omitempty"`
}

func (p selectionPayload) toSelection() export.Selection {
	selection := export.Selection{Mode: p.Mode, IDs: p.IDs}
	if p.Query != nil {
		selection.Query = export.SelectionQueryRef{
			Name:   p.Query.Name,
			Params: p.Query.Params,
		}
	}
	return selection
}

type selectionQueryPayload struct {
	Name   string         `json:"name"`
	Params map[string]any `json:"params,omitempty"`
}

type renderOptionsPayload struct {
	CSV      csvOptionsPayload      `json:"csv,omitempty"`
	JSON     jsonOptionsPayload     `json:"json,omitempty"`
	Template templateOptionsPayload `json:"template,omitempty"`
	XLSX     xlsxOptionsPayload     `json:"xlsx,omitempty"`
	Format   formatOptionsPayload   `json:"format,omitempty"`
}

func (p renderOptionsPayload) toRenderOptions() export.RenderOptions {
	return export.RenderOptions{
		CSV: export.CSVOptions{
			IncludeHeaders: p.CSV.IncludeHeaders,
			Delimiter:      p.CSV.Delimiter,
			HeadersSet:     p.CSV.HeadersSet,
		},
		JSON: export.JSONOptions{
			Mode: p.JSON.Mode,
		},
		Template: export.TemplateOptions{
			Strategy:     p.Template.Strategy,
			MaxRows:      p.Template.MaxRows,
			TemplateName: p.Template.TemplateName,
			Layout:       p.Template.Layout,
			Title:        p.Template.Title,
			Definition:   p.Template.Definition,
			GeneratedAt:  p.Template.GeneratedAt,
			ChartConfig:  p.Template.ChartConfig,
			Theme:        p.Template.Theme,
			Header:       p.Template.Header,
			Footer:       p.Template.Footer,
			Data:         p.Template.Data,
		},
		XLSX: export.XLSXOptions{
			IncludeHeaders: p.XLSX.IncludeHeaders,
			HeadersSet:     p.XLSX.HeadersSet,
			SheetName:      p.XLSX.SheetName,
			MaxRows:        p.XLSX.MaxRows,
			MaxBytes:       p.XLSX.MaxBytes,
		},
		Format: export.FormatOptions{
			Locale:   p.Format.Locale,
			Timezone: p.Format.Timezone,
		},
	}
}

type csvOptionsPayload struct {
	IncludeHeaders bool `json:"include_headers,omitempty"`
	Delimiter      rune `json:"delimiter,omitempty"`
	HeadersSet     bool `json:"headers_set,omitempty"`
}

type jsonOptionsPayload struct {
	Mode export.JSONMode `json:"mode,omitempty"`
}

type templateOptionsPayload struct {
	Strategy     export.TemplateStrategy `json:"strategy,omitempty"`
	MaxRows      int                     `json:"max_rows,omitempty"`
	TemplateName string                  `json:"template_name,omitempty"`
	Layout       string                  `json:"layout,omitempty"`
	Title        string                  `json:"title,omitempty"`
	Definition   string                  `json:"definition,omitempty"`
	GeneratedAt  time.Time               `json:"generated_at,omitempty"`
	ChartConfig  any                     `json:"chart_config,omitempty"`
	Theme        map[string]any          `json:"theme,omitempty"`
	Header       map[string]any          `json:"header,omitempty"`
	Footer       map[string]any          `json:"footer,omitempty"`
	Data         map[string]any          `json:"data,omitempty"`
}

type xlsxOptionsPayload struct {
	IncludeHeaders bool   `json:"include_headers,omitempty"`
	HeadersSet     bool   `json:"headers_set,omitempty"`
	SheetName      string `json:"sheet_name,omitempty"`
	MaxRows        int    `json:"max_rows,omitempty"`
	MaxBytes       int64  `json:"max_bytes,omitempty"`
}

type formatOptionsPayload struct {
	Locale   string `json:"locale,omitempty"`
	Timezone string `json:"timezone,omitempty"`
}

type durationValue struct {
	time.Duration
}

func (d *durationValue) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || string(data) == "null" {
		return nil
	}
	var asString string
	if err := json.Unmarshal(data, &asString); err == nil {
		if asString == "" {
			return nil
		}
		parsed, err := time.ParseDuration(asString)
		if err != nil {
			return err
		}
		d.Duration = parsed
		return nil
	}

	var asNumber float64
	if err := json.Unmarshal(data, &asNumber); err == nil {
		d.Duration = time.Duration(asNumber * float64(time.Second))
		return nil
	}

	return export.NewError(export.KindValidation, "invalid duration", nil)
}

func decodePayload(body io.Reader) (requestPayload, error) {
	var payload requestPayload
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		return requestPayload{}, export.NewError(export.KindValidation, "invalid request payload", err)
	}
	return payload, nil
}
