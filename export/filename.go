package export

import (
	"bytes"
	"fmt"
	"strings"
	"text/template"
	"time"
)

type filenameData struct {
	Definition string
	Format     string
	Timestamp  string
	Date       string
	Resource   string
	Variant    string
}

func renderFilename(def ResolvedDefinition, req ExportRequest, now time.Time) (string, error) {
	name := def.DefaultFilename
	if name == "" {
		name = "{{.Definition}}_{{.Timestamp}}"
	}

	data := filenameData{
		Definition: def.Name,
		Format:     string(req.Format),
		Timestamp:  now.UTC().Format("20060102T150405Z"),
		Date:       now.UTC().Format("20060102"),
		Resource:   def.Resource,
		Variant:    def.Variant,
	}

	tmpl, err := template.New("filename").Parse(name)
	if err != nil {
		return "", err
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", err
	}

	result := strings.TrimSpace(buf.String())
	if result == "" {
		return "", fmt.Errorf("empty filename")
	}

	ext := string(req.Format)
	switch req.Format {
	case FormatTemplate:
		ext = "html"
	case FormatPDF:
		ext = "pdf"
	}
	if !strings.HasSuffix(strings.ToLower(result), "."+ext) {
		result = result + "." + ext
	}
	return result, nil
}
