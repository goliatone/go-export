package gotemplate

import (
	"encoding/json"
	"errors"
	"io"
	"strings"

	exporttemplate "github.com/goliatone/go-export/adapters/template"
	template "github.com/goliatone/go-template"
)

// Executor adapts go-template's Renderer to go-export's TemplateExecutor.
type Executor struct {
	Renderer template.Renderer
}

var _ exporttemplate.TemplateExecutor = (*Executor)(nil)

// NewExecutor wraps a go-template Renderer for use in go-export.
func NewExecutor(renderer template.Renderer) *Executor {
	return &Executor{Renderer: renderer}
}

// ExecuteTemplate renders a named template into the provided writer.
func (e Executor) ExecuteTemplate(w io.Writer, name string, data any) error {
	if e.Renderer == nil {
		return errors.New("gotemplate executor requires renderer")
	}
	_, err := e.Renderer.Render(name, data, w)
	return err
}

// NewEngine creates a go-template engine with the to_json filter registered.
func NewEngine(opts ...template.Option) (*template.Engine, error) {
	engine, err := template.NewRenderer(opts...)
	if err != nil {
		return nil, err
	}
	if err := RegisterToJSON(engine); err != nil {
		return nil, err
	}
	return engine, nil
}

// RegisterToJSON registers a to_json filter for embedding JSON in templates.
func RegisterToJSON(engine *template.Engine) error {
	if engine == nil {
		return errors.New("gotemplate: engine is nil")
	}
	err := engine.RegisterFilter("to_json", func(input any, _ any) (any, error) {
		payload, err := json.Marshal(input)
		if err != nil {
			return "", err
		}
		return string(payload), nil
	})
	if err == nil {
		return nil
	}
	if strings.Contains(err.Error(), "already exists") {
		return nil
	}
	return err
}
