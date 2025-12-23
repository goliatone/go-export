package exporthttp

import (
	"net/http"

	"github.com/goliatone/go-export/adapters/exportapi"
	"github.com/goliatone/go-export/export"
)

// Config configures the HTTP adapter.
type Config = exportapi.Config

// Handler exposes export HTTP endpoints.
type Handler struct {
	controller *exportapi.Controller
}

// NewHandler creates a new HTTP handler.
func NewHandler(cfg Config) *Handler {
	return &Handler{controller: exportapi.NewController(cfg)}
}

// RegisterRoutes registers handlers on a compatible router.
func (h *Handler) RegisterRoutes(router any) {
	switch r := router.(type) {
	case interface{ Handle(string, http.Handler) }:
		r.Handle(h.basePath(), h)
		r.Handle(h.basePath()+"/", h)
		if history := h.historyPath(); history != "" {
			r.Handle(history, h)
			r.Handle(history+"/", h)
		}
	case interface {
		HandleFunc(string, func(http.ResponseWriter, *http.Request))
	}:
		r.HandleFunc(h.basePath(), h.ServeHTTP)
		r.HandleFunc(h.basePath()+"/", h.ServeHTTP)
		if history := h.historyPath(); history != "" {
			r.HandleFunc(history, h.ServeHTTP)
			r.HandleFunc(history+"/", h.ServeHTTP)
		}
	}
}

// ServeHTTP routes export endpoints.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if w == nil {
		return
	}
	if h == nil || h.controller == nil {
		exportapi.WriteError(httpResponse{w: w}, export.NewError(export.KindInternal, "handler is nil", nil))
		return
	}
	h.controller.Serve(httpRequest{r: r}, httpResponse{w: w, req: r})
}

func (h *Handler) basePath() string {
	if h == nil || h.controller == nil {
		return "/admin/exports"
	}
	path := h.controller.BasePath()
	if path == "" {
		return "/admin/exports"
	}
	return path
}

func (h *Handler) historyPath() string {
	if h == nil || h.controller == nil {
		return "/admin/exports/history"
	}
	path := h.controller.HistoryPath()
	if path == "" {
		return "/admin/exports/history"
	}
	return path
}
