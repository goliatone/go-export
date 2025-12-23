package exportrouter

import (
	"github.com/goliatone/go-export/adapters/exportapi"
	"github.com/goliatone/go-export/export"
	"github.com/goliatone/go-router"
)

// Config configures the go-router adapter.
type Config = exportapi.Config

// Handler exposes export routes for go-router.
type Handler struct {
	controller *exportapi.Controller
}

// NewHandler creates a go-router handler.
func NewHandler(cfg Config) *Handler {
	return &Handler{controller: exportapi.NewController(cfg)}
}

// RegisterRoutes registers routes on a compatible go-router router.
func (h *Handler) RegisterRoutes(router any) {
	r, ok := router.(routeRegistrar)
	if !ok {
		return
	}
	base := h.basePath()
	history := h.historyPath()

	r.Post(base, h.Handle)
	r.Post(base+"/", h.Handle)
	r.Get(base, h.Handle)
	r.Get(base+"/", h.Handle)
	r.Get(base+"/:id", h.Handle)
	r.Get(base+"/:id/download", h.Handle)
	r.Get(base+"/:id/preview", h.Handle)
	r.Delete(base+"/:id", h.Handle)
	if history != "" {
		r.Get(history, h.Handle)
		r.Get(history+"/", h.Handle)
	}
}

// Handle executes the shared export workflow.
func (h *Handler) Handle(c router.Context) error {
	if c == nil {
		return nil
	}
	if h == nil || h.controller == nil {
		exportapi.WriteError(routerResponse{ctx: c}, export.NewError(export.KindInternal, "handler is nil", nil))
		return nil
	}
	h.controller.Serve(routerRequest{ctx: c}, routerResponse{ctx: c})
	return nil
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

type routeRegistrar interface {
	Get(path string, handler router.HandlerFunc, mw ...router.MiddlewareFunc) router.RouteInfo
	Post(path string, handler router.HandlerFunc, mw ...router.MiddlewareFunc) router.RouteInfo
	Delete(path string, handler router.HandlerFunc, mw ...router.MiddlewareFunc) router.RouteInfo
}
