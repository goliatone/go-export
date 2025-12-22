package main

import (
	"github.com/gofiber/fiber/v2"
	exportrouter "github.com/goliatone/go-export/adapters/router"
	"github.com/goliatone/go-export/export"
	"github.com/goliatone/go-router"
)

// SetupRoutes registers all application routes.
func (a *App) SetupRoutes(r router.Router[*fiber.App]) {
	// Static assets
	r.Static("/public", "./public")
	r.Static(export.DefaultPDFAssetsPath, "", router.Static{
		FS: export.PDFAssetsFS(),
	})

	// Home page
	r.Get("/", a.renderHome())

	// Export API endpoints
	exportHandler := exportrouter.NewHandler(exportrouter.Config{
		Service:       a.Service,
		Runner:        a.Runner,
		Store:         a.Store,
		ActorProvider: staticActorProvider{actor: a.getActor()},
		Logger:        a.Logger,
	})
	exportHandler.RegisterRoutes(r)

	// API for available definitions
	r.Get("/api/definitions", a.ListDefinitions)
}
