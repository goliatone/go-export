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
		Service:        a.Service,
		Runner:         a.Runner,
		Store:          a.Store,
		ActorProvider:  staticActorProvider{actor: a.getActor()},
		Logger:         a.Logger,
		AsyncRequester: a.Scheduler,
	})
	exportHandler.RegisterRoutes(r)

	// Inbox API + realtime stream
	wsConfig := router.DefaultWebSocketConfig()
	wsConfig.OnPreUpgrade = func(c router.Context) (router.UpgradeData, error) {
		return router.UpgradeData{
			"user_id": a.inboxUserID(c),
		}, nil
	}
	r.WebSocket("/ws/inbox", wsConfig, a.InboxSocket())
	r.Get("/api/inbox", a.ListInbox)
	r.Get("/api/inbox/badge", a.InboxBadge)
	r.Patch("/api/inbox/read", a.InboxMarkRead)
	r.Delete("/api/inbox/:id", a.InboxDismiss)

	// API for available definitions
	r.Get("/api/definitions", a.ListDefinitions)

	// Demo schedule delivery endpoint
	r.Post("/api/schedule/deliveries", a.RunScheduledDeliveries)
}
