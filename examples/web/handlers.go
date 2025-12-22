package main

import (
	"context"

	"github.com/goliatone/go-export/export"
	"github.com/goliatone/go-router"
)

// renderHome renders the home page.
func (a *App) renderHome() router.HandlerFunc {
	return func(c router.Context) error {
		return c.Render("home", router.ViewContext{
			"title":       "Go Export Demo",
			"definitions": []string{"users", "products", "orders"},
			"formats":     []string{"csv", "json", "ndjson", "xlsx"},
			"max_rows":    a.Config.Export.MaxRows,
		})
	}
}

// ListDefinitions handles GET /api/definitions.
func (a *App) ListDefinitions(c router.Context) error {
	definitions := []map[string]any{
		{
			"name":     "users",
			"resource": "users",
			"formats":  []string{"csv", "json", "ndjson", "xlsx"},
			"columns":  []string{"id", "email", "name", "role", "created_at"},
		},
		{
			"name":     "products",
			"resource": "products",
			"formats":  []string{"csv", "json", "ndjson", "xlsx"},
			"columns":  []string{"id", "name", "sku", "price", "quantity"},
		},
		{
			"name":     "orders",
			"resource": "orders",
			"formats":  []string{"csv", "json", "ndjson", "xlsx"},
			"columns":  []string{"id", "customer", "total", "status", "created_at"},
		},
	}

	return c.JSON(200, map[string]any{
		"definitions": definitions,
	})
}

// getActor returns a demo actor for all requests.
func (a *App) getActor() export.Actor {
	return export.Actor{
		ID: "demo-user",
		Scope: export.Scope{
			TenantID: "demo-tenant",
		},
	}
}

type staticActorProvider struct {
	actor export.Actor
}

func (p staticActorProvider) FromContext(ctx context.Context) (export.Actor, error) {
	_ = ctx
	return p.actor, nil
}
