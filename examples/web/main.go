package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/goliatone/go-export/examples/web/config"
	"github.com/goliatone/go-router"
)

func main() {
	ctx := context.Background()

	// Load configuration
	cfg := config.Defaults()

	// Override from environment
	if port := os.Getenv("PORT"); port != "" {
		cfg.Server.Port = port
	}
	if host := os.Getenv("HOST"); host != "" {
		cfg.Server.Host = host
	}
	if artifactDir := os.Getenv("ARTIFACT_DIR"); artifactDir != "" {
		cfg.Export.ArtifactDir = artifactDir
	}

	// Create application
	app, err := NewApp(ctx, cfg)
	if err != nil {
		log.Fatalf("failed to create app: %v", err)
	}
	defer app.Close()

	// Build server
	srv, err := buildServer(app)
	if err != nil {
		log.Fatalf("failed to build server: %v", err)
	}

	// Setup routes
	app.SetupRoutes(srv.Router())

	// Start server
	addr := fmt.Sprintf("%s:%s", cfg.Server.Host, cfg.Server.Port)
	go func() {
		log.Printf("Starting server on http://%s", addr)
		log.Printf("Export API: http://%s/admin/exports", addr)
		log.Printf("Demo UI: http://%s/", addr)
		if err := srv.Serve(addr); err != nil {
			log.Fatalf("server error: %v", err)
		}
	}()

	// Wait for shutdown signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("shutdown error: %v", err)
	}
}

func buildServer(app *App) (router.Server[*fiber.App], error) {
	// Create view engine
	viewCfg := router.NewSimpleViewConfig("./views").
		WithExt(".html").
		WithReload(true).
		WithDebug(true)

	engine, err := router.InitializeViewEngine(viewCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize view engine: %w", err)
	}

	// Create Fiber adapter
	srv := router.NewFiberAdapter(fiberAppInitializer(engine))

	return srv, nil
}

func fiberAppInitializer(engine fiber.Views) func(*fiber.App) *fiber.App {
	return func(*fiber.App) *fiber.App {
		fiberApp := fiber.New(fiber.Config{
			AppName:           "Go Export Demo",
			PassLocalsToViews: true,
			Views:             engine,
			EnablePrintRoutes: true,
		})

		// Add middleware
		fiberApp.Use(logger.New(logger.Config{
			Format: "[${time}] ${status} ${method} ${path} ${latency}\n",
		}))
		fiberApp.Use(cors.New(cors.Config{
			AllowOrigins: "*",
			AllowMethods: "GET,POST,DELETE,OPTIONS",
			AllowHeaders: "Content-Type,Authorization,X-Idempotency-Key",
		}))

		return fiberApp
	}
}
