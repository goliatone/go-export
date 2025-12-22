package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
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

	// Template config overrides
	if templateEnabled := os.Getenv("EXPORT_TEMPLATE_ENABLED"); templateEnabled == "true" {
		cfg.Export.Template.Enabled = true
	} else if templateEnabled == "false" {
		cfg.Export.Template.Enabled = false
	}
	if templateDir := os.Getenv("EXPORT_TEMPLATE_DIR"); templateDir != "" {
		cfg.Export.Template.TemplateDir = templateDir
	}
	if templateName := os.Getenv("EXPORT_TEMPLATE_NAME"); templateName != "" {
		cfg.Export.Template.TemplateName = templateName
	}

	// PDF config overrides
	if pdfEnabled := os.Getenv("EXPORT_PDF_ENABLED"); pdfEnabled == "true" {
		cfg.Export.PDF.Enabled = true
	} else if pdfEnabled == "false" {
		cfg.Export.PDF.Enabled = false
	}
	if pdfEngine := os.Getenv("EXPORT_PDF_ENGINE"); pdfEngine != "" {
		cfg.Export.PDF.Engine = pdfEngine
	}
	if wkhtmlPath := os.Getenv("EXPORT_WKHTMLTOPDF_PATH"); wkhtmlPath != "" {
		cfg.Export.PDF.WKHTMLTOPDFPath = wkhtmlPath
	}
	if chromiumPath := os.Getenv("EXPORT_PDF_CHROMIUM_PATH"); chromiumPath != "" {
		cfg.Export.PDF.ChromiumPath = chromiumPath
	}
	if headless := os.Getenv("EXPORT_PDF_HEADLESS"); headless != "" {
		if parsed, err := strconv.ParseBool(headless); err == nil {
			cfg.Export.PDF.Headless = parsed
		}
	}
	if args := os.Getenv("EXPORT_PDF_CHROMIUM_ARGS"); args != "" {
		cfg.Export.PDF.Args = splitCSV(args)
	}
	if pdfTimeout := os.Getenv("EXPORT_PDF_TIMEOUT"); pdfTimeout != "" {
		if t, err := strconv.Atoi(pdfTimeout); err == nil && t > 0 {
			cfg.Export.PDF.Timeout = t
		}
	}
	if pageSize := os.Getenv("EXPORT_PDF_PAGE_SIZE"); pageSize != "" {
		cfg.Export.PDF.PageSize = pageSize
	}
	if printBg := os.Getenv("EXPORT_PDF_PRINT_BACKGROUND"); printBg != "" {
		if parsed, err := strconv.ParseBool(printBg); err == nil {
			cfg.Export.PDF.PrintBackground = parsed
		}
	}
	if preferCSS := os.Getenv("EXPORT_PDF_PREFER_CSS_PAGE_SIZE"); preferCSS != "" {
		if parsed, err := strconv.ParseBool(preferCSS); err == nil {
			cfg.Export.PDF.PreferCSSPageSize = parsed
		}
	}
	if scale := os.Getenv("EXPORT_PDF_SCALE"); scale != "" {
		if parsed, err := strconv.ParseFloat(scale, 64); err == nil {
			cfg.Export.PDF.Scale = parsed
		}
	}
	if marginTop := os.Getenv("EXPORT_PDF_MARGIN_TOP"); marginTop != "" {
		cfg.Export.PDF.MarginTop = marginTop
	}
	if marginBottom := os.Getenv("EXPORT_PDF_MARGIN_BOTTOM"); marginBottom != "" {
		cfg.Export.PDF.MarginBottom = marginBottom
	}
	if marginLeft := os.Getenv("EXPORT_PDF_MARGIN_LEFT"); marginLeft != "" {
		cfg.Export.PDF.MarginLeft = marginLeft
	}
	if marginRight := os.Getenv("EXPORT_PDF_MARGIN_RIGHT"); marginRight != "" {
		cfg.Export.PDF.MarginRight = marginRight
	}
	if baseURL := os.Getenv("EXPORT_PDF_BASE_URL"); baseURL != "" {
		cfg.Export.PDF.BaseURL = baseURL
	}
	if policy := os.Getenv("EXPORT_PDF_EXTERNAL_ASSETS_POLICY"); policy != "" {
		cfg.Export.PDF.ExternalAssetsPolicy = policy
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

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		out = append(out, part)
	}
	return out
}

func buildServer(app *App) (router.Server[*fiber.App], error) {
	// Create view engine
	viewCfg := router.NewSimpleViewConfig("./views").
		WithExt(".html").
		WithReload(true).
		WithDebug(true).
		WithFunctions(templateFunctions())

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

func templateFunctions() map[string]any {
	return map[string]any{
		"to_json": func(data any) string {
			payload, err := json.Marshal(data)
			if err != nil {
				return ""
			}
			return string(payload)
		},
	}
}
