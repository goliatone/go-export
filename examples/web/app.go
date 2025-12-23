package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/flosch/pongo2/v6"
	"github.com/goliatone/go-command/dispatcher"
	exportdelivery "github.com/goliatone/go-export/adapters/delivery"
	exportjob "github.com/goliatone/go-export/adapters/job"
	exportpdf "github.com/goliatone/go-export/adapters/pdf"
	exporttemplate "github.com/goliatone/go-export/adapters/template"
	"github.com/goliatone/go-export/examples"
	"github.com/goliatone/go-export/examples/web/config"
	"github.com/goliatone/go-export/export"
	"github.com/goliatone/go-export/export/notify"
	exportcallback "github.com/goliatone/go-export/sources/callback"
	exportcrud "github.com/goliatone/go-export/sources/crud"
	gojob "github.com/goliatone/go-job"
)

// App holds the application dependencies.
type App struct {
	Config         config.Config
	Logger         *SimpleLogger
	Service        export.Service
	Runner         *export.Runner
	Tracker        export.ProgressTracker
	Store          export.ArtifactStore
	Delivery       *exportdelivery.Service
	Scheduler      *exportjob.Scheduler
	GenerateTask   *exportjob.GenerateTask
	CancelRegistry *exportjob.CancelRegistry
	subscriptions  []dispatcher.Subscription
}

// NewApp creates and initializes the application.
func NewApp(ctx context.Context, cfg config.Config) (*App, error) {
	logger := &SimpleLogger{prefix: "go-export"}

	// Create artifact directory
	if err := os.MkdirAll(cfg.Export.ArtifactDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create artifact directory: %w", err)
	}

	// Initialize in-memory tracker
	tracker := export.NewMemoryTracker()

	// Initialize filesystem artifact store
	store := NewFSStore(cfg.Export.ArtifactDir)

	// Initialize row source registry with demo data
	sourceRegistry := export.NewRowSourceRegistry()
	registerDemoSources(sourceRegistry)

	// Initialize definition registry
	definitions := export.NewDefinitionRegistry()
	registerDemoDefinitions(definitions)

	// Initialize cancel registry for async job cancellation
	cancelRegistry := exportjob.NewCancelRegistry()

	// Create runner
	runner := export.NewRunner()
	runner.Definitions = definitions
	runner.RowSources = sourceRegistry
	runner.Tracker = tracker
	runner.Store = store
	runner.Logger = logger
	runner.Guard = &NoOpGuard{}

	// Initialize template and PDF renderers
	if err := registerTemplateRenderers(runner, cfg, logger); err != nil {
		return nil, fmt.Errorf("failed to register template renderers: %w", err)
	}

	// Create service with delivery policy
	deliveryPolicy := export.DeliveryPolicy{
		Default: export.DeliverySync,
	}
	if cfg.Export.EnableAsync && cfg.Export.MaxRows > 0 {
		deliveryPolicy.Thresholds.MaxRows = cfg.Export.MaxRows
	}
	if cfg.Export.EnableAsync && cfg.Export.Notifications.Enabled {
		deliveryPolicy.Default = export.DeliveryAsync
	}
	runner.DeliveryPolicy = deliveryPolicy

	baseService := export.NewService(export.ServiceConfig{
		Runner:         runner,
		Tracker:        tracker,
		Store:          store,
		Guard:          &NoOpGuard{},
		DeliveryPolicy: deliveryPolicy,
		CancelHook:     cancelRegistry,
	})
	service := baseService

	var notifier notify.ExportReadyNotifier
	if cfg.Export.Notifications.Enabled {
		setup, err := setupExportReadyNotifier(ctx, logger, cfg.Export.Notifications)
		if err != nil {
			return nil, fmt.Errorf("failed to setup notifications: %w", err)
		}
		notifier = setup
		if notifier != nil {
			baseURL := buildServerBaseURL(cfg.Server)
			service = newNotifyingService(service, store, notifier, cfg.Export.Notifications, logger, baseURL)
		}
	}

	// Register go-command handlers
	subscriptions, err := examples.RegisterExportHandlers(nil, service)
	if err != nil {
		return nil, fmt.Errorf("failed to register export handlers: %w", err)
	}

	// Create generate task for async exports
	generateTask := exportjob.NewGenerateTask(exportjob.TaskConfig{
		CancelRegistry: cancelRegistry,
		Store:          store,
		Logger:         logger,
		// Dispatch defaults to dispatcher.Dispatch
	})

	// Create scheduler with demo enqueuer that runs task in a goroutine
	// NOTE: In production, use a real job runner (e.g., go-job worker pool, Redis queue, etc.)
	var scheduler *exportjob.Scheduler
	if cfg.Export.EnableAsync {
		enqueuer := exportjob.EnqueuerFunc(func(ctx context.Context, msg *gojob.ExecutionMessage) error {
			go func() {
				const asyncExportTimeout = 10 * time.Minute
				// Demo safeguard: allow background exports to finish, but bound runtime.
				// In production, prefer a job runner with request-scoped cancellation/timeouts.
				execCtx, cancel := context.WithTimeout(context.Background(), asyncExportTimeout)
				defer cancel()

				if err := generateTask.Execute(execCtx, msg); err != nil {
					logger.Errorf("async export task failed: %v", err)
				}
			}()
			return nil
		})

		scheduler = exportjob.NewScheduler(exportjob.Config{
			Service:  service,
			Enqueuer: enqueuer,
			Tracker:  tracker,
			Logger:   logger,
		})
	}

	var delivery *exportdelivery.Service
	if notifier != nil {
		delivery = exportdelivery.NewService(exportdelivery.Config{
			Service:     baseService,
			Store:       store,
			EmailSender: logEmailSender{logger: logger},
			Logger:      logger,
			Notifier:    notifier,
		})
	}

	app := &App{
		Config:         cfg,
		Logger:         logger,
		Service:        service,
		Runner:         runner,
		Tracker:        tracker,
		Store:          store,
		Delivery:       delivery,
		Scheduler:      scheduler,
		GenerateTask:   generateTask,
		CancelRegistry: cancelRegistry,
		subscriptions:  subscriptions,
	}

	app.maybeSendDemoNotification(ctx)

	return app, nil
}

// Close releases app resources.
func (a *App) Close() error {
	for _, sub := range a.subscriptions {
		sub.Unsubscribe()
	}
	return nil
}

// SimpleLogger is a basic logger implementation.
type SimpleLogger struct {
	prefix string
}

func (l *SimpleLogger) Debugf(format string, args ...any) {
	fmt.Printf("[DEBUG] %s: %s\n", l.prefix, fmt.Sprintf(format, args...))
}

func (l *SimpleLogger) Infof(format string, args ...any) {
	fmt.Printf("[INFO] %s: %s\n", l.prefix, fmt.Sprintf(format, args...))
}

func (l *SimpleLogger) Errorf(format string, args ...any) {
	fmt.Printf("[ERROR] %s: %s\n", l.prefix, fmt.Sprintf(format, args...))
}

// NoOpGuard allows all operations.
type NoOpGuard struct{}

func (g *NoOpGuard) AuthorizeExport(ctx context.Context, actor export.Actor, req export.ExportRequest, def export.ResolvedDefinition) error {
	return nil
}

func (g *NoOpGuard) AuthorizeDownload(ctx context.Context, actor export.Actor, exportID string) error {
	return nil
}

// FSStore is a filesystem-based artifact store.
type FSStore struct {
	root string
	mu   sync.RWMutex
	meta map[string]export.ArtifactMeta
}

// NewFSStore creates a new filesystem artifact store.
func NewFSStore(root string) *FSStore {
	return &FSStore{
		root: root,
		meta: make(map[string]export.ArtifactMeta),
	}
}

func (s *FSStore) path(key string) string {
	return filepath.Join(s.root, key)
}

func (s *FSStore) Put(ctx context.Context, key string, r io.Reader, meta export.ArtifactMeta) (export.ArtifactRef, error) {
	path := s.path(key)

	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return export.ArtifactRef{}, fmt.Errorf("failed to create directory: %w", err)
	}

	f, err := os.Create(path)
	if err != nil {
		return export.ArtifactRef{}, fmt.Errorf("failed to create file: %w", err)
	}
	defer f.Close()

	n, err := io.Copy(f, r)
	if err != nil {
		return export.ArtifactRef{}, fmt.Errorf("failed to write file: %w", err)
	}

	meta.Size = n
	if meta.CreatedAt.IsZero() {
		meta.CreatedAt = time.Now()
	}

	s.mu.Lock()
	s.meta[key] = meta
	s.mu.Unlock()

	return export.ArtifactRef{
		Key:  key,
		Meta: meta,
	}, nil
}

func (s *FSStore) Open(ctx context.Context, key string) (io.ReadCloser, export.ArtifactMeta, error) {
	path := s.path(key)

	f, err := os.Open(path)
	if err != nil {
		return nil, export.ArtifactMeta{}, fmt.Errorf("failed to open file: %w", err)
	}

	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, export.ArtifactMeta{}, fmt.Errorf("failed to stat file: %w", err)
	}

	s.mu.RLock()
	meta, ok := s.meta[key]
	s.mu.RUnlock()

	if !ok {
		meta = export.ArtifactMeta{
			Size:      stat.Size(),
			CreatedAt: stat.ModTime(),
		}
	}

	return f, meta, nil
}

func (s *FSStore) Delete(ctx context.Context, key string) error {
	path := s.path(key)

	s.mu.Lock()
	delete(s.meta, key)
	s.mu.Unlock()

	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete file: %w", err)
	}
	return nil
}

func (s *FSStore) SignedURL(ctx context.Context, key string, ttl time.Duration) (string, error) {
	// For local development, just return the file path
	return s.path(key), nil
}

func buildServerBaseURL(cfg config.ServerConfig) string {
	host := strings.TrimSpace(cfg.Host)
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "localhost"
	}
	port := strings.TrimSpace(cfg.Port)
	if port == "" {
		return "http://" + host
	}
	return fmt.Sprintf("http://%s:%s", host, port)
}

// registerDemoSources adds demo row sources to the registry.
func registerDemoSources(registry *export.RowSourceRegistry) {
	// Users source
	usersSource := exportcrud.NewSource(UserStreamer{}, exportcrud.Config{PrimaryKey: "id"})
	registry.Register("users", func(req export.ExportRequest, def export.ResolvedDefinition) (export.RowSource, error) {
		return usersSource, nil
	})

	// Products source
	productsSource := exportcallback.NewSource(func(ctx context.Context, spec export.RowSourceSpec) (export.RowIterator, error) {
		return NewProductIterator(), nil
	})
	registry.Register("products", func(req export.ExportRequest, def export.ResolvedDefinition) (export.RowSource, error) {
		return productsSource, nil
	})

	// Orders source
	ordersSource := exportcallback.NewSource(func(ctx context.Context, spec export.RowSourceSpec) (export.RowIterator, error) {
		return NewOrderIterator(), nil
	})
	registry.Register("orders", func(req export.ExportRequest, def export.ResolvedDefinition) (export.RowSource, error) {
		return ordersSource, nil
	})
}

// registerDemoDefinitions adds demo export definitions to the registry.
func registerDemoDefinitions(registry *export.DefinitionRegistry) {
	// All formats including template and PDF
	allFormats := []export.Format{
		export.FormatCSV,
		export.FormatJSON,
		export.FormatNDJSON,
		export.FormatXLSX,
		export.FormatTemplate,
		export.FormatPDF,
	}

	// Default template options for exports
	defaultTemplate := export.TemplateOptions{
		TemplateName: "export",
		Layout:       "default",
		Data: map[string]any{
			"pdf_assets_host": export.DefaultPDFAssetsHost(),
		},
	}

	registry.Register(export.ExportDefinition{
		Name:            "users",
		Resource:        "users",
		AllowedFormats:  allFormats,
		DefaultFilename: "users-export",
		RowSourceKey:    "users",
		Template: export.TemplateOptions{
			TemplateName: "export",
			Layout:       "default",
			Title:        "Users Export",
			Data: map[string]any{
				"pdf_assets_host": export.DefaultPDFAssetsHost(),
			},
		},
		Schema: export.Schema{
			Columns: []export.Column{
				{Name: "id", Label: "ID", Type: "string"},
				{Name: "email", Label: "Email", Type: "string"},
				{Name: "name", Label: "Name", Type: "string"},
				{Name: "role", Label: "Role", Type: "string"},
				{Name: "created_at", Label: "Created At", Type: "datetime"},
			},
		},
	})

	registry.Register(export.ExportDefinition{
		Name:            "products",
		Resource:        "products",
		AllowedFormats:  allFormats,
		DefaultFilename: "products-export",
		RowSourceKey:    "products",
		Template: export.TemplateOptions{
			TemplateName: "export",
			Layout:       "default",
			Title:        "Products Export",
			Data: map[string]any{
				"pdf_assets_host": export.DefaultPDFAssetsHost(),
			},
		},
		Schema: export.Schema{
			Columns: []export.Column{
				{Name: "id", Label: "ID", Type: "string"},
				{Name: "name", Label: "Product Name", Type: "string"},
				{Name: "sku", Label: "SKU", Type: "string"},
				{Name: "price", Label: "Price", Type: "number"},
				{Name: "quantity", Label: "Quantity", Type: "integer"},
			},
		},
	})

	registry.Register(export.ExportDefinition{
		Name:            "orders",
		Resource:        "orders",
		AllowedFormats:  allFormats,
		DefaultFilename: "orders-export",
		RowSourceKey:    "orders",
		Template:        defaultTemplate,
		Schema: export.Schema{
			Columns: []export.Column{
				{Name: "id", Label: "Order ID", Type: "string"},
				{Name: "customer", Label: "Customer", Type: "string"},
				{Name: "total", Label: "Total", Type: "number"},
				{Name: "status", Label: "Status", Type: "string"},
				{Name: "created_at", Label: "Created At", Type: "datetime"},
			},
		},
	})
}

// registerTemplateRenderers initializes and registers template and PDF renderers.
func registerTemplateRenderers(runner *export.Runner, cfg config.Config, logger *SimpleLogger) error {
	if runner.Renderers == nil {
		runner.Renderers = export.NewRendererRegistry()
	}

	// Register default renderers if not already present
	runner.Renderers.Register(export.FormatCSV, export.CSVRenderer{})
	runner.Renderers.Register(export.FormatJSON, export.JSONRenderer{})
	runner.Renderers.Register(export.FormatNDJSON, export.JSONRenderer{})
	runner.Renderers.Register(export.FormatXLSX, export.XLSXRenderer{})

	// Initialize template renderer if enabled
	if cfg.Export.Template.Enabled {
		templateDir := cfg.Export.Template.TemplateDir
		if templateDir == "" {
			templateDir = "./templates/export"
		}

		// Create template directory if it doesn't exist
		if err := os.MkdirAll(templateDir, 0755); err != nil {
			return fmt.Errorf("failed to create template directory: %w", err)
		}

		// Initialize pongo2 template set with custom loader
		loader, err := pongo2.NewLocalFileSystemLoader(templateDir)
		if err != nil {
			return fmt.Errorf("failed to create template loader: %w", err)
		}
		templateSet := pongo2.NewSet("export", loader)

		// Register to_json filter
		if err := pongo2.RegisterFilter("to_json", toJSONFilter); err != nil {
			// Filter may already exist, ignore error
			logger.Debugf("to_json filter registration: %v", err)
		}

		executor := &Pongo2Executor{
			TemplateSet: templateSet,
			Extension:   ".html",
		}

		templateRenderer := exporttemplate.Renderer{
			Enabled:      true,
			Templates:    executor,
			TemplateName: cfg.Export.Template.TemplateName,
			Strategy:     exporttemplate.BufferedStrategy{MaxRows: cfg.Export.Template.MaxRows},
		}

		runner.Renderers.Register(export.FormatTemplate, templateRenderer)
		logger.Infof("Template renderer enabled (dir: %s, template: %s)", templateDir, cfg.Export.Template.TemplateName)

		// Initialize PDF renderer if enabled (requires template renderer)
		if cfg.Export.PDF.Enabled {
			engineName := strings.ToLower(strings.TrimSpace(cfg.Export.PDF.Engine))
			var pdfEngine exportpdf.Engine
			engineLabel := engineName

			switch engineName {
			case "", "chromium", "chromedp":
				pdfEngine = &exportpdf.ChromiumEngine{
					BrowserPath: cfg.Export.PDF.ChromiumPath,
					Headless:    cfg.Export.PDF.Headless,
					Timeout:     time.Duration(cfg.Export.PDF.Timeout) * time.Second,
					Args:        cfg.Export.PDF.Args,
					DefaultPDF: export.PDFOptions{
						PageSize:             cfg.Export.PDF.PageSize,
						PrintBackground:      boolPtr(cfg.Export.PDF.PrintBackground),
						PreferCSSPageSize:    boolPtr(cfg.Export.PDF.PreferCSSPageSize),
						Scale:                cfg.Export.PDF.Scale,
						MarginTop:            cfg.Export.PDF.MarginTop,
						MarginBottom:         cfg.Export.PDF.MarginBottom,
						MarginLeft:           cfg.Export.PDF.MarginLeft,
						MarginRight:          cfg.Export.PDF.MarginRight,
						BaseURL:              cfg.Export.PDF.BaseURL,
						ExternalAssetsPolicy: export.PDFExternalAssetsPolicy(cfg.Export.PDF.ExternalAssetsPolicy),
					},
				}
				engineLabel = "chromium"
			case "wkhtmltopdf":
				pdfEngine = exportpdf.WKHTMLTOPDFEngine{
					Command: cfg.Export.PDF.WKHTMLTOPDFPath,
					Timeout: time.Duration(cfg.Export.PDF.Timeout) * time.Second,
				}
				engineLabel = "wkhtmltopdf"
			default:
				return fmt.Errorf("unsupported pdf engine: %s", cfg.Export.PDF.Engine)
			}

			pdfRenderer := exportpdf.Renderer{
				Enabled:      true,
				HTMLRenderer: templateRenderer,
				Engine:       pdfEngine,
			}

			runner.Renderers.Register(export.FormatPDF, pdfRenderer)
			logger.Infof("PDF renderer enabled (engine: %s)", engineLabel)
		}
	}

	return nil
}

// Pongo2Executor adapts pongo2 to the TemplateExecutor interface.
type Pongo2Executor struct {
	TemplateSet *pongo2.TemplateSet
	Extension   string
}

// ExecuteTemplate renders a named template with the given data.
func (e *Pongo2Executor) ExecuteTemplate(w io.Writer, name string, data any) error {
	templateName := name
	if e.Extension != "" && filepath.Ext(name) == "" {
		templateName = name + e.Extension
	}

	tpl, err := e.TemplateSet.FromFile(templateName)
	if err != nil {
		return fmt.Errorf("failed to load template %q: %w", templateName, err)
	}

	ctx, err := toPongo2Context(data)
	if err != nil {
		return fmt.Errorf("failed to convert data to pongo2 context: %w", err)
	}

	return tpl.ExecuteWriter(ctx, w)
}

// toPongo2Context converts arbitrary data to a pongo2.Context.
func toPongo2Context(data any) (pongo2.Context, error) {
	if data == nil {
		return pongo2.Context{}, nil
	}

	// If it's already a map, use it directly
	if m, ok := data.(map[string]any); ok {
		return pongo2.Context(m), nil
	}
	if m, ok := data.(pongo2.Context); ok {
		return m, nil
	}
	if ctx, ok := mapFromStringKeyedMap(data); ok {
		return ctx, nil
	}
	if ctx, ok := mapFromStruct(data); ok {
		return ctx, nil
	}

	// Convert struct to map via JSON round-trip
	jsonBytes, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	var result map[string]any
	if err := json.Unmarshal(jsonBytes, &result); err != nil {
		return nil, err
	}

	return pongo2.Context(result), nil
}

func mapFromStruct(data any) (pongo2.Context, bool) {
	value := reflect.ValueOf(data)
	if !value.IsValid() {
		return pongo2.Context{}, true
	}
	if value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return pongo2.Context{}, true
		}
		value = value.Elem()
	}
	if value.Kind() != reflect.Struct {
		return nil, false
	}

	ctx := pongo2.Context{}
	addStructFields(ctx, value)
	return ctx, true
}

func addStructFields(ctx pongo2.Context, value reflect.Value) {
	typ := value.Type()
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if field.PkgPath != "" && !field.Anonymous {
			continue
		}

		fieldValue := value.Field(i)
		if field.Anonymous {
			if fieldValue.Kind() == reflect.Pointer {
				if fieldValue.IsNil() {
					continue
				}
				fieldValue = fieldValue.Elem()
			}
			if fieldValue.Kind() == reflect.Struct {
				addStructFields(ctx, fieldValue)
				continue
			}
		}

		if !fieldValue.CanInterface() {
			continue
		}
		ctx[field.Name] = fieldValue.Interface()

		if tag := field.Tag.Get("json"); tag != "" {
			name := strings.Split(tag, ",")[0]
			if name != "" && name != "-" {
				if _, exists := ctx[name]; !exists {
					ctx[name] = fieldValue.Interface()
				}
			}
		}
	}
}

func mapFromStringKeyedMap(data any) (pongo2.Context, bool) {
	value := reflect.ValueOf(data)
	if !value.IsValid() {
		return pongo2.Context{}, true
	}
	if value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return pongo2.Context{}, true
		}
		value = value.Elem()
	}
	if value.Kind() != reflect.Map || value.Type().Key().Kind() != reflect.String {
		return nil, false
	}

	ctx := pongo2.Context{}
	for _, key := range value.MapKeys() {
		val := value.MapIndex(key)
		if !val.IsValid() {
			continue
		}
		ctx[key.String()] = val.Interface()
	}
	return ctx, true
}

func boolPtr(value bool) *bool {
	return &value
}

// toJSONFilter is a pongo2 filter that converts data to JSON string.
func toJSONFilter(in *pongo2.Value, param *pongo2.Value) (*pongo2.Value, *pongo2.Error) {
	jsonBytes, err := json.Marshal(in.Interface())
	if err != nil {
		return nil, &pongo2.Error{
			Sender:    "filter:to_json",
			OrigError: err,
		}
	}
	return pongo2.AsValue(string(jsonBytes)), nil
}
