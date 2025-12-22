package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/goliatone/go-command/dispatcher"
	"github.com/goliatone/go-export/adapters/job"
	"github.com/goliatone/go-export/examples"
	"github.com/goliatone/go-export/examples/web/config"
	"github.com/goliatone/go-export/export"
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

	// Create service with delivery policy
	deliveryPolicy := export.DeliveryPolicy{
		Default: export.DeliverySync,
	}
	if cfg.Export.EnableAsync && cfg.Export.MaxRows > 0 {
		deliveryPolicy.Thresholds.MaxRows = cfg.Export.MaxRows
	}
	runner.DeliveryPolicy = deliveryPolicy

	service := export.NewService(export.ServiceConfig{
		Runner:         runner,
		Tracker:        tracker,
		Store:          store,
		Guard:          &NoOpGuard{},
		DeliveryPolicy: deliveryPolicy,
		CancelHook:     cancelRegistry,
	})

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

	return &App{
		Config:         cfg,
		Logger:         logger,
		Service:        service,
		Runner:         runner,
		Tracker:        tracker,
		Store:          store,
		Scheduler:      scheduler,
		GenerateTask:   generateTask,
		CancelRegistry: cancelRegistry,
		subscriptions:  subscriptions,
	}, nil
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
	registry.Register(export.ExportDefinition{
		Name:            "users",
		Resource:        "users",
		AllowedFormats:  []export.Format{export.FormatCSV, export.FormatJSON, export.FormatNDJSON, export.FormatXLSX},
		DefaultFilename: "users-export",
		RowSourceKey:    "users",
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
		AllowedFormats:  []export.Format{export.FormatCSV, export.FormatJSON, export.FormatNDJSON, export.FormatXLSX},
		DefaultFilename: "products-export",
		RowSourceKey:    "products",
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
		AllowedFormats:  []export.Format{export.FormatCSV, export.FormatJSON, export.FormatNDJSON, export.FormatXLSX},
		DefaultFilename: "orders-export",
		RowSourceKey:    "orders",
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
