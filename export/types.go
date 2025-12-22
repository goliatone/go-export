package export

import (
	"context"
	"io"
	"time"
)

// Format is the export output format.
type Format string

const (
	FormatCSV      Format = "csv"
	FormatJSON     Format = "json"
	FormatNDJSON   Format = "ndjson"
	FormatXLSX     Format = "xlsx"
	FormatTemplate Format = "template"
	FormatPDF      Format = "pdf"
)

// DeliveryMode describes how exports are delivered.
type DeliveryMode string

const (
	DeliverySync  DeliveryMode = "sync"
	DeliveryAsync DeliveryMode = "async"
	DeliveryAuto  DeliveryMode = "auto"
)

// SelectionMode describes how rows are selected.
type SelectionMode string

const (
	SelectionAll   SelectionMode = "all"
	SelectionIDs   SelectionMode = "ids"
	SelectionQuery SelectionMode = "query"
)

// SelectionQueryRef references a named selection query plus optional params.
type SelectionQueryRef struct {
	Name   string
	Params any
}

// Selection captures row selection intent.
type Selection struct {
	Mode  SelectionMode
	IDs   []string
	Query SelectionQueryRef
}

// SelectionPolicy supplies default selections for requests without one.
type SelectionPolicy interface {
	DefaultSelection(ctx context.Context, actor Actor, req ExportRequest, def ResolvedDefinition) (Selection, bool, error)
}

// SelectionPolicyFunc adapts a function to a SelectionPolicy.
type SelectionPolicyFunc func(ctx context.Context, actor Actor, req ExportRequest, def ResolvedDefinition) (Selection, bool, error)

func (f SelectionPolicyFunc) DefaultSelection(ctx context.Context, actor Actor, req ExportRequest, def ResolvedDefinition) (Selection, bool, error) {
	if f == nil {
		return Selection{}, false, nil
	}
	return f(ctx, actor, req, def)
}

// ExportRequest captures an export request.
type ExportRequest struct {
	Definition        string
	SourceVariant     string
	Format            Format
	Query             any
	Selection         Selection
	Columns           []string
	Locale            string
	Timezone          string
	Delivery          DeliveryMode
	IdempotencyKey    string
	EstimatedRows     int
	EstimatedBytes    int64
	EstimatedDuration time.Duration
	Output            io.Writer
	RenderOptions     RenderOptions
}

// ExportDefinition declares an exportable dataset.
type ExportDefinition struct {
	Name             string
	Resource         string
	Schema           Schema
	AllowedFormats   []Format
	DefaultFilename  string
	RowSourceKey     string
	Transformers     []TransformerConfig
	DefaultSelection Selection
	SelectionPolicy  SelectionPolicy
	SourceVariants   map[string]SourceVariant
	Policy           ExportPolicy
	DeliveryPolicy   *DeliveryPolicy
	Template         TemplateOptions
}

// SourceVariant allows alternate sources and policy overrides.
type SourceVariant struct {
	RowSourceKey    string
	AllowedFormats  []Format
	DefaultFilename string
	Transformers    []TransformerConfig
	Policy          *ExportPolicy
	Template        *TemplateOptions
}

// ExportPolicy enforces export limits and redaction.
type ExportPolicy struct {
	AllowedColumns []string
	RedactColumns  []string
	RedactionValue any
	MaxRows        int
	MaxBytes       int64
	MaxDuration    time.Duration
}

// DeliveryPolicy configures delivery selection thresholds.
type DeliveryPolicy struct {
	Default    DeliveryMode
	Thresholds DeliveryThresholds
}

// DeliveryThresholds drive auto delivery decisions.
type DeliveryThresholds struct {
	MaxRows     int
	MaxBytes    int64
	MaxDuration time.Duration
}

// Column defines a column in the export schema.
type Column struct {
	Name   string
	Label  string
	Type   string
	Format ColumnFormat
}

// ColumnFormat provides renderer-specific formatting hints.
type ColumnFormat struct {
	Layout string
	Number string
	Excel  string
}

// Schema defines the columns for a dataset.
type Schema struct {
	Columns []Column
}

// ExportCounts tracks row counts.
type ExportCounts struct {
	Processed int64
	Total     int64
	Errors    int64
}

// ExportState captures progress states.
type ExportState string

const (
	StateQueued     ExportState = "queued"
	StateRunning    ExportState = "running"
	StatePublishing ExportState = "publishing"
	StateCompleted  ExportState = "completed"
	StateFailed     ExportState = "failed"
	StateCanceled   ExportState = "canceled"
	StateDeleted    ExportState = "deleted"
)

// ExportRecord captures tracker state for an export.
type ExportRecord struct {
	ID           string
	Definition   string
	Format       Format
	State        ExportState
	RequestedBy  Actor
	Scope        Scope
	Request      ExportRequest `json:"-"`
	Counts       ExportCounts
	BytesWritten int64
	Artifact     ArtifactRef
	CreatedAt    time.Time
	StartedAt    time.Time
	CompletedAt  time.Time
	ExpiresAt    time.Time
}

// Actor identifies the requesting principal.
type Actor struct {
	ID      string
	Scope   Scope
	Roles   []string
	Details map[string]any
}

// Scope identifies tenant/workspace scope.
type Scope struct {
	TenantID    string
	WorkspaceID string
}

// ExportResult captures a completed export.
type ExportResult struct {
	ID       string
	Delivery DeliveryMode
	Format   Format
	Rows     int64
	Bytes    int64
	Filename string
	Artifact *ArtifactRef
}

// Row is a column-aligned record.
type Row []any

// RowSourceSpec is passed to RowSource.Open.
type RowSourceSpec struct {
	Definition ResolvedDefinition
	Request    ExportRequest
	Columns    []Column
	Actor      Actor
}

// RowSource provides row iterators for exports.
type RowSource interface {
	Open(ctx context.Context, spec RowSourceSpec) (RowIterator, error)
}

// RowIterator streams rows.
type RowIterator interface {
	Next(ctx context.Context) (Row, error)
	Close() error
}

// RowTransformer wraps an iterator with row-level transformations.
type RowTransformer interface {
	Wrap(ctx context.Context, in RowIterator, schema Schema) (RowIterator, Schema, error)
}

// BufferedTransformer collects rows to perform non-streaming transforms.
type BufferedTransformer interface {
	Process(ctx context.Context, rows RowIterator, schema Schema) ([]Row, Schema, error)
}

// Renderer writes rows to the destination.
type Renderer interface {
	Render(ctx context.Context, schema Schema, rows RowIterator, w io.Writer, opts RenderOptions) (RenderStats, error)
}

// RenderStats capture renderer output.
type RenderStats struct {
	Rows  int64
	Bytes int64
}

// JSONMode configures JSON rendering.
type JSONMode string

const (
	JSONModeArray  JSONMode = "array"
	JSONModeLines  JSONMode = "ndjson"
	JSONModeObject JSONMode = "object"
)

// CSVOptions configures CSV output.
type CSVOptions struct {
	IncludeHeaders bool
	Delimiter      rune
	HeadersSet     bool
}

// JSONOptions configures JSON output.
type JSONOptions struct {
	Mode JSONMode
}

// TemplateStrategy selects template rendering behavior.
type TemplateStrategy string

const (
	TemplateStrategyBuffered  TemplateStrategy = "buffered"
	TemplateStrategyStreaming TemplateStrategy = "streaming"
)

// TemplateOptions configures template rendering.
type TemplateOptions struct {
	Strategy     TemplateStrategy
	MaxRows      int
	TemplateName string
	Layout       string
	Title        string
	Definition   string
	GeneratedAt  time.Time
	ChartConfig  any
	Theme        map[string]any
	Header       map[string]any
	Footer       map[string]any
	Data         map[string]any
}

// XLSXOptions configures XLSX output.
type XLSXOptions struct {
	IncludeHeaders bool
	HeadersSet     bool
	SheetName      string
	MaxRows        int
	MaxBytes       int64
}

// PDFExternalAssetsPolicy controls how external assets are handled in PDF rendering.
type PDFExternalAssetsPolicy string

const (
	PDFExternalAssetsUnspecified PDFExternalAssetsPolicy = ""
	PDFExternalAssetsAllow       PDFExternalAssetsPolicy = "allow"
	PDFExternalAssetsBlock       PDFExternalAssetsPolicy = "block"
)

// PDFOptions configures PDF output for headless engines.
type PDFOptions struct {
	PageSize             string
	Landscape            *bool
	PrintBackground      *bool
	Scale                float64
	MarginTop            string
	MarginBottom         string
	MarginLeft           string
	MarginRight          string
	PreferCSSPageSize    *bool
	BaseURL              string
	ExternalAssetsPolicy PDFExternalAssetsPolicy
}

// FormatOptions configures locale/timezone formatting.
type FormatOptions struct {
	Locale   string
	Timezone string
}

// RenderOptions configures renderer behavior.
type RenderOptions struct {
	CSV      CSVOptions
	JSON     JSONOptions
	Template TemplateOptions
	XLSX     XLSXOptions
	PDF      PDFOptions
	Format   FormatOptions
}

// ArtifactMeta captures stored artifact metadata.
type ArtifactMeta struct {
	ContentType string
	Size        int64
	Filename    string
	CreatedAt   time.Time
	ExpiresAt   time.Time
}

// ArtifactRef references a stored artifact.
type ArtifactRef struct {
	Key  string
	Meta ArtifactMeta
}

// ArtifactStore stores export artifacts.
type ArtifactStore interface {
	Put(ctx context.Context, key string, r io.Reader, meta ArtifactMeta) (ArtifactRef, error)
	Open(ctx context.Context, key string) (io.ReadCloser, ArtifactMeta, error)
	Delete(ctx context.Context, key string) error
	SignedURL(ctx context.Context, key string, ttl time.Duration) (string, error)
}

// ProgressDelta indicates progress changes.
type ProgressDelta struct {
	Rows  int64
	Bytes int64
}

// ProgressTracker tracks export progress.
type ProgressTracker interface {
	Start(ctx context.Context, record ExportRecord) (string, error)
	Advance(ctx context.Context, id string, delta ProgressDelta, meta map[string]any) error
	SetState(ctx context.Context, id string, state ExportState, meta map[string]any) error
	Fail(ctx context.Context, id string, err error, meta map[string]any) error
	Complete(ctx context.Context, id string, meta map[string]any) error
	Status(ctx context.Context, id string) (ExportRecord, error)
	List(ctx context.Context, filter ProgressFilter) ([]ExportRecord, error)
}

// CancelHook allows adapters to cancel running exports.
type CancelHook interface {
	Cancel(ctx context.Context, exportID string) error
}

// ArtifactTracker updates stored artifact metadata.
type ArtifactTracker interface {
	SetArtifact(ctx context.Context, id string, ref ArtifactRef) error
}

// RecordUpdater updates records outside state transitions.
type RecordUpdater interface {
	Update(ctx context.Context, record ExportRecord) error
}

// RecordDeleter removes records from the tracker.
type RecordDeleter interface {
	Delete(ctx context.Context, id string) error
}

// ProgressFilter filters tracker lists.
type ProgressFilter struct {
	Definition string
	State      ExportState
	Since      time.Time
	Until      time.Time
}

// Guard enforces authorization.
type Guard interface {
	AuthorizeExport(ctx context.Context, actor Actor, req ExportRequest, def ResolvedDefinition) error
	AuthorizeDownload(ctx context.Context, actor Actor, exportID string) error
}

// ActorProvider extracts the actor from context.
type ActorProvider interface {
	FromContext(ctx context.Context) (Actor, error)
}

// Logger provides logging hooks.
type Logger interface {
	Debugf(format string, args ...any)
	Infof(format string, args ...any)
	Errorf(format string, args ...any)
}

// ChangeEvent describes lifecycle events.
type ChangeEvent struct {
	Name       string
	ExportID   string
	Definition string
	Format     Format
	Delivery   DeliveryMode
	Actor      Actor
	Timestamp  time.Time
	Metadata   map[string]any
}

// ChangeEmitter emits lifecycle events.
type ChangeEmitter interface {
	Emit(ctx context.Context, evt ChangeEvent) error
}

// RouterRegistrar provides optional route registration.
type RouterRegistrar interface {
	RegisterRoutes(router any)
}

// QuotaHook enforces limits beyond per-definition policy.
type QuotaHook interface {
	Allow(ctx context.Context, actor Actor, req ExportRequest, def ResolvedDefinition) error
}

// MetricsEvent describes lifecycle metrics.
type MetricsEvent struct {
	Name       string
	ExportID   string
	Definition string
	Format     Format
	Delivery   DeliveryMode
	Actor      Actor
	Rows       int64
	Bytes      int64
	Duration   time.Duration
	ErrorKind  ErrorKind
	Timestamp  time.Time
}

// MetricsHook emits metrics-friendly lifecycle observations.
type MetricsHook interface {
	Emit(ctx context.Context, evt MetricsEvent) error
}

// RetentionPolicy decides artifact TTLs.
type RetentionPolicy interface {
	TTL(ctx context.Context, actor Actor, req ExportRequest, def ResolvedDefinition) (time.Duration, error)
}

// ResolvedDefinition is a definition with variant overrides applied.
type ResolvedDefinition struct {
	ExportDefinition
	Variant string
}
