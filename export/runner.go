package export

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"time"
)

// Runner orchestrates export execution.
type Runner struct {
	Definitions    *DefinitionRegistry
	RowSources     *RowSourceRegistry
	Renderers      *RendererRegistry
	Tracker        ProgressTracker
	Store          ArtifactStore
	Guard          Guard
	ActorProvider  ActorProvider
	Logger         Logger
	Emitter        ChangeEmitter
	QuotaHook      QuotaHook
	Retention      RetentionPolicy
	DeliveryPolicy DeliveryPolicy
	Now            func() time.Time
	IDGenerator    func() string
}

// NewRunner creates a runner with default registries.
func NewRunner() *Runner {
	renderers := NewRendererRegistry()
	_ = renderers.Register(FormatCSV, CSVRenderer{})
	_ = renderers.Register(FormatJSON, JSONRenderer{})
	_ = renderers.Register(FormatNDJSON, JSONRenderer{})

	return &Runner{
		Definitions: NewDefinitionRegistry(),
		RowSources:  NewRowSourceRegistry(),
		Renderers:   renderers,
		Logger:      NopLogger{},
		Now:         time.Now,
		IDGenerator: defaultIDGenerator(),
	}
}

// Run executes an export request.
func (r *Runner) Run(ctx context.Context, req ExportRequest) (ExportResult, error) {
	if r == nil {
		return ExportResult{}, AsGoError(NewError(KindInternal, "runner is nil", nil))
	}
	if r.Definitions == nil || r.RowSources == nil || r.Renderers == nil {
		return ExportResult{}, AsGoError(NewError(KindInternal, "runner registries are not configured", nil))
	}
	if r.Now == nil {
		r.Now = time.Now
	}
	if r.Logger == nil {
		r.Logger = NopLogger{}
	}
	if r.IDGenerator == nil {
		r.IDGenerator = defaultIDGenerator()
	}

	def, err := r.Definitions.Resolve(req)
	if err != nil {
		return ExportResult{}, AsGoError(err)
	}

	resolved, err := ResolveExport(req, def, r.Now())
	if err != nil {
		return ExportResult{}, AsGoError(err)
	}

	if resolved.Request.Output == nil {
		return ExportResult{}, AsGoError(NewError(KindValidation, "output writer is required", nil))
	}

	delivery := SelectDelivery(resolved.Request, resolved.Definition, r.DeliveryPolicy)
	if delivery == DeliveryAsync {
		return ExportResult{}, AsGoError(NewError(KindNotImpl, "async delivery not supported", nil))
	}

	actor := Actor{}
	if r.ActorProvider != nil {
		actor, err = r.ActorProvider.FromContext(ctx)
		if err != nil {
			return ExportResult{}, AsGoError(NewError(KindAuthz, "failed to resolve actor", err))
		}
	}

	if r.Guard != nil {
		if err := r.Guard.AuthorizeExport(ctx, actor, resolved.Request, resolved.Definition); err != nil {
			return ExportResult{}, AsGoError(NewError(KindAuthz, "export not authorized", err))
		}
	}

	if r.QuotaHook != nil {
		if err := r.QuotaHook.Allow(ctx, resolved.Request, resolved.Definition); err != nil {
			return ExportResult{}, AsGoError(err)
		}
	}

	exportID := r.IDGenerator()
	if r.Tracker != nil {
		record := ExportRecord{
			ID:          exportID,
			Definition:  resolved.Definition.Name,
			Format:      resolved.Request.Format,
			State:       StateQueued,
			RequestedBy: actor,
			Scope:       actor.Scope,
			CreatedAt:   r.Now(),
		}
		id, err := r.Tracker.Start(ctx, record)
		if err != nil {
			return ExportResult{}, AsGoError(err)
		}
		if id != "" {
			exportID = id
		}
		_ = r.Tracker.SetState(ctx, exportID, StateRunning, nil)
	}

	r.emit(ctx, "export.requested", exportID, nil)
	r.emit(ctx, "export.started", exportID, nil)

	factory, ok := r.RowSources.Resolve(resolved.Definition.RowSourceKey)
	if !ok {
		err := NewError(KindNotFound, fmt.Sprintf("row source %q not registered", resolved.Definition.RowSourceKey), nil)
		r.fail(ctx, exportID, err)
		return ExportResult{}, AsGoError(err)
	}

	source, err := factory(resolved.Request, resolved.Definition)
	if err != nil {
		r.fail(ctx, exportID, err)
		return ExportResult{}, AsGoError(err)
	}

	iterator, err := source.Open(ctx, RowSourceSpec{
		Definition: resolved.Definition,
		Request:    resolved.Request,
		Columns:    resolved.Columns,
		Actor:      actor,
	})
	if err != nil {
		r.fail(ctx, exportID, err)
		return ExportResult{}, AsGoError(err)
	}
	defer iterator.Close()

	tracked := newTrackingIterator(iterator, resolved, r.Tracker, exportID)

	renderer, ok := r.Renderers.Resolve(resolved.Request.Format)
	if !ok {
		err := NewError(KindNotFound, fmt.Sprintf("renderer %q not registered", resolved.Request.Format), nil)
		r.fail(ctx, exportID, err)
		return ExportResult{}, AsGoError(err)
	}

	stats, err := renderer.Render(ctx, Schema{Columns: resolved.Columns}, tracked, resolved.Request.Output, resolved.Request.RenderOptions)
	if err != nil {
		r.fail(ctx, exportID, err)
		return ExportResult{}, AsGoError(err)
	}

	result := ExportResult{
		ID:       exportID,
		Delivery: delivery,
		Format:   resolved.Request.Format,
		Rows:     stats.Rows,
		Bytes:    stats.Bytes,
		Filename: resolved.Filename,
	}

	if r.Tracker != nil {
		_ = r.Tracker.Complete(ctx, exportID, map[string]any{
			"rows":  stats.Rows,
			"bytes": stats.Bytes,
		})
	}

	r.emit(ctx, "export.completed", exportID, map[string]any{
		"rows":  stats.Rows,
		"bytes": stats.Bytes,
	})

	return result, nil
}

func (r *Runner) fail(ctx context.Context, exportID string, err error) {
	if exportID == "" {
		return
	}

	if errors.Is(err, context.Canceled) {
		if r.Tracker != nil {
			_ = r.Tracker.SetState(ctx, exportID, StateCanceled, nil)
		}
		r.emit(ctx, "export.canceled", exportID, nil)
		return
	}

	if r.Tracker != nil {
		_ = r.Tracker.Fail(ctx, exportID, err, nil)
	}
	r.emit(ctx, "export.failed", exportID, map[string]any{"error": err.Error()})
}

func (r *Runner) emit(ctx context.Context, name, exportID string, meta map[string]any) {
	if r.Emitter == nil {
		return
	}
	_ = r.Emitter.Emit(ctx, ChangeEvent{
		Name:      name,
		ExportID:  exportID,
		Timestamp: r.Now(),
		Metadata:  meta,
	})
}

// NopLogger is a no-op logger.
type NopLogger struct{}

func (NopLogger) Debugf(string, ...any) {}
func (NopLogger) Infof(string, ...any)  {}
func (NopLogger) Errorf(string, ...any) {}

func defaultIDGenerator() func() string {
	var counter uint64
	return func() string {
		id := atomic.AddUint64(&counter, 1)
		return fmt.Sprintf("exp-%d", id)
	}
}

type trackingIterator struct {
	base        RowIterator
	tracker     ProgressTracker
	exportID    string
	redactions  map[int]any
	maxRows     int
	currentRows int64
}

func newTrackingIterator(base RowIterator, resolved ResolvedExport, tracker ProgressTracker, exportID string) *trackingIterator {
	return &trackingIterator{
		base:       base,
		tracker:    tracker,
		exportID:   exportID,
		redactions: resolved.RedactIndices,
		maxRows:    resolved.Definition.Policy.MaxRows,
	}
}

func (it *trackingIterator) Next(ctx context.Context) (Row, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	row, err := it.base.Next(ctx)
	if err != nil {
		return nil, err
	}

	it.currentRows++
	if it.maxRows > 0 && it.currentRows > int64(it.maxRows) {
		return nil, NewError(KindValidation, "max rows exceeded", nil)
	}

	if len(it.redactions) > 0 {
		copyRow := make(Row, len(row))
		copy(copyRow, row)
		row = copyRow
		for idx, value := range it.redactions {
			if idx >= 0 && idx < len(row) {
				row[idx] = value
			}
		}
	}

	if it.tracker != nil {
		if err := it.tracker.Advance(ctx, it.exportID, ProgressDelta{Rows: 1}, nil); err != nil {
			return nil, err
		}
	}

	return row, nil
}

func (it *trackingIterator) Close() error {
	return it.base.Close()
}

var _ io.Writer = (*countingWriter)(nil)
