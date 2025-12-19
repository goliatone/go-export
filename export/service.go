package export

import (
	"context"
	"fmt"
	"io"
	"time"
)

// DownloadInfo describes downloadable export metadata.
type DownloadInfo struct {
	ExportID string
	Artifact ArtifactRef
}

// Service coordinates export operations across runner, guard, tracker, and store.
type Service interface {
	RequestExport(ctx context.Context, actor Actor, req ExportRequest) (ExportRecord, error)
	GenerateExport(ctx context.Context, actor Actor, exportID string, req ExportRequest) (ExportResult, error)
	CancelExport(ctx context.Context, actor Actor, exportID string) (ExportRecord, error)
	DeleteExport(ctx context.Context, actor Actor, exportID string) error
	Status(ctx context.Context, actor Actor, exportID string) (ExportRecord, error)
	History(ctx context.Context, actor Actor, filter ProgressFilter) ([]ExportRecord, error)
	DownloadMetadata(ctx context.Context, actor Actor, exportID string) (DownloadInfo, error)
	Cleanup(ctx context.Context, now time.Time) (int, error)
}

// DeleteStrategy defines how delete requests are handled.
type DeleteStrategy interface {
	Delete(ctx context.Context, params DeleteParams) error
}

// DeleteParams provides dependencies to delete strategies.
type DeleteParams struct {
	Record  ExportRecord
	Tracker ProgressTracker
	Store   ArtifactStore
	Now     time.Time
}

// ServiceConfig supplies dependencies for Service.
type ServiceConfig struct {
	Runner         *Runner
	Tracker        ProgressTracker
	Store          ArtifactStore
	Guard          Guard
	DeliveryPolicy DeliveryPolicy
	DeleteStrategy DeleteStrategy
	Now            func() time.Time
	IDGenerator    func() string
}

type service struct {
	runner         *Runner
	tracker        ProgressTracker
	store          ArtifactStore
	guard          Guard
	deliveryPolicy DeliveryPolicy
	deleteStrategy DeleteStrategy
	now            func() time.Time
	idGenerator    func() string
}

// NewService creates a Service with the provided configuration.
func NewService(cfg ServiceConfig) Service {
	runner := cfg.Runner
	if runner == nil {
		runner = NewRunner()
	}

	nowFn := cfg.Now
	if nowFn == nil {
		nowFn = time.Now
	}
	idGen := cfg.IDGenerator
	if idGen == nil {
		idGen = defaultIDGenerator()
	}

	if runner.Now == nil {
		runner.Now = nowFn
	}

	if cfg.Guard != nil && runner.Guard == nil {
		runner.Guard = cfg.Guard
	}
	if cfg.Tracker != nil && runner.Tracker == nil {
		runner.Tracker = cfg.Tracker
	}
	if cfg.Store != nil && runner.Store == nil {
		runner.Store = cfg.Store
	}

	tracker := cfg.Tracker
	if tracker == nil {
		tracker = runner.Tracker
	}
	store := cfg.Store
	if store == nil {
		store = runner.Store
	}
	guard := cfg.Guard
	if guard == nil {
		guard = runner.Guard
	}

	policy := cfg.DeliveryPolicy
	if isZeroDeliveryPolicy(policy) {
		policy = runner.DeliveryPolicy
	}

	deleteStrategy := cfg.DeleteStrategy
	if deleteStrategy == nil {
		deleteStrategy = SoftDeleteStrategy{}
	}

	return &service{
		runner:         runner,
		tracker:        tracker,
		store:          store,
		guard:          guard,
		deliveryPolicy: policy,
		deleteStrategy: deleteStrategy,
		now:            nowFn,
		idGenerator:    idGen,
	}
}

// RequestExport handles sync/async export requests.
func (s *service) RequestExport(ctx context.Context, actor Actor, req ExportRequest) (ExportRecord, error) {
	if s == nil {
		return ExportRecord{}, AsGoError(NewError(KindInternal, "service is nil", nil))
	}

	resolved, err := s.resolveRequest(req)
	if err != nil {
		return ExportRecord{}, AsGoError(err)
	}

	delivery := SelectDelivery(resolved.Request, resolved.Definition, s.deliveryPolicy)
	if delivery == DeliveryAsync {
		return s.requestAsync(ctx, actor, resolved)
	}

	return s.requestSync(ctx, actor, resolved)
}

// GenerateExport produces an artifact for async jobs.
func (s *service) GenerateExport(ctx context.Context, actor Actor, exportID string, req ExportRequest) (ExportResult, error) {
	if s == nil {
		return ExportResult{}, AsGoError(NewError(KindInternal, "service is nil", nil))
	}
	if exportID == "" {
		return ExportResult{}, AsGoError(NewError(KindValidation, "export ID is required", nil))
	}
	if s.store == nil {
		return ExportResult{}, AsGoError(NewError(KindNotImpl, "artifact store not configured", nil))
	}
	if s.tracker == nil {
		return ExportResult{}, AsGoError(NewError(KindNotImpl, "progress tracker not configured", nil))
	}

	resolved, err := s.resolveRequest(req)
	if err != nil {
		return ExportResult{}, AsGoError(err)
	}

	if _, err := s.tracker.Status(ctx, exportID); err != nil {
		_, startErr := s.tracker.Start(ctx, ExportRecord{
			ID:          exportID,
			Definition:  resolved.Definition.Name,
			Format:      resolved.Request.Format,
			State:       StateQueued,
			RequestedBy: actor,
			Scope:       actor.Scope,
			CreatedAt:   s.now(),
			Artifact: ArtifactRef{
				Key: s.artifactKey(exportID, resolved.Request.Format),
				Meta: ArtifactMeta{
					ContentType: contentTypeForFormat(resolved.Request.Format),
					Filename:    resolved.Filename,
					CreatedAt:   s.now(),
				},
			},
		})
		if startErr != nil {
			return ExportResult{}, AsGoError(startErr)
		}
	}

	key := s.artifactKey(exportID, resolved.Request.Format)
	meta := ArtifactMeta{
		ContentType: contentTypeForFormat(resolved.Request.Format),
		Filename:    resolved.Filename,
		CreatedAt:   s.now(),
	}

	pr, pw := io.Pipe()
	putCh := make(chan storeResult, 1)
	go func() {
		ref, err := s.store.Put(ctx, key, pr, meta)
		_ = pr.CloseWithError(err)
		putCh <- storeResult{ref: ref, err: err}
	}()

	run := s.runnerWithActor(actor)
	if run == nil {
		_ = pw.Close()
		_ = pr.Close()
		return ExportResult{}, AsGoError(NewError(KindInternal, "runner is nil", nil))
	}
	run.IDGenerator = func() string { return exportID }
	run.Tracker = runnerTracker{base: s.tracker, exportID: exportID}

	runReq := resolved.Request
	runReq.Delivery = DeliverySync
	runReq.Output = pw

	result, err := run.Run(ctx, runReq)
	if err != nil {
		_ = pw.CloseWithError(err)
		<-putCh
		return ExportResult{}, err
	}

	_ = pw.Close()
	putResult := <-putCh
	if putResult.err != nil {
		_ = s.tracker.Fail(ctx, exportID, putResult.err, nil)
		return result, AsGoError(putResult.err)
	}

	s.updateArtifact(ctx, exportID, putResult.ref)
	result.Artifact = &putResult.ref
	return result, nil
}

// CancelExport marks an export as canceled.
func (s *service) CancelExport(ctx context.Context, actor Actor, exportID string) (ExportRecord, error) {
	if s == nil {
		return ExportRecord{}, AsGoError(NewError(KindInternal, "service is nil", nil))
	}
	if exportID == "" {
		return ExportRecord{}, AsGoError(NewError(KindValidation, "export ID is required", nil))
	}
	if s.tracker == nil {
		return ExportRecord{}, AsGoError(NewError(KindNotImpl, "progress tracker not configured", nil))
	}
	if err := s.authorizeDownload(ctx, actor, exportID); err != nil {
		return ExportRecord{}, err
	}

	if err := s.tracker.SetState(ctx, exportID, StateCanceled, nil); err != nil {
		return ExportRecord{}, AsGoError(err)
	}
	record, err := s.tracker.Status(ctx, exportID)
	if err != nil {
		return ExportRecord{}, AsGoError(err)
	}
	return record, nil
}

// DeleteExport removes artifacts for an export.
func (s *service) DeleteExport(ctx context.Context, actor Actor, exportID string) error {
	if s == nil {
		return AsGoError(NewError(KindInternal, "service is nil", nil))
	}
	if exportID == "" {
		return AsGoError(NewError(KindValidation, "export ID is required", nil))
	}
	if s.tracker == nil {
		return AsGoError(NewError(KindNotImpl, "progress tracker not configured", nil))
	}
	if err := s.authorizeDownload(ctx, actor, exportID); err != nil {
		return err
	}

	record, err := s.tracker.Status(ctx, exportID)
	if err != nil {
		return AsGoError(err)
	}
	if s.deleteStrategy == nil {
		s.deleteStrategy = SoftDeleteStrategy{}
	}
	if err := s.deleteStrategy.Delete(ctx, DeleteParams{
		Record:  record,
		Tracker: s.tracker,
		Store:   s.store,
		Now:     s.now(),
	}); err != nil {
		return AsGoError(err)
	}
	return nil
}

// Status returns a single export record.
func (s *service) Status(ctx context.Context, actor Actor, exportID string) (ExportRecord, error) {
	if s == nil {
		return ExportRecord{}, AsGoError(NewError(KindInternal, "service is nil", nil))
	}
	if exportID == "" {
		return ExportRecord{}, AsGoError(NewError(KindValidation, "export ID is required", nil))
	}
	if s.tracker == nil {
		return ExportRecord{}, AsGoError(NewError(KindNotImpl, "progress tracker not configured", nil))
	}
	if err := s.authorizeDownload(ctx, actor, exportID); err != nil {
		return ExportRecord{}, err
	}

	record, err := s.tracker.Status(ctx, exportID)
	if err != nil {
		return ExportRecord{}, AsGoError(err)
	}
	return record, nil
}

// History returns export records matching the filter.
func (s *service) History(ctx context.Context, actor Actor, filter ProgressFilter) ([]ExportRecord, error) {
	if s == nil {
		return nil, AsGoError(NewError(KindInternal, "service is nil", nil))
	}
	if s.tracker == nil {
		return nil, AsGoError(NewError(KindNotImpl, "progress tracker not configured", nil))
	}

	records, err := s.tracker.List(ctx, filter)
	if err != nil {
		return nil, AsGoError(err)
	}

	result := make([]ExportRecord, 0, len(records))
	for _, record := range records {
		if !scopeMatches(actor.Scope, record.Scope) {
			continue
		}
		if s.guard != nil {
			if err := s.guard.AuthorizeDownload(ctx, actor, record.ID); err != nil {
				continue
			}
		}
		result = append(result, record)
	}
	return result, nil
}

// DownloadMetadata returns artifact metadata for an export.
func (s *service) DownloadMetadata(ctx context.Context, actor Actor, exportID string) (DownloadInfo, error) {
	if s == nil {
		return DownloadInfo{}, AsGoError(NewError(KindInternal, "service is nil", nil))
	}
	if exportID == "" {
		return DownloadInfo{}, AsGoError(NewError(KindValidation, "export ID is required", nil))
	}
	if s.tracker == nil {
		return DownloadInfo{}, AsGoError(NewError(KindNotImpl, "progress tracker not configured", nil))
	}
	if s.store == nil {
		return DownloadInfo{}, AsGoError(NewError(KindNotImpl, "artifact store not configured", nil))
	}
	if err := s.authorizeDownload(ctx, actor, exportID); err != nil {
		return DownloadInfo{}, err
	}

	record, err := s.tracker.Status(ctx, exportID)
	if err != nil {
		return DownloadInfo{}, AsGoError(err)
	}
	if record.State != StateCompleted {
		return DownloadInfo{}, AsGoError(NewError(KindValidation, "export not completed", nil))
	}

	key := record.Artifact.Key
	if key == "" {
		key = s.artifactKey(exportID, record.Format)
	}
	reader, meta, err := s.store.Open(ctx, key)
	if err != nil {
		return DownloadInfo{}, AsGoError(err)
	}
	_ = reader.Close()

	return DownloadInfo{
		ExportID: exportID,
		Artifact: ArtifactRef{Key: key, Meta: meta},
	}, nil
}

// Cleanup deletes expired artifacts and returns the count removed.
func (s *service) Cleanup(ctx context.Context, now time.Time) (int, error) {
	if s == nil {
		return 0, AsGoError(NewError(KindInternal, "service is nil", nil))
	}
	if s.tracker == nil {
		return 0, AsGoError(NewError(KindNotImpl, "progress tracker not configured", nil))
	}
	if s.store == nil {
		return 0, AsGoError(NewError(KindNotImpl, "artifact store not configured", nil))
	}
	if now.IsZero() {
		now = s.now()
	}

	records, err := s.tracker.List(ctx, ProgressFilter{})
	if err != nil {
		return 0, AsGoError(err)
	}

	deleted := 0
	for _, record := range records {
		if record.ExpiresAt.IsZero() || record.ExpiresAt.After(now) {
			continue
		}
		key := record.Artifact.Key
		if key == "" {
			key = s.artifactKey(record.ID, record.Format)
		}
		if key != "" {
			if err := s.store.Delete(ctx, key); err != nil {
				return deleted, AsGoError(err)
			}
		}
		if deleter, ok := s.tracker.(RecordDeleter); ok {
			if err := deleter.Delete(ctx, record.ID); err != nil {
				return deleted, AsGoError(err)
			}
		} else {
			record.State = StateDeleted
			if updater, ok := s.tracker.(RecordUpdater); ok {
				if err := updater.Update(ctx, record); err != nil {
					return deleted, AsGoError(err)
				}
			} else {
				_ = s.tracker.SetState(ctx, record.ID, StateDeleted, map[string]any{"cleanup": true})
			}
		}
		deleted++
	}
	return deleted, nil
}

func (s *service) requestSync(ctx context.Context, actor Actor, resolved ResolvedExport) (ExportRecord, error) {
	if resolved.Request.Output == nil {
		return ExportRecord{}, AsGoError(NewError(KindValidation, "output writer is required", nil))
	}

	run := s.runnerWithActor(actor)
	if run == nil {
		return ExportRecord{}, AsGoError(NewError(KindInternal, "runner is nil", nil))
	}

	result, err := run.Run(ctx, resolved.Request)
	if err != nil {
		return ExportRecord{}, err
	}

	if s.tracker != nil {
		record, err := s.tracker.Status(ctx, result.ID)
		if err == nil {
			return record, nil
		}
	}

	return recordFromResult(actor, resolved, result, s.now()), nil
}

func (s *service) requestAsync(ctx context.Context, actor Actor, resolved ResolvedExport) (ExportRecord, error) {
	if s.store == nil {
		return ExportRecord{}, AsGoError(NewError(KindNotImpl, "artifact store not configured", nil))
	}
	if s.tracker == nil {
		return ExportRecord{}, AsGoError(NewError(KindNotImpl, "progress tracker not configured", nil))
	}

	if s.guard != nil {
		if err := s.guard.AuthorizeExport(ctx, actor, resolved.Request, resolved.Definition); err != nil {
			return ExportRecord{}, AsGoError(NewError(KindAuthz, "export not authorized", err))
		}
	}

	exportID := s.nextID()
	record := ExportRecord{
		ID:          exportID,
		Definition:  resolved.Definition.Name,
		Format:      resolved.Request.Format,
		State:       StateQueued,
		RequestedBy: actor,
		Scope:       actor.Scope,
		CreatedAt:   s.now(),
		Artifact: ArtifactRef{
			Key: s.artifactKey(exportID, resolved.Request.Format),
			Meta: ArtifactMeta{
				ContentType: contentTypeForFormat(resolved.Request.Format),
				Filename:    resolved.Filename,
				CreatedAt:   s.now(),
			},
		},
	}

	if s.runner != nil && s.runner.Retention != nil {
		ttl, err := s.runner.Retention.TTL(ctx, actor, resolved.Request, resolved.Definition)
		if err != nil {
			return ExportRecord{}, AsGoError(err)
		}
		if ttl > 0 {
			record.ExpiresAt = s.now().Add(ttl)
			record.Artifact.Meta.ExpiresAt = record.ExpiresAt
		}
	}

	id, err := s.tracker.Start(ctx, record)
	if err != nil {
		return ExportRecord{}, AsGoError(err)
	}
	if id != "" && id != record.ID {
		record.ID = id
		record.Artifact.Key = s.artifactKey(id, resolved.Request.Format)
	}
	return record, nil
}

func (s *service) resolveRequest(req ExportRequest) (ResolvedExport, error) {
	if s.runner == nil || s.runner.Definitions == nil {
		return ResolvedExport{}, NewError(KindInternal, "definition registry not configured", nil)
	}
	if s.now == nil {
		s.now = time.Now
	}
	def, err := s.runner.Definitions.Resolve(req)
	if err != nil {
		return ResolvedExport{}, err
	}
	return ResolveExport(req, def, s.now())
}

func (s *service) runnerWithActor(actor Actor) *Runner {
	if s.runner == nil {
		return nil
	}
	run := *s.runner
	run.ActorProvider = staticActorProvider{actor: actor}
	return &run
}

func (s *service) authorizeDownload(ctx context.Context, actor Actor, exportID string) error {
	if s.guard == nil {
		return nil
	}
	if err := s.guard.AuthorizeDownload(ctx, actor, exportID); err != nil {
		return AsGoError(NewError(KindAuthz, "download not authorized", err))
	}
	return nil
}

func (s *service) artifactKey(exportID string, format Format) string {
	if exportID == "" {
		return ""
	}
	if format == "" {
		format = FormatCSV
	}
	return fmt.Sprintf("exports/%s.%s", exportID, format)
}

func (s *service) nextID() string {
	if s.idGenerator == nil {
		s.idGenerator = defaultIDGenerator()
	}
	return s.idGenerator()
}

func (s *service) updateArtifact(ctx context.Context, exportID string, ref ArtifactRef) {
	if s.tracker == nil {
		return
	}
	if tracker, ok := s.tracker.(ArtifactTracker); ok {
		_ = tracker.SetArtifact(ctx, exportID, ref)
		return
	}
	if updater, ok := s.tracker.(RecordUpdater); ok {
		record, err := s.tracker.Status(ctx, exportID)
		if err != nil {
			return
		}
		record.Artifact = ref
		_ = updater.Update(ctx, record)
	}
}

type runnerTracker struct {
	base     ProgressTracker
	exportID string
}

func (t runnerTracker) Start(ctx context.Context, record ExportRecord) (string, error) {
	if t.base == nil {
		return record.ID, nil
	}
	if t.exportID != "" {
		return t.exportID, nil
	}
	return t.base.Start(ctx, record)
}

func (t runnerTracker) Advance(ctx context.Context, id string, delta ProgressDelta, meta map[string]any) error {
	if t.base == nil {
		return nil
	}
	return t.base.Advance(ctx, id, delta, meta)
}

func (t runnerTracker) SetState(ctx context.Context, id string, state ExportState, meta map[string]any) error {
	if t.base == nil {
		return nil
	}
	return t.base.SetState(ctx, id, state, meta)
}

func (t runnerTracker) Fail(ctx context.Context, id string, err error, meta map[string]any) error {
	if t.base == nil {
		return nil
	}
	return t.base.Fail(ctx, id, err, meta)
}

func (t runnerTracker) Complete(ctx context.Context, id string, meta map[string]any) error {
	if t.base == nil {
		return nil
	}
	return t.base.Complete(ctx, id, meta)
}

func (t runnerTracker) Status(ctx context.Context, id string) (ExportRecord, error) {
	if t.base == nil {
		return ExportRecord{}, NewError(KindNotImpl, "progress tracker not configured", nil)
	}
	return t.base.Status(ctx, id)
}

func (t runnerTracker) List(ctx context.Context, filter ProgressFilter) ([]ExportRecord, error) {
	if t.base == nil {
		return nil, NewError(KindNotImpl, "progress tracker not configured", nil)
	}
	return t.base.List(ctx, filter)
}

type staticActorProvider struct {
	actor Actor
}

func (p staticActorProvider) FromContext(ctx context.Context) (Actor, error) {
	_ = ctx
	return p.actor, nil
}

type storeResult struct {
	ref ArtifactRef
	err error
}

func recordFromResult(actor Actor, resolved ResolvedExport, result ExportResult, now time.Time) ExportRecord {
	return ExportRecord{
		ID:          result.ID,
		Definition:  resolved.Definition.Name,
		Format:      result.Format,
		State:       StateCompleted,
		RequestedBy: actor,
		Scope:       actor.Scope,
		Counts: ExportCounts{
			Processed: result.Rows,
		},
		BytesWritten: result.Bytes,
		CreatedAt:    now,
		StartedAt:    now,
		CompletedAt:  now,
	}
}

func scopeMatches(actor Scope, record Scope) bool {
	if actor.TenantID != "" && actor.TenantID != record.TenantID {
		return false
	}
	if actor.WorkspaceID != "" && actor.WorkspaceID != record.WorkspaceID {
		return false
	}
	return true
}

// SoftDeleteStrategy deletes artifacts and tombstones the record.
type SoftDeleteStrategy struct{}

func (SoftDeleteStrategy) Delete(ctx context.Context, params DeleteParams) error {
	if params.Tracker == nil {
		return NewError(KindNotImpl, "progress tracker not configured", nil)
	}
	if params.Store != nil {
		artifactKey := params.Record.Artifact.Key
		if artifactKey == "" {
			artifactKey = fmt.Sprintf("exports/%s.%s", params.Record.ID, params.Record.Format)
		}
		if artifactKey != "" {
			if err := params.Store.Delete(ctx, artifactKey); err != nil {
				return err
			}
		}
	}
	updated := params.Record
	updated.State = StateDeleted
	return updateRecord(ctx, params.Tracker, updated)
}

// TombstoneDeleteStrategy marks records deleted and purges them after TTL.
type TombstoneDeleteStrategy struct {
	TTL time.Duration
}

func (s TombstoneDeleteStrategy) Delete(ctx context.Context, params DeleteParams) error {
	if s.TTL <= 0 {
		return SoftDeleteStrategy{}.Delete(ctx, params)
	}
	if params.Tracker == nil {
		return NewError(KindNotImpl, "progress tracker not configured", nil)
	}
	if params.Store != nil {
		artifactKey := params.Record.Artifact.Key
		if artifactKey == "" {
			artifactKey = fmt.Sprintf("exports/%s.%s", params.Record.ID, params.Record.Format)
		}
		if artifactKey != "" {
			if err := params.Store.Delete(ctx, artifactKey); err != nil {
				return err
			}
		}
	}
	updated := params.Record
	updated.State = StateDeleted
	updated.ExpiresAt = params.Now.Add(s.TTL)
	updated.Artifact.Meta.ExpiresAt = updated.ExpiresAt

	if updater, ok := params.Tracker.(RecordUpdater); ok {
		return updater.Update(ctx, updated)
	}
	return NewError(KindNotImpl, "tracker does not support record updates", nil)
}

func updateRecord(ctx context.Context, tracker ProgressTracker, record ExportRecord) error {
	if tracker == nil {
		return NewError(KindNotImpl, "progress tracker not configured", nil)
	}
	if updater, ok := tracker.(RecordUpdater); ok {
		return updater.Update(ctx, record)
	}
	return tracker.SetState(ctx, record.ID, record.State, nil)
}

func isZeroDeliveryPolicy(policy DeliveryPolicy) bool {
	return policy.Default == "" &&
		policy.Thresholds.MaxRows == 0 &&
		policy.Thresholds.MaxBytes == 0 &&
		policy.Thresholds.MaxDuration == 0
}

func contentTypeForFormat(format Format) string {
	switch format {
	case FormatCSV:
		return "text/csv"
	case FormatNDJSON:
		return "application/x-ndjson"
	case FormatJSON:
		return "application/json"
	case FormatXLSX:
		return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
	case FormatTemplate:
		return "text/html"
	default:
		return "application/octet-stream"
	}
}
