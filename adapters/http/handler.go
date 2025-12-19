package exporthttp

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"path"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	errorslib "github.com/goliatone/go-errors"
	"github.com/goliatone/go-export/export"
)

// Config configures the HTTP adapter.
type Config struct {
	Service          export.Service
	Runner           *export.Runner
	Store            export.ArtifactStore
	Guard            export.Guard
	ActorProvider    export.ActorProvider
	DeliveryPolicy   export.DeliveryPolicy
	BasePath         string
	SignedURLTTL     time.Duration
	IdempotencyStore IdempotencyStore
	IdempotencyTTL   time.Duration
	Logger           export.Logger
	IDGenerator      func() string
	RequestDecoder   RequestDecoder
}

// Handler exposes export HTTP endpoints.
type Handler struct {
	service          export.Service
	runner           *export.Runner
	store            export.ArtifactStore
	guard            export.Guard
	actorProvider    export.ActorProvider
	deliveryPolicy   export.DeliveryPolicy
	basePath         string
	signedURLTTL     time.Duration
	idempotencyStore IdempotencyStore
	idempotencyTTL   time.Duration
	logger           export.Logger
	idGenerator      func() string
	requestDecoder   RequestDecoder
}

// NewHandler creates a new HTTP handler.
func NewHandler(cfg Config) *Handler {
	basePath := strings.TrimRight(cfg.BasePath, "/")
	if basePath == "" {
		basePath = "/admin/exports"
	}
	logger := cfg.Logger
	if logger == nil {
		logger = export.NopLogger{}
	}
	decoder := cfg.RequestDecoder
	if decoder == nil {
		decoder = JSONRequestDecoder{}
	}
	return &Handler{
		service:          cfg.Service,
		runner:           cfg.Runner,
		store:            cfg.Store,
		guard:            cfg.Guard,
		actorProvider:    cfg.ActorProvider,
		deliveryPolicy:   cfg.DeliveryPolicy,
		basePath:         basePath,
		signedURLTTL:     cfg.SignedURLTTL,
		idempotencyStore: cfg.IdempotencyStore,
		idempotencyTTL:   cfg.IdempotencyTTL,
		logger:           logger,
		idGenerator:      cfg.IDGenerator,
		requestDecoder:   decoder,
	}
}

// RegisterRoutes registers handlers on a compatible router.
func (h *Handler) RegisterRoutes(router any) {
	switch r := router.(type) {
	case interface{ Handle(string, http.Handler) }:
		r.Handle(h.basePath, h)
		r.Handle(h.basePath+"/", h)
	case interface {
		HandleFunc(string, func(http.ResponseWriter, *http.Request))
	}:
		r.HandleFunc(h.basePath, h.ServeHTTP)
		r.HandleFunc(h.basePath+"/", h.ServeHTTP)
	}
}

// ServeHTTP routes export endpoints.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h == nil {
		writeError(w, export.NewError(export.KindInternal, "handler is nil", nil))
		return
	}
	if !strings.HasPrefix(r.URL.Path, h.basePath) {
		http.NotFound(w, r)
		return
	}

	pathSuffix := strings.TrimPrefix(r.URL.Path, h.basePath)
	pathSuffix = strings.Trim(pathSuffix, "/")
	parts := []string{}
	if pathSuffix != "" {
		parts = strings.Split(pathSuffix, "/")
	}

	switch r.Method {
	case http.MethodPost:
		if len(parts) != 0 {
			http.NotFound(w, r)
			return
		}
		h.handlePost(w, r)
	case http.MethodGet:
		switch len(parts) {
		case 0:
			h.handleList(w, r)
		case 1:
			h.handleStatus(w, r, parts[0])
		case 2:
			if parts[1] != "download" {
				http.NotFound(w, r)
				return
			}
			h.handleDownload(w, r, parts[0])
		default:
			http.NotFound(w, r)
		}
	case http.MethodDelete:
		if len(parts) != 1 {
			http.NotFound(w, r)
			return
		}
		h.handleDelete(w, r, parts[0])
	default:
		w.Header().Set("Allow", "GET,POST,DELETE")
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (h *Handler) handlePost(w http.ResponseWriter, r *http.Request) {
	if h.requestDecoder == nil {
		writeError(w, export.NewError(export.KindInternal, "request decoder not configured", nil))
		return
	}
	req, err := h.requestDecoder.Decode(r)
	if err != nil {
		writeError(w, err)
		return
	}
	if key := r.Header.Get("Idempotency-Key"); key != "" {
		req.IdempotencyKey = key
	}

	actor, err := h.actorFromRequest(r)
	if err != nil {
		writeError(w, err)
		return
	}

	resolved, err := h.resolve(req)
	if err != nil {
		writeError(w, err)
		return
	}
	delivery := export.SelectDelivery(resolved.Request, resolved.Definition, h.deliveryPolicyForRequest())

	if delivery == export.DeliveryAsync {
		h.handleAsync(w, r, actor, resolved)
		return
	}
	h.handleSync(w, r, actor, resolved)
}

func (h *Handler) handleAsync(w http.ResponseWriter, r *http.Request, actor export.Actor, resolved export.ResolvedExport) {
	if h.service == nil {
		writeError(w, export.NewError(export.KindNotImpl, "export service not configured", nil))
		return
	}

	idempotencyKey := resolved.Request.IdempotencyKey
	if idempotencyKey != "" && h.idempotencyStore != nil {
		signature := h.idempotencySignature(idempotencyKey, actor, resolved.Request)
		exportID, ok, err := h.idempotencyStore.Get(r.Context(), signature)
		if err != nil {
			writeError(w, err)
			return
		}
		if ok {
			record, err := h.service.Status(r.Context(), actor, exportID)
			if err == nil && isReusableState(record.State) {
				writeJSON(w, http.StatusAccepted, asyncResponse{
					ID:          record.ID,
					StatusURL:   h.statusURL(r, record.ID),
					DownloadURL: h.downloadURL(r, record.ID),
				})
				return
			}
		}
	}

	asyncReq := resolved.Request
	asyncReq.Delivery = export.DeliveryAsync
	asyncReq.Output = nil
	record, err := h.service.RequestExport(r.Context(), actor, asyncReq)
	if err != nil {
		writeError(w, err)
		return
	}

	if idempotencyKey != "" && h.idempotencyStore != nil {
		signature := h.idempotencySignature(idempotencyKey, actor, asyncReq)
		ttl := h.idempotencyTTL
		if ttl == 0 {
			ttl = 24 * time.Hour
		}
		if err := h.idempotencyStore.Set(r.Context(), signature, record.ID, ttl); err != nil {
			h.logger.Errorf("idempotency store set failed: %v", err)
		}
	}

	writeJSON(w, http.StatusAccepted, asyncResponse{
		ID:          record.ID,
		StatusURL:   h.statusURL(r, record.ID),
		DownloadURL: h.downloadURL(r, record.ID),
	})
}

func (h *Handler) handleSync(w http.ResponseWriter, r *http.Request, actor export.Actor, resolved export.ResolvedExport) {
	if h.runner == nil {
		writeError(w, export.NewError(export.KindNotImpl, "export runner not configured", nil))
		return
	}
	guard := h.guard
	if guard == nil {
		guard = h.runner.Guard
	}
	if guard != nil {
		if err := guard.AuthorizeExport(r.Context(), actor, resolved.Request, resolved.Definition); err != nil {
			writeError(w, export.NewError(export.KindAuthz, "export not authorized", err))
			return
		}
	}

	exportID := h.nextID()
	filename := sanitizeFilename(resolved.Filename, resolved.Request.Format)
	setDownloadHeaders(w.Header(), exportID, filename, contentTypeForFormat(resolved.Request.Format))

	req := resolved.Request
	req.Delivery = export.DeliverySync
	tracker := &trackingWriter{ResponseWriter: w}
	req.Output = tracker

	run := *h.runner
	run.IDGenerator = func() string { return exportID }
	run.ActorProvider = staticActorProvider{actor: actor}

	_, err := run.Run(r.Context(), req)
	if err != nil {
		if !tracker.Written() {
			clearDownloadHeaders(w.Header())
			writeError(w, err)
			return
		}
		h.logger.Errorf("sync export failed after write: %v", err)
	}
}

func (h *Handler) handleList(w http.ResponseWriter, r *http.Request) {
	if h.service == nil {
		writeError(w, export.NewError(export.KindNotImpl, "export service not configured", nil))
		return
	}
	actor, err := h.actorFromRequest(r)
	if err != nil {
		writeError(w, err)
		return
	}

	filter, err := parseFilter(r)
	if err != nil {
		writeError(w, err)
		return
	}

	records, err := h.service.History(r.Context(), actor, filter)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, records)
}

func (h *Handler) handleStatus(w http.ResponseWriter, r *http.Request, exportID string) {
	if h.service == nil {
		writeError(w, export.NewError(export.KindNotImpl, "export service not configured", nil))
		return
	}
	actor, err := h.actorFromRequest(r)
	if err != nil {
		writeError(w, err)
		return
	}

	record, err := h.service.Status(r.Context(), actor, exportID)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, record)
}

func (h *Handler) handleDownload(w http.ResponseWriter, r *http.Request, exportID string) {
	if h.service == nil {
		writeError(w, export.NewError(export.KindNotImpl, "export service not configured", nil))
		return
	}
	if h.store == nil {
		writeError(w, export.NewError(export.KindNotImpl, "artifact store not configured", nil))
		return
	}
	actor, err := h.actorFromRequest(r)
	if err != nil {
		writeError(w, err)
		return
	}

	info, err := h.service.DownloadMetadata(r.Context(), actor, exportID)
	if err != nil {
		writeError(w, err)
		return
	}

	ttl := h.signedURLTTL
	if ttl > 0 && !info.Artifact.Meta.ExpiresAt.IsZero() {
		remaining := time.Until(info.Artifact.Meta.ExpiresAt)
		if remaining <= 0 {
			writeError(w, export.NewError(export.KindValidation, "artifact expired", nil))
			return
		}
		if remaining < ttl {
			ttl = remaining
		}
	}
	if ttl > 0 {
		url, err := h.store.SignedURL(r.Context(), info.Artifact.Key, ttl)
		if err == nil {
			http.Redirect(w, r, url, http.StatusFound)
			return
		}
		if exportErr, ok := err.(*export.ExportError); ok && exportErr.Kind != export.KindNotImpl {
			writeError(w, err)
			return
		}
	}

	reader, meta, err := h.store.Open(r.Context(), info.Artifact.Key)
	if err != nil {
		writeError(w, err)
		return
	}
	defer reader.Close()

	filename := meta.Filename
	if filename == "" {
		filename = path.Base(info.Artifact.Key)
	}
	contentType := meta.ContentType
	if contentType == "" {
		contentType = mime.TypeByExtension(filepath.Ext(filename))
	}
	format := formatFromPath(filename)
	filename = sanitizeFilename(filename, format)
	setDownloadHeaders(w.Header(), info.ExportID, filename, contentType)
	if meta.Size > 0 {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", meta.Size))
	}
	if _, err := ioCopy(w, reader); err != nil {
		h.logger.Errorf("download copy failed: %v", err)
	}
}

func (h *Handler) handleDelete(w http.ResponseWriter, r *http.Request, exportID string) {
	if h.service == nil {
		writeError(w, export.NewError(export.KindNotImpl, "export service not configured", nil))
		return
	}
	actor, err := h.actorFromRequest(r)
	if err != nil {
		writeError(w, err)
		return
	}

	record, err := h.service.Status(r.Context(), actor, exportID)
	if err != nil {
		writeError(w, err)
		return
	}

	switch record.State {
	case export.StateQueued, export.StateRunning:
		if _, err := h.service.CancelExport(r.Context(), actor, exportID); err != nil {
			writeError(w, err)
			return
		}
	default:
		if err := h.service.DeleteExport(r.Context(), actor, exportID); err != nil {
			writeError(w, err)
			return
		}
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) actorFromRequest(r *http.Request) (export.Actor, error) {
	if h.actorProvider == nil {
		return export.Actor{}, nil
	}
	actor, err := h.actorProvider.FromContext(r.Context())
	if err != nil {
		return export.Actor{}, export.NewError(export.KindAuthz, "actor resolution failed", err)
	}
	return actor, nil
}

func (h *Handler) resolve(req export.ExportRequest) (export.ResolvedExport, error) {
	if h.runner == nil || h.runner.Definitions == nil {
		return export.ResolvedExport{}, export.NewError(export.KindInternal, "definition registry not configured", nil)
	}
	now := time.Now()
	if h.runner.Now != nil {
		now = h.runner.Now()
	}
	def, err := h.runner.Definitions.Resolve(req)
	if err != nil {
		return export.ResolvedExport{}, err
	}
	return export.ResolveExport(req, def, now)
}

func (h *Handler) statusURL(r *http.Request, exportID string) string {
	return path.Join(h.basePath, exportID)
}

func (h *Handler) downloadURL(r *http.Request, exportID string) string {
	return path.Join(h.basePath, exportID, "download")
}

func (h *Handler) nextID() string {
	if h.idGenerator == nil {
		h.idGenerator = defaultIDGenerator()
	}
	return h.idGenerator()
}

func (h *Handler) idempotencySignature(key string, actor export.Actor, req export.ExportRequest) string {
	return buildIdempotencyKey(key, actor, req)
}

func (h *Handler) deliveryPolicyForRequest() export.DeliveryPolicy {
	if !isZeroDeliveryPolicy(h.deliveryPolicy) {
		return h.deliveryPolicy
	}
	if h.runner != nil {
		return h.runner.DeliveryPolicy
	}
	return export.DeliveryPolicy{}
}

func writeError(w http.ResponseWriter, err error) {
	if err == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	ge := export.AsGoError(err)
	status := statusForError(ge)
	payload := errorResponse{
		Error: errorBody{
			Message: ge.Message,
			Code:    ge.TextCode,
		},
	}
	writeJSON(w, status, payload)
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func statusForError(err *errorslib.Error) int {
	if err == nil {
		return http.StatusInternalServerError
	}
	if err.TextCode == "not_implemented" {
		return http.StatusNotImplemented
	}
	switch err.Category {
	case errorslib.CategoryValidation:
		return http.StatusBadRequest
	case errorslib.CategoryAuthz:
		return http.StatusForbidden
	case errorslib.CategoryNotFound:
		return http.StatusNotFound
	case errorslib.CategoryOperation:
		if err.TextCode == "canceled" {
			return http.StatusConflict
		}
		return http.StatusRequestTimeout
	default:
		return http.StatusInternalServerError
	}
}

func parseFilter(r *http.Request) (export.ProgressFilter, error) {
	q := r.URL.Query()
	filter := export.ProgressFilter{
		Definition: q.Get("definition"),
		State:      export.ExportState(q.Get("state")),
	}
	if since := q.Get("since"); since != "" {
		ts, err := time.Parse(time.RFC3339, since)
		if err != nil {
			return export.ProgressFilter{}, export.NewError(export.KindValidation, "invalid since timestamp", err)
		}
		filter.Since = ts
	}
	if until := q.Get("until"); until != "" {
		ts, err := time.Parse(time.RFC3339, until)
		if err != nil {
			return export.ProgressFilter{}, export.NewError(export.KindValidation, "invalid until timestamp", err)
		}
		filter.Until = ts
	}
	return filter, nil
}

func isReusableState(state export.ExportState) bool {
	switch state {
	case export.StateQueued, export.StateRunning, export.StatePublishing, export.StateCompleted:
		return true
	default:
		return false
	}
}

func sanitizeFilename(filename string, format export.Format) string {
	name := strings.TrimSpace(filename)
	name = strings.ReplaceAll(name, "\"", "")
	name = strings.ReplaceAll(name, "/", "_")
	name = strings.ReplaceAll(name, "\\", "_")
	if name == "" {
		if format != "" {
			name = fmt.Sprintf("export.%s", format)
		} else {
			name = "export"
		}
	}
	return name
}

func setDownloadHeaders(header http.Header, exportID, filename, contentType string) {
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	header.Set("Content-Type", contentType)
	header.Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", filename))
	if exportID != "" {
		header.Set("X-Export-Id", exportID)
	}
}

func clearDownloadHeaders(header http.Header) {
	header.Del("Content-Disposition")
	header.Del("Content-Type")
	header.Del("X-Export-Id")
}

func contentTypeForFormat(format export.Format) string {
	switch format {
	case export.FormatCSV:
		return "text/csv"
	case export.FormatJSON:
		return "application/json"
	case export.FormatNDJSON:
		return "application/x-ndjson"
	case export.FormatXLSX:
		return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
	case export.FormatTemplate:
		return "text/html"
	default:
		return "application/octet-stream"
	}
}

func formatFromPath(name string) export.Format {
	ext := strings.ToLower(strings.TrimPrefix(filepath.Ext(name), "."))
	switch ext {
	case "csv":
		return export.FormatCSV
	case "json":
		return export.FormatJSON
	case "ndjson":
		return export.FormatNDJSON
	case "xlsx":
		return export.FormatXLSX
	case "html":
		return export.FormatTemplate
	default:
		return ""
	}
}

func isZeroDeliveryPolicy(policy export.DeliveryPolicy) bool {
	return policy.Default == "" &&
		policy.Thresholds.MaxRows == 0 &&
		policy.Thresholds.MaxBytes == 0 &&
		policy.Thresholds.MaxDuration == 0
}

type trackingWriter struct {
	http.ResponseWriter
	written bool
}

func (w *trackingWriter) WriteHeader(statusCode int) {
	w.written = true
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *trackingWriter) Write(p []byte) (int, error) {
	w.written = true
	return w.ResponseWriter.Write(p)
}

func (w *trackingWriter) Written() bool {
	return w.written
}

func (w *trackingWriter) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

type staticActorProvider struct {
	actor export.Actor
}

func (p staticActorProvider) FromContext(ctx context.Context) (export.Actor, error) {
	_ = ctx
	return p.actor, nil
}

func defaultIDGenerator() func() string {
	var counter uint64
	return func() string {
		id := atomic.AddUint64(&counter, 1)
		return fmt.Sprintf("exp-%d", id)
	}
}

func ioCopy(dst http.ResponseWriter, src io.Reader) (int64, error) {
	return io.Copy(dst, src)
}
