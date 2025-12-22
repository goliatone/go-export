package exportapi

import (
	"bytes"
	"context"
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

// DefaultMaxBufferBytes is the fallback buffer limit when streaming is unavailable.
const DefaultMaxBufferBytes int64 = 8 * 1024 * 1024

// Config configures the shared export API controller.
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
	MaxBufferBytes   int64
}

// Controller exposes export API handlers for multiple transports.
type Controller struct {
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
	maxBufferBytes   int64
}

// NewController creates a shared export API controller.
func NewController(cfg Config) *Controller {
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
	maxBuffer := cfg.MaxBufferBytes
	if maxBuffer <= 0 {
		maxBuffer = DefaultMaxBufferBytes
	}
	return &Controller{
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
		maxBufferBytes:   maxBuffer,
	}
}

// BasePath returns the configured base path.
func (c *Controller) BasePath() string {
	if c == nil {
		return ""
	}
	return c.basePath
}

// Serve routes export endpoints using the shared controller.
func (c *Controller) Serve(req Request, res Response) {
	if res == nil {
		return
	}
	if c == nil {
		WriteError(res, export.NewError(export.KindInternal, "handler is nil", nil))
		return
	}
	if req == nil {
		WriteError(res, export.NewError(export.KindInternal, "request is nil", nil))
		return
	}
	if !strings.HasPrefix(req.Path(), c.basePath) {
		writeNotFound(res)
		return
	}

	pathSuffix := strings.TrimPrefix(req.Path(), c.basePath)
	pathSuffix = strings.Trim(pathSuffix, "/")
	parts := []string{}
	if pathSuffix != "" {
		parts = strings.Split(pathSuffix, "/")
	}

	switch req.Method() {
	case http.MethodPost:
		if len(parts) != 0 {
			writeNotFound(res)
			return
		}
		c.handlePost(req, res)
	case http.MethodGet:
		switch len(parts) {
		case 0:
			c.handleList(req, res)
		case 1:
			c.handleStatus(req, res, parts[0])
		case 2:
			switch parts[1] {
			case "download":
				c.handleDownload(req, res, parts[0])
			case "preview":
				c.handlePreview(req, res, parts[0])
			default:
				writeNotFound(res)
			}
		default:
			writeNotFound(res)
		}
	case http.MethodDelete:
		if len(parts) != 1 {
			writeNotFound(res)
			return
		}
		c.handleDelete(req, res, parts[0])
	default:
		res.SetHeader("Allow", "GET,POST,DELETE")
		res.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (c *Controller) handlePost(req Request, res Response) {
	if c.requestDecoder == nil {
		WriteError(res, export.NewError(export.KindInternal, "request decoder not configured", nil))
		return
	}
	decoded, err := c.requestDecoder.Decode(req)
	if err != nil {
		WriteError(res, err)
		return
	}
	if key := req.Header("Idempotency-Key"); key != "" {
		decoded.IdempotencyKey = key
	}

	actor, err := c.actorFromRequest(req)
	if err != nil {
		WriteError(res, err)
		return
	}

	resolved, err := c.resolve(decoded)
	if err != nil {
		WriteError(res, err)
		return
	}
	delivery := export.SelectDelivery(resolved.Request, resolved.Definition, c.deliveryPolicyForRequest())

	if delivery == export.DeliveryAsync {
		c.handleAsync(req, res, actor, resolved)
		return
	}
	c.handleSync(req, res, actor, resolved)
}

func (c *Controller) handleAsync(req Request, res Response, actor export.Actor, resolved export.ResolvedExport) {
	if c.service == nil {
		WriteError(res, export.NewError(export.KindNotImpl, "export service not configured", nil))
		return
	}

	idempotencyKey := resolved.Request.IdempotencyKey
	if idempotencyKey != "" && c.idempotencyStore != nil {
		signature := c.idempotencySignature(idempotencyKey, actor, resolved.Request)
		exportID, ok, err := c.idempotencyStore.Get(req.Context(), signature)
		if err != nil {
			WriteError(res, err)
			return
		}
		if ok {
			record, err := c.service.Status(req.Context(), actor, exportID)
			if err == nil && isReusableState(record.State) {
				writeJSON(res, http.StatusAccepted, AsyncResponse{
					ID:          record.ID,
					StatusURL:   c.statusURL(record.ID),
					DownloadURL: c.downloadURL(record.ID),
				})
				return
			}
		}
	}

	asyncReq := resolved.Request
	asyncReq.Delivery = export.DeliveryAsync
	asyncReq.Output = nil
	record, err := c.service.RequestExport(req.Context(), actor, asyncReq)
	if err != nil {
		WriteError(res, err)
		return
	}

	if idempotencyKey != "" && c.idempotencyStore != nil {
		signature := c.idempotencySignature(idempotencyKey, actor, asyncReq)
		ttl := c.idempotencyTTL
		if ttl == 0 {
			ttl = 24 * time.Hour
		}
		if err := c.idempotencyStore.Set(req.Context(), signature, record.ID, ttl); err != nil {
			c.logger.Errorf("idempotency store set failed: %v", err)
		}
	}

	writeJSON(res, http.StatusAccepted, AsyncResponse{
		ID:          record.ID,
		StatusURL:   c.statusURL(record.ID),
		DownloadURL: c.downloadURL(record.ID),
	})
}

func (c *Controller) handleSync(req Request, res Response, actor export.Actor, resolved export.ResolvedExport) {
	if c.runner == nil {
		WriteError(res, export.NewError(export.KindNotImpl, "export runner not configured", nil))
		return
	}
	guard := c.guard
	if guard == nil {
		guard = c.runner.Guard
	}
	if guard != nil {
		if err := guard.AuthorizeExport(req.Context(), actor, resolved.Request, resolved.Definition); err != nil {
			WriteError(res, export.NewError(export.KindAuthz, "export not authorized", err))
			return
		}
	}

	exportID := c.nextID()
	filename := sanitizeFilename(resolved.Filename, resolved.Request.Format)
	setDownloadHeaders(res, exportID, filename, contentTypeForFormat(resolved.Request.Format))

	runReq := resolved.Request
	runReq.Delivery = export.DeliverySync

	run := *c.runner
	run.IDGenerator = func() string { return exportID }
	run.ActorProvider = staticActorProvider{actor: actor}

	if writer, ok := res.Writer(); ok {
		tracker := &trackingWriter{writer: writer}
		runReq.Output = tracker

		_, err := run.Run(req.Context(), runReq)
		if err != nil {
			if !tracker.Written() {
				clearDownloadHeaders(res)
				WriteError(res, err)
				return
			}
			c.logger.Errorf("sync export failed after write: %v", err)
		}
		return
	}

	buffer := newLimitedBuffer(c.maxBufferBytes)
	runReq.Output = buffer

	_, err := run.Run(req.Context(), runReq)
	if err != nil {
		if !buffer.Written() {
			clearDownloadHeaders(res)
			WriteError(res, err)
			return
		}
		c.logger.Errorf("sync export failed after buffer write: %v", err)
	}

	res.WriteHeader(http.StatusOK)
	if _, err := res.Write(buffer.Bytes()); err != nil {
		c.logger.Errorf("sync export buffer write failed: %v", err)
	}
}

func (c *Controller) handleList(req Request, res Response) {
	if c.service == nil {
		WriteError(res, export.NewError(export.KindNotImpl, "export service not configured", nil))
		return
	}
	actor, err := c.actorFromRequest(req)
	if err != nil {
		WriteError(res, err)
		return
	}

	filter, err := parseFilter(req)
	if err != nil {
		WriteError(res, err)
		return
	}

	records, err := c.service.History(req.Context(), actor, filter)
	if err != nil {
		WriteError(res, err)
		return
	}
	writeJSON(res, http.StatusOK, records)
}

func (c *Controller) handleStatus(req Request, res Response, exportID string) {
	if c.service == nil {
		WriteError(res, export.NewError(export.KindNotImpl, "export service not configured", nil))
		return
	}
	actor, err := c.actorFromRequest(req)
	if err != nil {
		WriteError(res, err)
		return
	}

	record, err := c.service.Status(req.Context(), actor, exportID)
	if err != nil {
		WriteError(res, err)
		return
	}
	writeJSON(res, http.StatusOK, record)
}

func (c *Controller) handleDownload(req Request, res Response, exportID string) {
	if c.service == nil {
		WriteError(res, export.NewError(export.KindNotImpl, "export service not configured", nil))
		return
	}
	if c.store == nil {
		WriteError(res, export.NewError(export.KindNotImpl, "artifact store not configured", nil))
		return
	}
	actor, err := c.actorFromRequest(req)
	if err != nil {
		WriteError(res, err)
		return
	}

	info, err := c.service.DownloadMetadata(req.Context(), actor, exportID)
	if err != nil {
		WriteError(res, err)
		return
	}

	ttl := c.signedURLTTL
	if ttl > 0 && !info.Artifact.Meta.ExpiresAt.IsZero() {
		remaining := time.Until(info.Artifact.Meta.ExpiresAt)
		if remaining <= 0 {
			WriteError(res, export.NewError(export.KindValidation, "artifact expired", nil))
			return
		}
		if remaining < ttl {
			ttl = remaining
		}
	}
	if ttl > 0 {
		url, err := c.store.SignedURL(req.Context(), info.Artifact.Key, ttl)
		if err == nil {
			_ = res.Redirect(url, http.StatusFound)
			return
		}
		if exportErr, ok := err.(*export.ExportError); ok && exportErr.Kind != export.KindNotImpl {
			WriteError(res, err)
			return
		}
	}

	reader, meta, err := c.store.Open(req.Context(), info.Artifact.Key)
	if err != nil {
		WriteError(res, err)
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
	setDownloadHeaders(res, info.ExportID, filename, contentType)
	if meta.Size > 0 {
		res.SetHeader("Content-Length", fmt.Sprintf("%d", meta.Size))
	}

	if writer, ok := res.Writer(); ok {
		res.WriteHeader(http.StatusOK)
		if _, err := io.Copy(writer, reader); err != nil {
			c.logger.Errorf("download copy failed: %v", err)
		}
		return
	}

	buffer := newLimitedBuffer(c.maxBufferBytes)
	if _, err := io.Copy(buffer, reader); err != nil {
		WriteError(res, err)
		return
	}

	res.WriteHeader(http.StatusOK)
	if _, err := res.Write(buffer.Bytes()); err != nil {
		c.logger.Errorf("download buffer write failed: %v", err)
	}
}

func (c *Controller) handlePreview(req Request, res Response, exportID string) {
	if c.service == nil {
		WriteError(res, export.NewError(export.KindNotImpl, "export service not configured", nil))
		return
	}
	if c.store == nil {
		WriteError(res, export.NewError(export.KindNotImpl, "artifact store not configured", nil))
		return
	}
	actor, err := c.actorFromRequest(req)
	if err != nil {
		WriteError(res, err)
		return
	}

	info, err := c.service.DownloadMetadata(req.Context(), actor, exportID)
	if err != nil {
		WriteError(res, err)
		return
	}

	reader, meta, err := c.store.Open(req.Context(), info.Artifact.Key)
	if err != nil {
		WriteError(res, err)
		return
	}
	defer reader.Close()

	if meta.ContentType != "" {
		mediaType, _, err := mime.ParseMediaType(meta.ContentType)
		if err != nil {
			mediaType = meta.ContentType
		}
		if mediaType != "text/html" {
			WriteError(res, export.NewError(export.KindValidation, "preview only supports HTML artifacts", nil))
			return
		}
	}

	filename := meta.Filename
	if filename == "" {
		filename = "export-preview.html"
	}
	filename = sanitizeFilename(filename, export.FormatTemplate)

	setPreviewHeaders(res, exportID, filename)
	if meta.Size > 0 {
		res.SetHeader("Content-Length", fmt.Sprintf("%d", meta.Size))
	}

	if writer, ok := res.Writer(); ok {
		res.WriteHeader(http.StatusOK)
		if _, err := io.Copy(writer, reader); err != nil {
			c.logger.Errorf("preview copy failed: %v", err)
		}
		return
	}

	buffer := newLimitedBuffer(c.maxBufferBytes)
	if _, err := io.Copy(buffer, reader); err != nil {
		WriteError(res, err)
		return
	}

	res.WriteHeader(http.StatusOK)
	if _, err := res.Write(buffer.Bytes()); err != nil {
		c.logger.Errorf("preview buffer write failed: %v", err)
	}
}

func (c *Controller) handleDelete(req Request, res Response, exportID string) {
	if c.service == nil {
		WriteError(res, export.NewError(export.KindNotImpl, "export service not configured", nil))
		return
	}
	actor, err := c.actorFromRequest(req)
	if err != nil {
		WriteError(res, err)
		return
	}

	record, err := c.service.Status(req.Context(), actor, exportID)
	if err != nil {
		WriteError(res, err)
		return
	}

	switch record.State {
	case export.StateQueued, export.StateRunning:
		if _, err := c.service.CancelExport(req.Context(), actor, exportID); err != nil {
			WriteError(res, err)
			return
		}
	default:
		if err := c.service.DeleteExport(req.Context(), actor, exportID); err != nil {
			WriteError(res, err)
			return
		}
	}

	res.WriteHeader(http.StatusNoContent)
}

func (c *Controller) actorFromRequest(req Request) (export.Actor, error) {
	if c.actorProvider == nil {
		return export.Actor{}, nil
	}
	actor, err := c.actorProvider.FromContext(req.Context())
	if err != nil {
		return export.Actor{}, export.NewError(export.KindAuthz, "actor resolution failed", err)
	}
	return actor, nil
}

func (c *Controller) resolve(req export.ExportRequest) (export.ResolvedExport, error) {
	if c.runner == nil || c.runner.Definitions == nil {
		return export.ResolvedExport{}, export.NewError(export.KindInternal, "definition registry not configured", nil)
	}
	now := time.Now()
	if c.runner.Now != nil {
		now = c.runner.Now()
	}
	def, err := c.runner.Definitions.Resolve(req)
	if err != nil {
		return export.ResolvedExport{}, err
	}
	return export.ResolveExport(req, def, now)
}

func (c *Controller) statusURL(exportID string) string {
	return path.Join(c.basePath, exportID)
}

func (c *Controller) downloadURL(exportID string) string {
	return path.Join(c.basePath, exportID, "download")
}

func (c *Controller) nextID() string {
	if c.idGenerator == nil {
		c.idGenerator = defaultIDGenerator()
	}
	return c.idGenerator()
}

func (c *Controller) idempotencySignature(key string, actor export.Actor, req export.ExportRequest) string {
	return buildIdempotencyKey(key, actor, req)
}

func (c *Controller) deliveryPolicyForRequest() export.DeliveryPolicy {
	if !isZeroDeliveryPolicy(c.deliveryPolicy) {
		return c.deliveryPolicy
	}
	if c.runner != nil {
		return c.runner.DeliveryPolicy
	}
	return export.DeliveryPolicy{}
}

func writeNotFound(res Response) {
	res.SetHeader("Content-Type", "text/plain; charset=utf-8")
	res.SetHeader("X-Content-Type-Options", "nosniff")
	res.WriteHeader(http.StatusNotFound)
	_, _ = res.Write([]byte("404 page not found\n"))
}

func WriteError(res Response, err error) {
	if err == nil {
		res.WriteHeader(http.StatusNoContent)
		return
	}
	ge := export.AsGoError(err)
	status := statusForError(ge)
	payload := ErrorResponse{
		Error: ErrorBody{
			Message: ge.Message,
			Code:    ge.TextCode,
		},
	}
	writeJSON(res, status, payload)
}

func writeJSON(res Response, status int, payload any) {
	_ = res.WriteJSON(status, payload)
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

func parseFilter(req Request) (export.ProgressFilter, error) {
	filter := export.ProgressFilter{
		Definition: req.Query("definition"),
		State:      export.ExportState(req.Query("state")),
	}
	if since := req.Query("since"); since != "" {
		ts, err := time.Parse(time.RFC3339, since)
		if err != nil {
			return export.ProgressFilter{}, export.NewError(export.KindValidation, "invalid since timestamp", err)
		}
		filter.Since = ts
	}
	if until := req.Query("until"); until != "" {
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

func setDownloadHeaders(res Response, exportID, filename, contentType string) {
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	res.SetHeader("Content-Type", contentType)
	res.SetHeader("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", filename))
	if exportID != "" {
		res.SetHeader("X-Export-Id", exportID)
	}
}

func setPreviewHeaders(res Response, exportID, filename string) {
	res.SetHeader("Content-Type", "text/html; charset=utf-8")
	res.SetHeader("Content-Disposition", fmt.Sprintf("inline; filename=\"%s\"", filename))
	if exportID != "" {
		res.SetHeader("X-Export-Id", exportID)
	}
}

func clearDownloadHeaders(res Response) {
	res.DelHeader("Content-Disposition")
	res.DelHeader("Content-Type")
	res.DelHeader("X-Export-Id")
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
	writer  io.Writer
	written bool
}

func (w *trackingWriter) Write(p []byte) (int, error) {
	w.written = true
	return w.writer.Write(p)
}

func (w *trackingWriter) Written() bool {
	return w.written
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

type limitedBuffer struct {
	buf     bytes.Buffer
	maxSize int64
	written bool
}

func newLimitedBuffer(maxSize int64) *limitedBuffer {
	if maxSize <= 0 {
		maxSize = DefaultMaxBufferBytes
	}
	return &limitedBuffer{maxSize: maxSize}
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	if b.maxSize > 0 && int64(b.buf.Len()+len(p)) > b.maxSize {
		return 0, export.NewError(export.KindInternal, "buffer limit exceeded", nil)
	}
	b.written = true
	return b.buf.Write(p)
}

func (b *limitedBuffer) Bytes() []byte {
	return b.buf.Bytes()
}

func (b *limitedBuffer) Written() bool {
	return b.written
}

func (b *limitedBuffer) Len() int {
	return b.buf.Len()
}
