package exportrouter

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/goliatone/go-export/adapters/exportapi"
	exporthttp "github.com/goliatone/go-export/adapters/http"
	"github.com/goliatone/go-export/export"
	"github.com/goliatone/go-router"
)

type stubSource struct {
	rows []export.Row
}

func (s *stubSource) Open(ctx context.Context, spec export.RowSourceSpec) (export.RowIterator, error) {
	_ = ctx
	_ = spec
	return &stubIterator{rows: s.rows}, nil
}

type stubIterator struct {
	rows []export.Row
	idx  int
}

func (it *stubIterator) Next(ctx context.Context) (export.Row, error) {
	_ = ctx
	if it.idx >= len(it.rows) {
		return nil, io.EOF
	}
	row := it.rows[it.idx]
	it.idx++
	return row, nil
}

func (it *stubIterator) Close() error { return nil }

func newTestRunner(t *testing.T) *export.Runner {
	t.Helper()
	runner := export.NewRunner()
	if err := runner.Definitions.Register(export.ExportDefinition{
		Name:         "users",
		RowSourceKey: "stub",
		Schema: export.Schema{Columns: []export.Column{
			{Name: "id"},
			{Name: "name"},
		}},
	}); err != nil {
		t.Fatalf("register definition: %v", err)
	}
	if err := runner.RowSources.Register("stub", func(req export.ExportRequest, def export.ResolvedDefinition) (export.RowSource, error) {
		_ = req
		_ = def
		return &stubSource{rows: []export.Row{{"1", "alice"}}}, nil
	}); err != nil {
		t.Fatalf("register source: %v", err)
	}
	return runner
}

func newTestService(runner *export.Runner, id string) (export.Service, export.ProgressTracker, export.ArtifactStore) {
	tracker := export.NewMemoryTracker()
	store := export.NewMemoryStore()
	svc := export.NewService(export.ServiceConfig{
		Runner:         runner,
		Tracker:        tracker,
		Store:          store,
		DeliveryPolicy: export.DeliveryPolicy{Default: export.DeliveryAsync},
		IDGenerator: func() string {
			return id
		},
	})
	return svc, tracker, store
}

func seedPreviewRecord(t *testing.T, tracker export.ProgressTracker, store export.ArtifactStore, exportID string, state export.ExportState, contentType string) {
	t.Helper()
	ctx := context.Background()
	ref := export.ArtifactRef{}
	if state == export.StateCompleted {
		var err error
		ref, err = store.Put(ctx, "exports/"+exportID+".html", bytes.NewBufferString("<html><body>preview</body></html>"), export.ArtifactMeta{
			Filename:    "export-preview.html",
			ContentType: contentType,
		})
		if err != nil {
			t.Fatalf("store put: %v", err)
		}
	}
	if _, err := tracker.Start(ctx, export.ExportRecord{
		ID:         exportID,
		Definition: "users",
		Format:     export.FormatTemplate,
		State:      state,
		Artifact:   ref,
	}); err != nil {
		t.Fatalf("tracker start: %v", err)
	}
}

func assertErrorParity(t *testing.T, rec *httptest.ResponseRecorder, routerRec *httptest.ResponseRecorder) {
	t.Helper()
	if rec.Code != routerRec.Code {
		t.Fatalf("status mismatch: http=%d router=%d", rec.Code, routerRec.Code)
	}
	if rec.Header().Get("Content-Type") != routerRec.Header().Get("Content-Type") {
		t.Fatalf("content-type mismatch: http=%q router=%q", rec.Header().Get("Content-Type"), routerRec.Header().Get("Content-Type"))
	}
	var httpPayload exportapi.ErrorResponse
	if err := json.NewDecoder(bytes.NewReader(rec.Body.Bytes())).Decode(&httpPayload); err != nil {
		t.Fatalf("decode http response: %v", err)
	}
	var routerPayload exportapi.ErrorResponse
	if err := json.NewDecoder(bytes.NewReader(routerRec.Body.Bytes())).Decode(&routerPayload); err != nil {
		t.Fatalf("decode router response: %v", err)
	}
	if httpPayload != routerPayload {
		t.Fatalf("payload mismatch: http=%+v router=%+v", httpPayload, routerPayload)
	}
}

func TestTransportParity_SyncExport(t *testing.T) {
	runner := newTestRunner(t)
	actor := export.Actor{ID: "user-1"}

	cfg := exportapi.Config{
		Runner:        runner,
		ActorProvider: exporthttp.StaticActorProvider{Actor: actor},
		IDGenerator: func() string {
			return "exp-sync"
		},
	}

	httpHandler := exporthttp.NewHandler(cfg)
	routerHandler := NewHandler(cfg)

	body := `{"definition":"users","format":"csv","delivery":"sync"}`

	req := httptest.NewRequest(http.MethodPost, "/admin/exports", strings.NewReader(body))
	rec := httptest.NewRecorder()
	httpHandler.ServeHTTP(rec, req)

	routerCtx := newTestHTTPContext(http.MethodPost, "/admin/exports", []byte(body), nil, nil)
	if err := routerHandler.Handle(routerCtx); err != nil {
		t.Fatalf("router handle: %v", err)
	}

	if rec.Code != routerCtx.recorder.Code {
		t.Fatalf("status mismatch: http=%d router=%d", rec.Code, routerCtx.recorder.Code)
	}
	if rec.Header().Get("Content-Type") != routerCtx.recorder.Header().Get("Content-Type") {
		t.Fatalf("content-type mismatch: http=%q router=%q", rec.Header().Get("Content-Type"), routerCtx.recorder.Header().Get("Content-Type"))
	}
	if rec.Header().Get("Content-Disposition") != routerCtx.recorder.Header().Get("Content-Disposition") {
		t.Fatalf("content-disposition mismatch: http=%q router=%q", rec.Header().Get("Content-Disposition"), routerCtx.recorder.Header().Get("Content-Disposition"))
	}
	if rec.Header().Get("X-Export-Id") != routerCtx.recorder.Header().Get("X-Export-Id") {
		t.Fatalf("export id mismatch: http=%q router=%q", rec.Header().Get("X-Export-Id"), routerCtx.recorder.Header().Get("X-Export-Id"))
	}
	if rec.Body.String() != routerCtx.recorder.Body.String() {
		t.Fatalf("body mismatch: http=%q router=%q", rec.Body.String(), routerCtx.recorder.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "id,name") {
		t.Fatalf("expected csv content, got %q", rec.Body.String())
	}
	if routerCtx.sendCalled {
		t.Fatalf("expected streaming response, got buffered send")
	}
}

func TestTransportParity_AsyncExport(t *testing.T) {
	runnerHTTP := newTestRunner(t)
	runnerRouter := newTestRunner(t)
	actor := export.Actor{ID: "user-1"}

	svcHTTP, _, storeHTTP := newTestService(runnerHTTP, "exp-async")
	svcRouter, _, storeRouter := newTestService(runnerRouter, "exp-async")

	cfgHTTP := exportapi.Config{
		Service:       svcHTTP,
		Runner:        runnerHTTP,
		Store:         storeHTTP,
		ActorProvider: exporthttp.StaticActorProvider{Actor: actor},
	}
	cfgRouter := exportapi.Config{
		Service:       svcRouter,
		Runner:        runnerRouter,
		Store:         storeRouter,
		ActorProvider: exporthttp.StaticActorProvider{Actor: actor},
	}

	httpHandler := exporthttp.NewHandler(cfgHTTP)
	routerHandler := NewHandler(cfgRouter)

	body := `{"definition":"users","format":"csv","delivery":"async"}`
	req := httptest.NewRequest(http.MethodPost, "/admin/exports", strings.NewReader(body))
	rec := httptest.NewRecorder()
	httpHandler.ServeHTTP(rec, req)

	routerCtx := newTestHTTPContext(http.MethodPost, "/admin/exports", []byte(body), nil, nil)
	if err := routerHandler.Handle(routerCtx); err != nil {
		t.Fatalf("router handle: %v", err)
	}

	var httpPayload exportapi.AsyncResponse
	if err := json.NewDecoder(bytes.NewReader(rec.Body.Bytes())).Decode(&httpPayload); err != nil {
		t.Fatalf("decode http response: %v", err)
	}

	var routerPayload exportapi.AsyncResponse
	if err := json.NewDecoder(bytes.NewReader(routerCtx.recorder.Body.Bytes())).Decode(&routerPayload); err != nil {
		t.Fatalf("decode router response: %v", err)
	}

	if rec.Code != routerCtx.recorder.Code {
		t.Fatalf("status mismatch: http=%d router=%d", rec.Code, routerCtx.recorder.Code)
	}
	if httpPayload != routerPayload {
		t.Fatalf("payload mismatch: http=%+v router=%+v", httpPayload, routerPayload)
	}
	if rec.Header().Get("Content-Type") != routerCtx.recorder.Header().Get("Content-Type") {
		t.Fatalf("content-type mismatch: http=%q router=%q", rec.Header().Get("Content-Type"), routerCtx.recorder.Header().Get("Content-Type"))
	}
}

func TestTransportParity_Download(t *testing.T) {
	actor := export.Actor{ID: "user-1"}

	serviceHTTP, trackerHTTP, storeHTTP := newTestService(export.NewRunner(), "exp-download")
	serviceRouter, trackerRouter, storeRouter := newTestService(export.NewRunner(), "exp-download")

	refHTTP, err := storeHTTP.Put(context.Background(), "exports/exp-download.csv", bytes.NewBufferString("id,name\n1,alice\n"), export.ArtifactMeta{
		Filename:    "users.csv",
		ContentType: "text/csv",
	})
	if err != nil {
		t.Fatalf("store put: %v", err)
	}
	if _, err := trackerHTTP.Start(context.Background(), export.ExportRecord{
		ID:         "exp-download",
		Definition: "users",
		Format:     export.FormatCSV,
		State:      export.StateCompleted,
		Artifact:   refHTTP,
	}); err != nil {
		t.Fatalf("tracker start: %v", err)
	}

	refRouter, err := storeRouter.Put(context.Background(), "exports/exp-download.csv", bytes.NewBufferString("id,name\n1,alice\n"), export.ArtifactMeta{
		Filename:    "users.csv",
		ContentType: "text/csv",
	})
	if err != nil {
		t.Fatalf("store put: %v", err)
	}
	if _, err := trackerRouter.Start(context.Background(), export.ExportRecord{
		ID:         "exp-download",
		Definition: "users",
		Format:     export.FormatCSV,
		State:      export.StateCompleted,
		Artifact:   refRouter,
	}); err != nil {
		t.Fatalf("tracker start: %v", err)
	}

	cfgHTTP := exportapi.Config{
		Service:       serviceHTTP,
		Store:         storeHTTP,
		ActorProvider: exporthttp.StaticActorProvider{Actor: actor},
	}
	cfgRouter := exportapi.Config{
		Service:       serviceRouter,
		Store:         storeRouter,
		ActorProvider: exporthttp.StaticActorProvider{Actor: actor},
	}

	httpHandler := exporthttp.NewHandler(cfgHTTP)
	routerHandler := NewHandler(cfgRouter)

	req := httptest.NewRequest(http.MethodGet, "/admin/exports/exp-download/download", nil)
	rec := httptest.NewRecorder()
	httpHandler.ServeHTTP(rec, req)

	routerCtx := newTestHTTPContext(http.MethodGet, "/admin/exports/exp-download/download", nil, nil, nil)
	if err := routerHandler.Handle(routerCtx); err != nil {
		t.Fatalf("router handle: %v", err)
	}

	if rec.Code != routerCtx.recorder.Code {
		t.Fatalf("status mismatch: http=%d router=%d", rec.Code, routerCtx.recorder.Code)
	}
	if rec.Header().Get("Content-Type") != routerCtx.recorder.Header().Get("Content-Type") {
		t.Fatalf("content-type mismatch: http=%q router=%q", rec.Header().Get("Content-Type"), routerCtx.recorder.Header().Get("Content-Type"))
	}
	if rec.Header().Get("Content-Disposition") != routerCtx.recorder.Header().Get("Content-Disposition") {
		t.Fatalf("content-disposition mismatch: http=%q router=%q", rec.Header().Get("Content-Disposition"), routerCtx.recorder.Header().Get("Content-Disposition"))
	}
	if rec.Body.String() != routerCtx.recorder.Body.String() {
		t.Fatalf("body mismatch: http=%q router=%q", rec.Body.String(), routerCtx.recorder.Body.String())
	}
}

func TestTransportParity_Preview(t *testing.T) {
	actor := export.Actor{ID: "user-1"}

	t.Run("ok", func(t *testing.T) {
		serviceHTTP, trackerHTTP, storeHTTP := newTestService(export.NewRunner(), "exp-preview")
		serviceRouter, trackerRouter, storeRouter := newTestService(export.NewRunner(), "exp-preview")

		seedPreviewRecord(t, trackerHTTP, storeHTTP, "exp-preview", export.StateCompleted, "text/html")
		seedPreviewRecord(t, trackerRouter, storeRouter, "exp-preview", export.StateCompleted, "text/html")

		cfgHTTP := exportapi.Config{
			Service:       serviceHTTP,
			Store:         storeHTTP,
			ActorProvider: exporthttp.StaticActorProvider{Actor: actor},
		}
		cfgRouter := exportapi.Config{
			Service:       serviceRouter,
			Store:         storeRouter,
			ActorProvider: exporthttp.StaticActorProvider{Actor: actor},
		}

		httpHandler := exporthttp.NewHandler(cfgHTTP)
		routerHandler := NewHandler(cfgRouter)

		req := httptest.NewRequest(http.MethodGet, "/admin/exports/exp-preview/preview", nil)
		rec := httptest.NewRecorder()
		httpHandler.ServeHTTP(rec, req)

		routerCtx := newTestHTTPContext(http.MethodGet, "/admin/exports/exp-preview/preview", nil, nil, nil)
		if err := routerHandler.Handle(routerCtx); err != nil {
			t.Fatalf("router handle: %v", err)
		}

		if rec.Code != routerCtx.recorder.Code {
			t.Fatalf("status mismatch: http=%d router=%d", rec.Code, routerCtx.recorder.Code)
		}
		if rec.Header().Get("Content-Type") != routerCtx.recorder.Header().Get("Content-Type") {
			t.Fatalf("content-type mismatch: http=%q router=%q", rec.Header().Get("Content-Type"), routerCtx.recorder.Header().Get("Content-Type"))
		}
		if rec.Header().Get("Content-Disposition") != routerCtx.recorder.Header().Get("Content-Disposition") {
			t.Fatalf("content-disposition mismatch: http=%q router=%q", rec.Header().Get("Content-Disposition"), routerCtx.recorder.Header().Get("Content-Disposition"))
		}
		if rec.Body.String() != routerCtx.recorder.Body.String() {
			t.Fatalf("body mismatch: http=%q router=%q", rec.Body.String(), routerCtx.recorder.Body.String())
		}
		if !strings.Contains(rec.Body.String(), "<html") {
			t.Fatalf("expected html content, got %q", rec.Body.String())
		}
	})

	t.Run("non-html", func(t *testing.T) {
		serviceHTTP, trackerHTTP, storeHTTP := newTestService(export.NewRunner(), "exp-preview")
		serviceRouter, trackerRouter, storeRouter := newTestService(export.NewRunner(), "exp-preview")

		seedPreviewRecord(t, trackerHTTP, storeHTTP, "exp-preview", export.StateCompleted, "text/csv")
		seedPreviewRecord(t, trackerRouter, storeRouter, "exp-preview", export.StateCompleted, "text/csv")

		cfgHTTP := exportapi.Config{
			Service:       serviceHTTP,
			Store:         storeHTTP,
			ActorProvider: exporthttp.StaticActorProvider{Actor: actor},
		}
		cfgRouter := exportapi.Config{
			Service:       serviceRouter,
			Store:         storeRouter,
			ActorProvider: exporthttp.StaticActorProvider{Actor: actor},
		}

		httpHandler := exporthttp.NewHandler(cfgHTTP)
		routerHandler := NewHandler(cfgRouter)

		req := httptest.NewRequest(http.MethodGet, "/admin/exports/exp-preview/preview", nil)
		rec := httptest.NewRecorder()
		httpHandler.ServeHTTP(rec, req)

		routerCtx := newTestHTTPContext(http.MethodGet, "/admin/exports/exp-preview/preview", nil, nil, nil)
		if err := routerHandler.Handle(routerCtx); err != nil {
			t.Fatalf("router handle: %v", err)
		}

		assertErrorParity(t, rec, routerCtx.recorder)
	})

	t.Run("not-completed", func(t *testing.T) {
		serviceHTTP, trackerHTTP, storeHTTP := newTestService(export.NewRunner(), "exp-preview")
		serviceRouter, trackerRouter, storeRouter := newTestService(export.NewRunner(), "exp-preview")

		seedPreviewRecord(t, trackerHTTP, storeHTTP, "exp-preview", export.StateRunning, "text/html")
		seedPreviewRecord(t, trackerRouter, storeRouter, "exp-preview", export.StateRunning, "text/html")

		cfgHTTP := exportapi.Config{
			Service:       serviceHTTP,
			Store:         storeHTTP,
			ActorProvider: exporthttp.StaticActorProvider{Actor: actor},
		}
		cfgRouter := exportapi.Config{
			Service:       serviceRouter,
			Store:         storeRouter,
			ActorProvider: exporthttp.StaticActorProvider{Actor: actor},
		}

		httpHandler := exporthttp.NewHandler(cfgHTTP)
		routerHandler := NewHandler(cfgRouter)

		req := httptest.NewRequest(http.MethodGet, "/admin/exports/exp-preview/preview", nil)
		rec := httptest.NewRecorder()
		httpHandler.ServeHTTP(rec, req)

		routerCtx := newTestHTTPContext(http.MethodGet, "/admin/exports/exp-preview/preview", nil, nil, nil)
		if err := routerHandler.Handle(routerCtx); err != nil {
			t.Fatalf("router handle: %v", err)
		}

		assertErrorParity(t, rec, routerCtx.recorder)
	})
}

func TestRouterBufferedFallback(t *testing.T) {
	runner := newTestRunner(t)
	actor := export.Actor{ID: "user-1"}

	cfg := exportapi.Config{
		Runner:         runner,
		ActorProvider:  exporthttp.StaticActorProvider{Actor: actor},
		MaxBufferBytes: 1024,
		IDGenerator: func() string {
			return "exp-buffer"
		},
	}

	handler := NewHandler(cfg)
	body := `{"definition":"users","format":"csv","delivery":"sync"}`
	ctx := newTestContext(http.MethodPost, "/admin/exports", []byte(body), nil, nil)

	if err := handler.Handle(ctx); err != nil {
		t.Fatalf("router handle: %v", err)
	}

	if ctx.recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", ctx.recorder.Code)
	}
	if !ctx.sendCalled {
		t.Fatalf("expected buffered send when HTTPContext is unavailable")
	}
	if !strings.Contains(ctx.recorder.Body.String(), "id,name") {
		t.Fatalf("expected csv content, got %q", ctx.recorder.Body.String())
	}
}

type testContext struct {
	method        string
	path          string
	body          []byte
	query         map[string]string
	headers       map[string]string
	params        map[string]string
	locals        map[any]any
	ctx           context.Context
	recorder      *httptest.ResponseRecorder
	statusWritten bool
	status        int
	sendCalled    bool
}

func newTestContext(method, path string, body []byte, headers map[string]string, query map[string]string) *testContext {
	if headers == nil {
		headers = make(map[string]string)
	}
	if query == nil {
		query = make(map[string]string)
	}
	return &testContext{
		method:   method,
		path:     path,
		body:     body,
		query:    query,
		headers:  headers,
		params:   make(map[string]string),
		locals:   make(map[any]any),
		ctx:      context.Background(),
		recorder: httptest.NewRecorder(),
	}
}

func (c *testContext) Bind(v any) error {
	if len(c.body) == 0 {
		return nil
	}
	return json.Unmarshal(c.body, v)
}

func (c *testContext) Context() context.Context {
	if c.ctx == nil {
		return context.Background()
	}
	return c.ctx
}

func (c *testContext) SetContext(ctx context.Context) {
	c.ctx = ctx
}

func (c *testContext) Next() error { return nil }

func (c *testContext) RouteName() string { return "" }

func (c *testContext) RouteParams() map[string]string { return c.params }

func (c *testContext) Method() string { return c.method }

func (c *testContext) Path() string { return c.path }

func (c *testContext) Param(name string, defaultValue ...string) string {
	if val, ok := c.params[name]; ok {
		return val
	}
	if len(defaultValue) > 0 {
		return defaultValue[0]
	}
	return ""
}

func (c *testContext) ParamsInt(key string, defaultValue int) int {
	val := c.Param(key)
	if val == "" {
		return defaultValue
	}
	parsed, err := strconv.Atoi(val)
	if err != nil {
		return defaultValue
	}
	return parsed
}

func (c *testContext) Query(name string, defaultValue ...string) string {
	if val, ok := c.query[name]; ok {
		return val
	}
	if len(defaultValue) > 0 {
		return defaultValue[0]
	}
	return ""
}

func (c *testContext) QueryValues(name string) []string {
	if val, ok := c.query[name]; ok {
		return []string{val}
	}
	return nil
}

func (c *testContext) QueryInt(name string, defaultValue int) int {
	val := c.Query(name)
	if val == "" {
		return defaultValue
	}
	parsed, err := strconv.Atoi(val)
	if err != nil {
		return defaultValue
	}
	return parsed
}

func (c *testContext) Queries() map[string]string { return c.query }

func (c *testContext) Body() []byte { return c.body }

func (c *testContext) Locals(key any, value ...any) any {
	if len(value) > 0 {
		c.locals[key] = value[0]
		return value[0]
	}
	return c.locals[key]
}

func (c *testContext) LocalsMerge(key any, value map[string]any) map[string]any {
	merged, _ := c.locals[key].(map[string]any)
	if merged == nil {
		merged = map[string]any{}
	}
	for k, v := range value {
		merged[k] = v
	}
	c.locals[key] = merged
	return merged
}

func (c *testContext) Render(name string, bind any, layouts ...string) error {
	return nil
}

func (c *testContext) Cookie(cookie *router.Cookie) {}

func (c *testContext) Cookies(key string, defaultValue ...string) string {
	if len(defaultValue) > 0 {
		return defaultValue[0]
	}
	return ""
}

func (c *testContext) CookieParser(out any) error { return nil }

func (c *testContext) Redirect(location string, status ...int) error {
	code := http.StatusFound
	if len(status) > 0 {
		code = status[0]
	}
	c.SetHeader("Location", location)
	c.writeHeader(code)
	return nil
}

func (c *testContext) RedirectToRoute(routeName string, params router.ViewContext, status ...int) error {
	return nil
}

func (c *testContext) RedirectBack(fallback string, status ...int) error {
	return nil
}

func (c *testContext) Header(name string) string {
	return c.headers[name]
}

func (c *testContext) Referer() string { return "" }

func (c *testContext) OriginalURL() string { return c.path }

func (c *testContext) FormFile(key string) (*multipart.FileHeader, error) {
	return nil, nil
}

func (c *testContext) FormValue(key string, defaultValue ...string) string {
	if len(defaultValue) > 0 {
		return defaultValue[0]
	}
	return ""
}

func (c *testContext) IP() string { return "127.0.0.1" }

func (c *testContext) Status(code int) router.Context {
	c.writeHeader(code)
	return c
}

func (c *testContext) Send(body []byte) error {
	c.sendCalled = true
	if !c.statusWritten {
		c.writeHeader(http.StatusOK)
	}
	_, err := c.recorder.Write(body)
	return err
}

func (c *testContext) SendString(body string) error {
	return c.Send([]byte(body))
}

func (c *testContext) SendStatus(code int) error {
	c.writeHeader(code)
	return nil
}

func (c *testContext) JSON(code int, v any) error {
	c.recorder.Header().Set("Content-Type", "application/json")
	c.writeHeader(code)
	return json.NewEncoder(c.recorder).Encode(v)
}

func (c *testContext) SendStream(r io.Reader) error {
	if !c.statusWritten {
		c.writeHeader(http.StatusOK)
	}
	_, err := io.Copy(c.recorder, r)
	return err
}

func (c *testContext) NoContent(code int) error {
	c.writeHeader(code)
	return nil
}

func (c *testContext) SetHeader(key, val string) router.Context {
	c.recorder.Header().Set(key, val)
	return c
}

func (c *testContext) Set(key string, value any) {
	c.locals[key] = value
}

func (c *testContext) Get(key string, def any) any {
	if val, ok := c.locals[key]; ok {
		return val
	}
	return def
}

func (c *testContext) GetString(key string, def string) string {
	if val, ok := c.locals[key]; ok {
		if str, ok := val.(string); ok {
			return str
		}
	}
	return def
}

func (c *testContext) GetInt(key string, def int) int {
	if val, ok := c.locals[key]; ok {
		if num, ok := val.(int); ok {
			return num
		}
	}
	return def
}

func (c *testContext) GetBool(key string, def bool) bool {
	if val, ok := c.locals[key]; ok {
		if flag, ok := val.(bool); ok {
			return flag
		}
	}
	return def
}

func (c *testContext) writeHeader(code int) {
	if c.statusWritten {
		c.status = code
		return
	}
	c.statusWritten = true
	c.status = code
	c.recorder.WriteHeader(code)
}

type testHTTPContext struct {
	*testContext
	req *http.Request
}

func newTestHTTPContext(method, path string, body []byte, headers map[string]string, query map[string]string) *testHTTPContext {
	base := newTestContext(method, path, body, headers, query)
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	for key, value := range headers {
		req.Header.Set(key, value)
		base.headers[key] = value
	}
	base.ctx = req.Context()
	return &testHTTPContext{testContext: base, req: req}
}

func (c *testHTTPContext) Request() *http.Request { return c.req }

func (c *testHTTPContext) Response() http.ResponseWriter { return c.recorder }

var _ router.Context = (*testContext)(nil)
var _ router.Context = (*testHTTPContext)(nil)
var _ router.HTTPContext = (*testHTTPContext)(nil)
