package verification

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	exportactivity "github.com/goliatone/go-export/adapters/activity"
	exporthttp "github.com/goliatone/go-export/adapters/http"
	trackerbun "github.com/goliatone/go-export/adapters/tracker/bun"
	"github.com/goliatone/go-export/export"
	exportsql "github.com/goliatone/go-export/sources/sql"
	"github.com/goliatone/go-users/activity"
	"github.com/goliatone/go-users/groups"
	"github.com/goliatone/go-users/pkg/types"
	"github.com/google/uuid"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"
	"github.com/xuri/excelize/v2"
)

type groupKey struct{}
type actorKey struct{}
type pageKey struct{}
type page struct{ Limit, Offset int }

var groupA = uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
var groupB = uuid.MustParse("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
var actorID = uuid.MustParse("11111111-1111-4111-8111-111111111111")

func groupFrom(ctx context.Context) (uuid.UUID, error) {
	g, _ := ctx.Value(groupKey{}).(uuid.UUID)
	if g == uuid.Nil {
		return g, groups.ErrDenied
	}
	return g, nil
}
func actorFrom(ctx context.Context) (export.Actor, error) {
	id, _ := ctx.Value(actorKey{}).(uuid.UUID)
	g, err := groupFrom(ctx)
	if err != nil || id == uuid.Nil {
		return export.Actor{}, groups.ErrDenied
	}
	return export.Actor{ID: id.String(), Details: map[string]any{"group_id": g.String()}}, nil
}

type actorProvider struct{}

func (actorProvider) FromContext(ctx context.Context) (export.Actor, error) { return actorFrom(ctx) }
func scopedContext(g uuid.UUID) context.Context {
	return context.WithValue(context.WithValue(context.Background(), groupKey{}, g), actorKey{}, actorID)
}

type guard struct {
	f  *nativeFixture
	tr *scopedTracker
}

func (g guard) AuthorizeExport(ctx context.Context, a export.Actor, _ export.ExportRequest, _ export.ResolvedDefinition) error {
	trusted, err := actorFrom(ctx)
	if err != nil || trusted.ID != a.ID || a.Details["group_id"] != trusted.Details["group_id"] || a.Scope.TenantID != "" || a.Scope.WorkspaceID != "" {
		return groups.ErrDenied
	}
	id, _ := groupFrom(ctx)
	native, err := g.f.service.Guard()
	if err != nil {
		return err
	}
	_, err = native.Enforce(ctx, types.ActorRef{ID: actorID, Type: "user"}, groups.Scope(id), "export.read", uuid.Nil)
	return err
}
func (g guard) AuthorizeDownload(ctx context.Context, a export.Actor, id string) error {
	if err := g.AuthorizeExport(ctx, a, export.ExportRequest{}, export.ResolvedDefinition{}); err != nil {
		return err
	}
	return g.tr.owns(ctx, id)
}

type scopedTracker struct {
	*trackerbun.Tracker
	postgres bool
	lists    atomic.Int64
}

func (s *scopedTracker) predicate() string {
	if s.postgres {
		return "convert_from(requested_by_details, 'UTF8')::jsonb ->> 'group_id' = ?"
	}
	return "json_extract(requested_by_details, '$.group_id') = ?"
}
func (s *scopedTracker) owns(ctx context.Context, id string) error {
	g, e := groupFrom(ctx)
	if e != nil {
		return e
	}
	var v string
	return s.DB.NewSelect().Table("export_records").Column("id").Where("id = ?", id).Where(s.predicate(), g.String()).Scan(ctx, &v)
}
func (s *scopedTracker) Status(ctx context.Context, id string) (export.ExportRecord, error) {
	if e := s.owns(ctx, id); e != nil {
		return export.ExportRecord{}, groups.ErrDenied
	}
	return s.Tracker.Status(ctx, id)
}
func (s *scopedTracker) List(ctx context.Context, f export.ProgressFilter) ([]export.ExportRecord, error) {
	g, e := groupFrom(ctx)
	if e != nil {
		return nil, e
	}
	s.lists.Add(1)
	ids := []string{}
	q := s.DB.NewSelect().Table("export_records").Column("id").Where(s.predicate(), g.String())
	if f.Definition != "" {
		q = q.Where("definition = ?", f.Definition)
	}
	if f.State != "" {
		q = q.Where("state = ?", f.State)
	}
	if !f.Since.IsZero() {
		q = q.Where("created_at >= ?", f.Since)
	}
	if !f.Until.IsZero() {
		q = q.Where("created_at <= ?", f.Until)
	}
	q = q.Order("created_at DESC", "id ASC")
	if p, ok := ctx.Value(pageKey{}).(page); ok {
		q = q.Limit(p.Limit).Offset(p.Offset)
	}
	if e = q.Scan(ctx, &ids); e != nil {
		return nil, e
	}
	out := []export.ExportRecord{}
	for _, id := range ids {
		r, e := s.Tracker.Status(ctx, id)
		if e != nil {
			return nil, e
		}
		out = append(out, r)
	}
	return out, nil
}

// List's external authorization is checked before this native tracker wrapper.
// It predicates the trusted Group in SQL before pagination and native record reads.
type protectedService struct {
	export.Service
	guard guard
}

func (s protectedService) History(ctx context.Context, a export.Actor, f export.ProgressFilter) ([]export.ExportRecord, error) {
	if e := s.guard.AuthorizeExport(ctx, a, export.ExportRequest{}, export.ResolvedDefinition{}); e != nil {
		return nil, export.NewError(export.KindAuthz, "export not authorized", e)
	}
	return s.Service.History(ctx, a, f)
}

type sqlExecutor struct {
	db    *bun.DB
	guard guard
	total atomic.Int64
	calls atomic.Int64
	fail  atomic.Bool
}

func (e *sqlExecutor) Query(ctx context.Context, s exportsql.QuerySpec) (export.RowIterator, error) {
	if err := e.guard.AuthorizeExport(ctx, s.Actor, export.ExportRequest{}, export.ResolvedDefinition{}); err != nil {
		return nil, err
	}
	e.calls.Add(1)
	if e.fail.Load() {
		return nil, errors.New("private Group B secret=do-not-expose")
	}
	g, _ := groupFrom(ctx)
	params, _ := s.Params.(map[string]any)
	limit, offset := 2, 0
	if v, ok := params["limit"].(float64); ok {
		limit = int(v)
	}
	if v, ok := params["offset"].(float64); ok {
		offset = int(v)
	}
	if limit < 1 || limit > 10 || offset < 0 {
		return nil, export.NewError(export.KindValidation, "invalid bounded page", nil)
	}
	ids, _ := params["ids"].([]string)
	where := "group_id = ?"
	args := []any{g.String()}
	if len(ids) > 0 {
		where += " AND id IN (?)"
		args = append(args, bun.In(ids))
	}
	var total int64
	if err := e.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM export_resources WHERE "+where, args...).Scan(&total); err != nil {
		return nil, err
	}
	e.total.Store(total)
	args = append(args, limit, offset)
	rows, err := e.db.QueryContext(ctx, "SELECT id,label,secret FROM export_resources WHERE "+where+" ORDER BY id LIMIT ? OFFSET ?", args...)
	if err != nil {
		return nil, err
	}
	return &sqlRows{rows: rows, columns: s.Columns}, nil
}

type sqlRows struct {
	rows    *sql.Rows
	columns []export.Column
}

func (r *sqlRows) Next(ctx context.Context) (export.Row, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !r.rows.Next() {
		if e := r.rows.Err(); e != nil {
			return nil, e
		}
		return nil, io.EOF
	}
	var id, label, secret string
	if e := r.rows.Scan(&id, &label, &secret); e != nil {
		return nil, e
	}
	m := map[string]any{"id": id, "label": label, "secret": secret}
	out := export.Row{}
	for _, c := range r.columns {
		out = append(out, m[c.Name])
	}
	return out, nil
}
func (r *sqlRows) Close() error { return r.rows.Close() }

type scopedSink struct{ repo *activity.Repository }

func (s scopedSink) Log(ctx context.Context, r types.ActivityRecord) error {
	g, e := groupFrom(ctx)
	if e != nil {
		return e
	}
	r.Labels = groups.Scope(g).Labels
	r.Data = map[string]any{"kind": "export-operation"}
	return s.repo.Log(ctx, r)
}

type host struct {
	f        *nativeFixture
	tr       *scopedTracker
	runner   *export.Runner
	svc      export.Service
	store    *watchedStore
	exec     *sqlExecutor
	handler  http.Handler
	activity *activity.Repository
}

func openDB(t *testing.T) *bun.DB {
	t.Helper()
	dsn := os.Getenv("EXPORT_POSTGRES_DSN")
	if dsn == "" {
		return nativeDB(t)
	}
	if os.Getenv("EXPORT_POSTGRES_DISPOSABLE") != "1" {
		t.Fatal("disposable PostgreSQL opt-in required")
	}
	schema := "export01_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	raw := sql.OpenDB(pgdriver.NewConnector(pgdriver.WithDSN(dsn)))
	_, err := raw.Exec(`CREATE SCHEMA "` + schema + `"`)
	nativeMust(t, err)
	nativeMust(t, raw.Close())
	b := bun.NewDB(sql.OpenDB(pgdriver.NewConnector(pgdriver.WithDSN(dsn), pgdriver.WithConnParams(map[string]interface{}{"search_path": schema, "statement_timeout": "10000"}))), pgdialect.New())
	t.Cleanup(func() { _ = b.Close() })
	return b
}
func newHost(t *testing.T) *host {
	t.Helper()
	db := openDB(t)
	f := newNativeFixture(t, db, groupA, groupB)
	ctx := context.Background()
	nativeMust(t, f.grant(ctx, actorID, groupA, "export.read"))
	nativeMust(t, f.grant(ctx, actorID, groupB, "export.read"))
	_, e := db.NewCreateTable().Model((*recordModel)(nil)).Exec(ctx)
	nativeMust(t, e)
	_, e = db.Exec("CREATE TABLE export_resources(id TEXT PRIMARY KEY,group_id TEXT NOT NULL,label TEXT NOT NULL,secret TEXT NOT NULL)")
	nativeMust(t, e)
	for _, r := range []struct {
		id            string
		g             uuid.UUID
		label, secret string
	}{{"a1", groupA, "=1+1", "A-private"}, {"a2", groupA, "public-a2", "A-private"}, {"0-b", groupB, "Group-B-hidden", "B-private"}} {
		_, e = db.Exec("INSERT INTO export_resources VALUES(?,?,?,?)", r.id, r.g.String(), r.label, r.secret)
		nativeMust(t, e)
	}
	h := &host{f: f, store: &watchedStore{MemoryStore: export.NewMemoryStore()}}
	h.tr = &scopedTracker{Tracker: trackerbun.NewTracker(db), postgres: os.Getenv("EXPORT_POSTGRES_DSN") != ""}
	g := guard{f, h.tr}
	h.exec = &sqlExecutor{db: db, guard: g}
	h.runner = export.NewRunner()
	h.runner.Guard = g
	h.runner.Tracker = h.tr
	h.activity, e = activity.NewRepository(activity.RepositoryConfig{DB: db})
	nativeMust(t, e)
	h.runner.Emitter = exportactivity.NewEmitter(exportactivity.Config{Sink: scopedSink{h.activity}})
	nativeMust(t, h.runner.Transformers.Register("safe-text", func(export.TransformerConfig) (export.RowTransformer, error) {
		return export.NewMapTransformer(func(_ context.Context, row export.Row) (export.Row, error) {
			next := append(export.Row{}, row...)
			for i, v := range next {
				if s, ok := v.(string); ok && len(s) > 0 && strings.ContainsRune("=+-@\t\r\n", rune(s[0])) {
					next[i] = "'" + s
				}
			}
			return next, nil
		}), nil
	}))
	nativeMust(t, h.runner.Definitions.Register(export.ExportDefinition{Name: "resources", RowSourceKey: "scoped-sql", Schema: export.Schema{Columns: []export.Column{{Name: "id"}, {Name: "label"}, {Name: "secret"}}}, Policy: export.ExportPolicy{AllowedColumns: []string{"id", "label", "secret"}, RedactColumns: []string{"secret"}, RedactionValue: "[REDACTED]"}, Transformers: []export.TransformerConfig{{Key: "safe-text"}}}))
	reg := exportsql.NewRegistry()
	nativeMust(t, reg.Register(exportsql.Definition{Name: "resources", Query: "host-bound-parameterized-group-select"}))
	nativeMust(t, h.runner.RowSources.Register("scoped-sql", func(req export.ExportRequest, _ export.ResolvedDefinition) (export.RowSource, error) {
		p, _ := req.Query.(map[string]any)
		copy := map[string]any{}
		for k, v := range p {
			copy[k] = v
		}
		if req.Selection.Mode == export.SelectionIDs {
			copy["ids"] = req.Selection.IDs
		}
		req.Query = copy
		return selectedSource{exportsql.NewSource(reg, h.exec, "resources"), req}, nil
	}))
	h.svc = protectedService{export.NewService(export.ServiceConfig{Runner: h.runner, Guard: g, Store: h.store, Tracker: h.tr}), g}
	base := exporthttp.NewHandler(exporthttp.Config{Runner: h.runner, Service: h.svc, Store: h.store, Guard: g, ActorProvider: actorProvider{}})
	h.handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if r.Header.Get("Authorization") == "Bearer synthetic-actor" {
			ctx = context.WithValue(ctx, actorKey{}, actorID)
		}
		id, _ := uuid.Parse(r.Header.Get("X-Group-ID"))
		ctx = context.WithValue(ctx, groupKey{}, id)
		if r.URL.Query().Get("limit") != "" {
			n, _ := strconv.Atoi(r.URL.Query().Get("limit"))
			offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
			if n < 1 || n > 10 || offset < 0 {
				http.Error(w, "invalid page", 400)
				return
			}
			ctx = context.WithValue(ctx, pageKey{}, page{n, offset})
		}
		base.ServeHTTP(w, r.WithContext(ctx))
	})
	return h
}

type selectedSource struct {
	*exportsql.Source
	req export.ExportRequest
}

func (s selectedSource) Open(ctx context.Context, spec export.RowSourceSpec) (export.RowIterator, error) {
	spec.Request = s.req
	return s.Source.Open(ctx, spec)
}
func call(t *testing.T, h *host, method, path string, g uuid.UUID, payload string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(payload))
	r.Header.Set("Authorization", "Bearer synthetic-actor")
	r.Header.Set("X-Group-ID", g.String())
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.handler.ServeHTTP(w, r)
	return w
}
func requireNoLeak(t *testing.T, w *httptest.ResponseRecorder) {
	t.Helper()
	for _, s := range []string{"Group-B-hidden", "B-private", "A-private", "do-not-expose"} {
		if strings.Contains(w.Body.String(), s) {
			t.Fatalf("private data leaked: %q", w.Body.String())
		}
	}
}
func queue(t *testing.T, h *host, g uuid.UUID) string {
	t.Helper()
	w := call(t, h, "POST", "/admin/exports", g, `{"definition":"resources","format":"csv","delivery":"async"}`)
	if w.Code != 202 {
		t.Fatalf("queue: %d %s", w.Code, w.Body.String())
	}
	var v struct {
		ID string `json:"id"`
	}
	nativeMust(t, json.Unmarshal(w.Body.Bytes(), &v))
	return v.ID
}
func TestScopedExportsHTTPAndSQL(t *testing.T) {
	h := newHost(t)
	t.Run("authorized-stream-count-redaction-and-formula-policy", func(t *testing.T) {
		w := call(t, h, "POST", "/admin/exports", groupA, `{"definition":"resources","format":"csv"}`)
		if w.Code != 200 {
			t.Fatalf("stream: %d %s", w.Code, w.Body.String())
		}
		requireNoLeak(t, w)
		rows, e := csv.NewReader(bytes.NewReader(w.Body.Bytes())).ReadAll()
		nativeMust(t, e)
		if len(rows) != 3 || rows[1][0] != "a1" || rows[1][1] != "'=1+1" || rows[1][2] != "[REDACTED]" || h.exec.total.Load() != 2 {
			t.Fatalf("wrong scoped rows/counts: %v total=%d", rows, h.exec.total.Load())
		}
		a, e := actorFrom(scopedContext(groupA))
		nativeMust(t, e)
		r, e := h.svc.Status(scopedContext(groupA), a, w.Header().Get("X-Export-ID"))
		nativeMust(t, e)
		if r.Counts.Processed != 2 {
			t.Fatalf("wrong native processed count: %+v", r.Counts)
		}
	})
	t.Run("SQL-scope-before-limit-and-offset", func(t *testing.T) {
		w := call(t, h, "POST", "/admin/exports", groupA, `{"definition":"resources","format":"json","query":{"group_id":"`+groupB.String()+`","limit":1,"offset":1}}`)
		if w.Code != 200 {
			t.Fatalf("page: %d %s", w.Code, w.Body.String())
		}
		requireNoLeak(t, w)
		if !strings.Contains(w.Body.String(), "a2") || strings.Contains(w.Body.String(), "a1") || h.exec.total.Load() != 2 {
			t.Fatalf("pagination or scope override: %s", w.Body.String())
		}
	})
	t.Run("cross-group-direct-selection", func(t *testing.T) {
		w := call(t, h, "POST", "/admin/exports", groupA, `{"definition":"resources","format":"json","selection":{"mode":"ids","ids":["0-b"]}}`)
		if w.Code != 200 || strings.TrimSpace(w.Body.String()) != "[]" || h.exec.total.Load() != 0 {
			t.Fatalf("cross-scope selection: %d %s total=%d", w.Code, w.Body.String(), h.exec.total.Load())
		}
	})
	t.Run("mixed-selection-stays-scoped", func(t *testing.T) {
		w := call(t, h, "POST", "/admin/exports", groupA, `{"definition":"resources","format":"json","selection":{"mode":"ids","ids":["a1","0-b"]}}`)
		if w.Code != 200 || !strings.Contains(w.Body.String(), "a1") || h.exec.total.Load() != 1 {
			t.Fatalf("mixed scope: %d %s", w.Code, w.Body.String())
		}
		requireNoLeak(t, w)
	})
	t.Run("forged-output-column-denied", func(t *testing.T) {
		before := h.exec.calls.Load()
		w := call(t, h, "POST", "/admin/exports", groupA, `{"definition":"resources","format":"csv","columns":["group_id"]}`)
		if w.Code != 400 || h.exec.calls.Load() != before || w.Header().Get("Content-Disposition") != "" {
			t.Fatalf("column rejection: %d %s", w.Code, w.Body.String())
		}
	})
	t.Run("missing-group-and-identity-denied-before-source-and-headers", func(t *testing.T) {
		before := h.exec.calls.Load()
		for _, g := range []uuid.UUID{uuid.Nil, uuid.New()} {
			w := call(t, h, "POST", "/admin/exports", g, `{"definition":"resources","format":"csv"}`)
			if w.Code != 403 || w.Header().Get("Content-Disposition") != "" || w.Header().Get("X-Export-ID") != "" || h.exec.calls.Load() != before {
				t.Fatalf("guard/headers: %d %s", w.Code, w.Body.String())
			}
		}
		r := httptest.NewRequest("POST", "/admin/exports", strings.NewReader(`{"definition":"resources","format":"csv"}`))
		r.Header.Set("X-Group-ID", groupA.String())
		w := httptest.NewRecorder()
		h.handler.ServeHTTP(w, r)
		if w.Code != 403 || h.exec.calls.Load() != before {
			t.Fatalf("anonymous: %d %s", w.Code, w.Body.String())
		}
	})
	t.Run("safe-rendered-workbook", func(t *testing.T) {
		w := call(t, h, "POST", "/admin/exports", groupA, `{"definition":"resources","format":"xlsx"}`)
		if w.Code != 200 {
			t.Fatalf("xlsx: %d %s", w.Code, w.Body.String())
		}
		file, e := excelize.OpenReader(bytes.NewReader(w.Body.Bytes()))
		nativeMust(t, e)
		defer file.Close()
		rows, e := file.GetRows("Sheet1")
		nativeMust(t, e)
		if len(rows) != 3 || rows[1][1] != "'=1+1" || rows[1][2] != "[REDACTED]" {
			t.Fatalf("workbook projection: %v", rows)
		}
		formula, e := file.GetCellFormula("Sheet1", "B2")
		nativeMust(t, e)
		if formula != "" {
			t.Fatal("formula executed")
		}
	})
	t.Run("SQL-error-is-sanitized", func(t *testing.T) {
		h.exec.fail.Store(true)
		defer h.exec.fail.Store(false)
		w := call(t, h, "POST", "/admin/exports", groupA, `{"definition":"resources","format":"csv"}`)
		if w.Code != 500 {
			t.Fatalf("source error: %d", w.Code)
		}
		requireNoLeak(t, w)
		if w.Header().Get("Content-Disposition") != "" {
			t.Fatal("error committed download headers")
		}
	})
}
func TestScopedExportMetadataAndRevocation(t *testing.T) {
	h := newHost(t)
	aID := queue(t, h, groupA)
	bID := queue(t, h, groupB)
	t.Run("execution-and-download", func(t *testing.T) {
		ctx := scopedContext(groupA)
		a, e := actorFrom(ctx)
		nativeMust(t, e)
		out, e := h.svc.GenerateExport(ctx, a, aID, export.ExportRequest{Definition: "resources", Format: export.FormatCSV})
		nativeMust(t, e)
		if out.Rows != 2 || out.Artifact == nil {
			t.Fatalf("generation result: %+v", out)
		}
		w := call(t, h, "GET", "/admin/exports/"+aID+"/download", groupA, "")
		if w.Code != 200 {
			t.Fatalf("download: %d %s", w.Code, w.Body.String())
		}
		requireNoLeak(t, w)
	})
	t.Run("cross-group-status-download-preview-delete-denied", func(t *testing.T) {
		beforeOpen, beforePut := h.store.opens.Load(), h.store.puts.Load()
		for _, suffix := range []string{"", "/download", "/preview"} {
			w := call(t, h, "GET", "/admin/exports/"+bID+suffix, groupA, "")
			if w.Code != 403 || w.Header().Get("Content-Disposition") != "" {
				t.Fatalf("cross-scope metadata %s: %d %s", suffix, w.Code, w.Body.String())
			}
			requireNoLeak(t, w)
		}
		w := call(t, h, "DELETE", "/admin/exports/"+bID, groupA, "")
		if w.Code != 403 {
			t.Fatalf("delete: %d", w.Code)
		}
		ctx := scopedContext(groupA)
		actor, _ := actorFrom(ctx)
		_, err := h.svc.GenerateExport(ctx, actor, bID, export.ExportRequest{Definition: "resources", Format: export.FormatCSV})
		if err == nil {
			t.Fatal("regenerated another Group's target record")
		}
		if h.store.opens.Load() != beforeOpen || h.store.puts.Load() != beforePut {
			t.Fatal("denied target accessed artifact store")
		}

	})
	t.Run("history-sql-before-page", func(t *testing.T) {
		w := call(t, h, "GET", "/admin/exports/history?limit=1&offset=0", groupA, "")
		if w.Code != 200 {
			t.Fatalf("history: %d %s", w.Code, w.Body.String())
		}
		var rows []export.ExportRecord
		nativeMust(t, json.Unmarshal(w.Body.Bytes(), &rows))
		if len(rows) != 1 || rows[0].ID != aID || rows[0].Scope.TenantID != "" || rows[0].Scope.WorkspaceID != "" {
			t.Fatalf("history escaped Group-only binding: %+v", rows)
		}
		if strings.Contains(w.Body.String(), bID) {
			t.Fatal("other Group ID exposed")
		}
		requireNoLeak(t, w)
	})
	t.Run("forged-service-actor-denied", func(t *testing.T) {
		ctx := scopedContext(groupA)
		a, _ := actorFrom(ctx)
		a.ID = uuid.NewString()
		var output bytes.Buffer
		_, e := h.svc.RequestExport(ctx, a, export.ExportRequest{Definition: "resources", Format: export.FormatCSV, Output: &output})
		if e == nil || output.Len() != 0 {
			t.Fatal("forged actor bypass")
		}
	})
	t.Run("revoked-grant-denies-replay-metadata-and-source", func(t *testing.T) {
		nativeMust(t, h.f.revoke(context.Background(), actorID, groupA))
		before := h.exec.calls.Load()
		for _, path := range []string{"/admin/exports/" + aID, "/admin/exports/" + aID + "/download", "/admin/exports/history"} {
			w := call(t, h, "GET", path, groupA, "")
			if w.Code != 403 {
				t.Fatalf("revoked %s: %d %s", path, w.Code, w.Body.String())
			}
		}
		ctx := scopedContext(groupA)
		a, _ := actorFrom(ctx)
		_, e := h.svc.GenerateExport(ctx, a, aID, export.ExportRequest{Definition: "resources", Format: export.FormatCSV})
		if e == nil || h.exec.calls.Load() != before {
			t.Fatal("revoked queued generation reached rows")
		}
		w := call(t, h, "POST", "/admin/exports", groupA, `{"definition":"resources","format":"csv"}`)
		if w.Code != 403 || h.exec.calls.Load() != before {
			t.Fatal("revoked generation streamed")
		}
		w = call(t, h, "POST", "/admin/exports", groupB, `{"definition":"resources","format":"csv"}`)
		if w.Code != 200 || !strings.Contains(w.Body.String(), "Group-B-hidden") {
			t.Fatalf("independent Group grant affected: %d %s", w.Code, w.Body.String())
		}
	})
}

func TestScopedExportOperationalActivity(t *testing.T) {
	h := newHost(t)
	for _, g := range []uuid.UUID{groupA, groupB} {
		w := call(t, h, "POST", "/admin/exports", g, `{"definition":"resources","format":"csv"}`)
		if w.Code != 200 {
			t.Fatalf("export: %d %s", w.Code, w.Body.String())
		}
	}
	for _, g := range []uuid.UUID{groupA, groupB} {
		ctx := scopedContext(g)
		page, e := h.activity.ListActivity(ctx, types.ActivityFilter{Actor: types.ActorRef{ID: actorID}, Scope: groups.Scope(g), ObjectType: "export"})
		nativeMust(t, e)
		if page.Total != 3 || len(page.Records) != 3 {
			t.Fatalf("expected three scoped operational events, got %+v", page)
		}
		for _, r := range page.Records {
			if r.Labels["group"] != g || r.TenantID != uuid.Nil || r.OrgID != uuid.Nil || r.ActorID != actorID || len(r.Data) != 1 || r.Data["kind"] != "export-operation" {
				t.Fatalf("invalid projected activity: %+v", r)
			}
		}
	}
}

type watchedStore struct {
	*export.MemoryStore
	opens, puts atomic.Int64
}

func (s *watchedStore) Open(ctx context.Context, k string) (io.ReadCloser, export.ArtifactMeta, error) {
	s.opens.Add(1)
	return s.MemoryStore.Open(ctx, k)
}
func (s *watchedStore) Put(ctx context.Context, k string, r io.Reader, m export.ArtifactMeta) (export.ArtifactRef, error) {
	s.puts.Add(1)
	return s.MemoryStore.Put(ctx, k, r, m)
}
func TestScopedExportCurrentNativeGroupState(t *testing.T) {
	for _, state := range []string{"membership-alone", "removed-member", "inactive-group"} {
		t.Run(state, func(t *testing.T) {
			h := newHost(t)
			ctx := context.Background()
			switch state {
			case "membership-alone":
				nativeMust(t, h.f.revoke(ctx, actorID, groupA))
			case "removed-member":
				nativeMust(t, h.f.service.SetMember(ctx, nativeBootstrap, groupA, actorID, false))
			case "inactive-group":
				nativeMust(t, h.f.service.SetActive(ctx, nativeBootstrap, groupA, false))
			}
			w := call(t, h, "POST", "/admin/exports", groupA, `{"definition":"resources","format":"csv"}`)
			if w.Code != 403 || h.exec.calls.Load() != 0 || w.Header().Get("Content-Disposition") != "" {
				t.Fatalf("native state bypass %s: %d %s", state, w.Code, w.Body.String())
			}
		})
	}
}
