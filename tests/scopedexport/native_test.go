package verification

import (
	"context"
	"database/sql"
	"embed"
	"github.com/goliatone/go-users/groups"
	"github.com/goliatone/go-users/pkg/types"
	"github.com/goliatone/go-users/registry"
	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
	"github.com/uptrace/bun/dialect/sqlitedialect"
	"path/filepath"
	"testing"
	"time"
)

//go:embed fixtures/*/*.sql
var nativeMigrations embed.FS
var nativeBootstrap = types.ActorRef{ID: uuid.MustParse("44444444-4444-4444-8444-444444444444"), Type: "service"}

type nativeFixture struct {
	db      *bun.DB
	service *groups.Service
	roles   *registry.RoleRegistry
	catalog types.ScopeFilter
	grants  map[string]uuid.UUID
	a, b    uuid.UUID
}

func nativeMust(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func nativeContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func nativeDB(t *testing.T) *bun.DB {
	t.Helper()
	raw, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "native.sqlite")+"?_journal_mode=WAL&_foreign_keys=on&_busy_timeout=5000&_txlock=immediate")
	nativeMust(t, err)
	raw.SetMaxOpenConns(4)
	db := bun.NewDB(raw, sqlitedialect.New())
	t.Cleanup(func() { nativeMust(t, db.Close()) })
	return db
}

func newNativeFixture(t *testing.T, db *bun.DB, a, b uuid.UUID) *nativeFixture {
	t.Helper()
	ctx := nativeContext(t)
	for _, name := range []string{"00003_custom_roles.up.sql", "00004_user_activity.up.sql", "00006_custom_roles_metadata.up.sql", "00007_custom_roles_order.up.sql", "00010_role_assignment_scopes.tx.up.sql", "00011_activity_scopes.tx.up.sql", "00012_groups.tx.up.sql"} {
		dir := "sqlite"
		if db.Dialect().Name() == dialect.PG {
			dir = "postgres"
		}
		ddl, err := nativeMigrations.ReadFile("fixtures/" + dir + "/" + name)
		nativeMust(t, err)
		nativeMust(t, db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error { _, err := tx.ExecContext(ctx, string(ddl)); return err }))
	}
	f := &nativeFixture{db: db, catalog: types.ScopeFilter{TenantID: uuid.New(), OrgID: uuid.New()}, grants: map[string]uuid.UUID{}, a: a, b: b}
	var err error
	f.roles, err = registry.NewRoleRegistry(registry.RoleRegistryConfig{DB: db})
	nativeMust(t, err)
	f.service, err = groups.NewStrict(groups.Config{DB: db, CatalogScope: f.catalog,
		Resolver: types.PassthroughScopeResolver{}, Actions: []types.PolicyAction{"read", "write", "export.read", "endpoint.manage", "admin.commands.dispatch", "admin.endpoints.operate"},
		Policy: types.AuthorizationPolicyFunc(func(_ context.Context, p types.PolicyCheck) error {
			if p.Action == groups.ActionCreate && p.Actor.ID != nativeBootstrap.ID {
				return groups.ErrDenied
			}
			return nil // Resource ownership is checked explicitly in nativeWrite and the host adapter.
		})})
	nativeMust(t, err)
	permissions := []string{string(groups.ActionRead), string(groups.ActionManage), string(groups.ActionMembers), string(groups.ActionRoles), "read", "write", "export.read", "endpoint.manage", "admin.commands.dispatch", "admin.endpoints.operate"}
	for _, p := range append([]string{"manager"}, permissions...) {
		ps := []string{p}
		if p == "manager" {
			ps = permissions
		}
		role, err := f.roles.CreateRole(ctx, types.RoleMutation{Name: "native-" + p, Scope: f.catalog, Permissions: ps, ActorID: nativeBootstrap.ID})
		nativeMust(t, err)
		f.grants[p] = role.ID
	}
	// Explicit trusted provisioning. Neither membership nor native Create grants
	// any permissions, and opaque principals require no authentication-user row.
	nativeMust(t, db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		r, err := f.roles.WithTx(tx)
		if err != nil {
			return err
		}
		for i, id := range []uuid.UUID{a, b} {
			now := time.Now().UTC()
			g := &groups.Group{ID: id, Name: []string{"Group A", "Group B"}[i], Active: true, CreatedAt: now, UpdatedAt: now}
			if _, err := tx.NewInsert().Model(g).Exec(ctx); err != nil {
				return err
			}
			m := &groups.Membership{GroupID: id, PrincipalID: nativeBootstrap.ID, Active: true, CreatedAt: now, UpdatedAt: now}
			if _, err := tx.NewInsert().Model(m).Exec(ctx); err != nil {
				return err
			}
			if err := r.AssignRoleFromCatalog(ctx, types.ScopedRoleGrant{UserID: nativeBootstrap.ID, RoleID: f.grants["manager"], CatalogScope: f.catalog, Scope: groups.Scope(id), ActorID: nativeBootstrap.ID}); err != nil {
				return err
			}
		}
		return nil
	}))
	_, err = db.ExecContext(ctx, "CREATE TABLE native_resources (id TEXT PRIMARY KEY, group_id TEXT NOT NULL, payload TEXT NOT NULL)")
	nativeMust(t, err)
	return f
}

func (f *nativeFixture) grant(ctx context.Context, user, group uuid.UUID, permissions ...string) error {
	if err := f.service.SetMember(ctx, nativeBootstrap, group, user, true); err != nil {
		return err
	}
	for _, permission := range permissions {
		if err := f.service.SetRole(ctx, nativeBootstrap, group, user, f.grants[permission], true); err != nil {
			return err
		}
	}
	return nil
}

func (f *nativeFixture) revoke(ctx context.Context, user, group uuid.UUID) error {
	assignments, err := f.roles.ListAssignments(ctx, types.RoleAssignmentFilter{UserID: user, Scope: groups.Scope(group)})
	if err != nil {
		return err
	}
	for _, assignment := range assignments {
		if err := f.service.SetRole(ctx, nativeBootstrap, group, user, assignment.RoleID, false); err != nil {
			return err
		}
	}
	return nil
}
