-- 00003_custom_roles.up.sql
-- Introduces custom roles and assignment tables with tenant/org scope columns.

CREATE TABLE IF NOT EXISTS custom_roles (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    permissions JSONB NOT NULL DEFAULT '[]',
    is_system BOOLEAN NOT NULL DEFAULT FALSE,
    tenant_id TEXT NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000',
    org_id TEXT NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by TEXT NOT NULL,
    updated_by TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS custom_roles_scope_name_idx
    ON custom_roles (tenant_id, org_id, lower(name));

CREATE INDEX IF NOT EXISTS custom_roles_scope_idx
    ON custom_roles (tenant_id, org_id);

CREATE TABLE IF NOT EXISTS user_custom_roles (
    user_id TEXT NOT NULL,
    role_id TEXT NOT NULL REFERENCES custom_roles(id) ON DELETE CASCADE,
    tenant_id TEXT NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000',
    org_id TEXT NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000',
    assigned_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    assigned_by TEXT NOT NULL,
    PRIMARY KEY (user_id, role_id, tenant_id, org_id)
);

CREATE INDEX IF NOT EXISTS user_custom_roles_scope_idx
    ON user_custom_roles (tenant_id, org_id);

CREATE INDEX IF NOT EXISTS user_custom_roles_user_idx
    ON user_custom_roles (user_id);

CREATE INDEX IF NOT EXISTS user_custom_roles_role_idx
    ON user_custom_roles (role_id);
