-- SQLite requires a table rebuild to extend the assignment primary key.
CREATE TABLE user_custom_roles_scoped (
    user_id TEXT NOT NULL,
    role_id TEXT NOT NULL REFERENCES custom_roles(id) ON DELETE CASCADE,
    tenant_id TEXT NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000',
    org_id TEXT NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000',
    scope_key TEXT NOT NULL DEFAULT '',
    labels TEXT NOT NULL DEFAULT '{}',
    assigned_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    assigned_by TEXT NOT NULL,
    PRIMARY KEY (user_id, role_id, tenant_id, org_id, scope_key)
);
INSERT INTO user_custom_roles_scoped (user_id, role_id, tenant_id, org_id, assigned_at, assigned_by)
SELECT user_id, role_id, tenant_id, org_id, assigned_at, assigned_by FROM user_custom_roles;
DROP TABLE user_custom_roles;
ALTER TABLE user_custom_roles_scoped RENAME TO user_custom_roles;
CREATE INDEX user_custom_roles_scope_idx ON user_custom_roles (tenant_id, org_id);
CREATE INDEX user_custom_roles_scope_key_idx ON user_custom_roles (tenant_id, org_id, scope_key);
CREATE INDEX user_custom_roles_user_idx ON user_custom_roles (user_id);
CREATE INDEX user_custom_roles_role_idx ON user_custom_roles (role_id);
