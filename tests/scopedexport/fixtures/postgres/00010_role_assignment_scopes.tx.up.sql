-- Preserve complete assignment scope independently of the tenant/org role catalog.
ALTER TABLE user_custom_roles ADD COLUMN scope_key TEXT NOT NULL DEFAULT '';
ALTER TABLE user_custom_roles ADD COLUMN labels JSONB NOT NULL DEFAULT '{}';
ALTER TABLE user_custom_roles DROP CONSTRAINT user_custom_roles_pkey;
ALTER TABLE user_custom_roles ADD PRIMARY KEY (user_id, role_id, tenant_id, org_id, scope_key);
CREATE INDEX user_custom_roles_scope_key_idx ON user_custom_roles (tenant_id, org_id, scope_key);
