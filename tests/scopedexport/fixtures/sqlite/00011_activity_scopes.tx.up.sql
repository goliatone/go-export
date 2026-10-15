ALTER TABLE user_activity ADD COLUMN scope_key TEXT NOT NULL DEFAULT '';
ALTER TABLE user_activity ADD COLUMN labels TEXT NOT NULL DEFAULT '{}';
CREATE INDEX user_activity_exact_scope_idx ON user_activity (tenant_id, org_id, scope_key, created_at DESC, id DESC);
