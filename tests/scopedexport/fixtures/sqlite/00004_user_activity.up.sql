-- 00004_user_activity.up.sql (SQLite version)
-- Activity log table powering go-admin dashboards and audit exports.
-- Changes from PostgreSQL: JSONB -> TEXT

CREATE TABLE IF NOT EXISTS user_activity (
    id TEXT PRIMARY KEY,
    user_id TEXT,
    actor_id TEXT,
    tenant_id TEXT NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000',
    org_id TEXT NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000',
    verb TEXT NOT NULL,
    object_type TEXT,
    object_id TEXT,
    channel TEXT,
    ip TEXT,
    data TEXT NOT NULL DEFAULT '{}',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS user_activity_scope_idx
    ON user_activity (tenant_id, org_id, created_at DESC);

CREATE INDEX IF NOT EXISTS user_activity_user_idx
    ON user_activity (user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS user_activity_object_idx
    ON user_activity (object_type, object_id);

CREATE INDEX IF NOT EXISTS user_activity_verb_idx
    ON user_activity (verb);
