-- 00006_custom_roles_metadata.up.sql (SQLite version)
-- Adds role_key and metadata to custom_roles.

ALTER TABLE custom_roles ADD COLUMN role_key TEXT;

---bun:split

ALTER TABLE custom_roles ADD COLUMN metadata TEXT NOT NULL DEFAULT '{}';
