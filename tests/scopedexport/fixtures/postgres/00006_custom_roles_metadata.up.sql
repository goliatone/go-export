-- 00006_custom_roles_metadata.up.sql
-- Adds role_key and metadata to custom_roles.

ALTER TABLE custom_roles
    ADD COLUMN role_key TEXT,
    ADD COLUMN metadata JSONB NOT NULL DEFAULT '{}'::jsonb;
