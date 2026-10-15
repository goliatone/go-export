-- 00007_custom_roles_order.up.sql (SQLite version)
-- Adds ordering column to custom_roles for deterministic display.

ALTER TABLE custom_roles ADD COLUMN "order" INTEGER NOT NULL DEFAULT 0;
