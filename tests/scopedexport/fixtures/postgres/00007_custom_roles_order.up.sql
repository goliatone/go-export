-- 00007_custom_roles_order.up.sql
-- Adds ordering column to custom_roles for deterministic display.

ALTER TABLE custom_roles
    ADD COLUMN "order" INT NOT NULL DEFAULT 0;
