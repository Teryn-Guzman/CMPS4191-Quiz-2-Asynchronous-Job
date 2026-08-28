-- Filename: 000006_add_public_id_to_jobs_table.up.sql
BEGIN;

ALTER TABLE jobs
-- Expose an opaque lookup identifier while internal updates continue using id.
ADD COLUMN IF NOT EXISTS public_id UUID NOT NULL DEFAULT uuidv4();

COMMIT;
