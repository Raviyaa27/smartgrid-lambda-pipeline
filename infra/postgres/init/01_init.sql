-- ════════════════════════════════════════════════════════════════════
--  Runs ONCE, on first boot of an empty postgres volume.
--  If you change this file you must run `docker compose down -v`
--  to wipe the volume, otherwise it will not re-run.
-- ════════════════════════════════════════════════════════════════════

-- Metadata database for Airflow (Section 6).
CREATE DATABASE airflow;

\connect smartgrid

-- ── Lambda layer separation, enforced at the schema level ───────────
CREATE SCHEMA IF NOT EXISTS speed;
CREATE SCHEMA IF NOT EXISTS batch;
CREATE SCHEMA IF NOT EXISTS ops;

COMMENT ON SCHEMA speed IS
  'Speed layer. Low-latency, approximate, PROVISIONAL views produced by '
  'Spark Structured Streaming. Authoritative only for the current '
  'simulated day; superseded by the batch layer thereafter.';

COMMENT ON SCHEMA batch IS
  'Batch layer. Settled, auditable, fully recomputable views produced by '
  'the daily Airflow settlement DAG from the immutable Parquet master '
  'dataset. SYSTEM OF RECORD for all billing.';

COMMENT ON SCHEMA ops IS
  'Operational metadata: pipeline run log, data-quality results, and the '
  'speed-vs-batch reconciliation used to quantify speed-layer error.';

-- Sanity marker the smoke test reads back.
CREATE TABLE IF NOT EXISTS ops.schema_version (
    version      TEXT        NOT NULL,
    applied_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    description  TEXT        NOT NULL
);

INSERT INTO ops.schema_version (version, description)
VALUES ('0.1.0', 'Section 1: Lambda layer schemas created.');