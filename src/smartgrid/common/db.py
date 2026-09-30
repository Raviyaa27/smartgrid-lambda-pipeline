"""
Serving-store schema, created idempotently by whichever component needs it.

Tables live in the three schemas created at first boot (ADR-0005):

    speed.*   provisional, low-latency views from the speed layer
    batch.*   settled views from the batch layer
    ops.*     pipeline bookkeeping: stream progress, quarantine counts

Every statement is `IF NOT EXISTS`, so any component can call
`ensure_schema` on start-up without coordinating with the others.
"""

from __future__ import annotations

import psycopg

SPEED_DDL = (
    """
    CREATE TABLE IF NOT EXISTS speed.zone_metrics (
        grid_zone         TEXT             NOT NULL,
        window_start      TIMESTAMPTZ      NOT NULL,   -- simulated time
        window_end        TIMESTAMPTZ      NOT NULL,
        consumption_kwh   DOUBLE PRECISION NOT NULL,
        generation_kwh    DOUBLE PRECISION NOT NULL,
        net_kwh           DOUBLE PRECISION NOT NULL,
        grid_load_kw      DOUBLE PRECISION NOT NULL,   -- average draw across the window
        solar_kw          DOUBLE PRECISION NOT NULL,
        renewable_share   DOUBLE PRECISION,            -- generation / consumption; NULL if none
        readings          INTEGER          NOT NULL,
        meters_reporting  INTEGER          NOT NULL,
        updated_at        TIMESTAMPTZ      NOT NULL DEFAULT now(),
        PRIMARY KEY (grid_zone, window_start)
    )
    """,
    "CREATE INDEX IF NOT EXISTS zone_metrics_window_idx ON speed.zone_metrics (window_start DESC)",
    """
    COMMENT ON TABLE speed.zone_metrics IS
      'PROVISIONAL per-zone load and renewable mix per 15-minute simulated window. '
      'Readings arriving later than the watermark are absent here but present in the '
      'archive; the batch layer supersedes these figures once a day is settled.'
    """,
)

OPS_DDL = (
    """
    CREATE TABLE IF NOT EXISTS ops.ingest_batches (
        sim_id               BIGINT      NOT NULL,     -- which simulation (clock anchor)
        batch_id             BIGINT      NOT NULL,     -- Spark micro-batch id
        records_in           INTEGER     NOT NULL,
        records_valid        INTEGER     NOT NULL,
        records_quarantined  INTEGER     NOT NULL,
        max_event_time       TIMESTAMPTZ,
        recorded_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (sim_id, batch_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ops.quarantine_counts (
        sim_id       BIGINT      NOT NULL,
        batch_id     BIGINT      NOT NULL,
        reason       TEXT        NOT NULL,
        records      INTEGER     NOT NULL,
        recorded_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (sim_id, batch_id, reason)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ops.stream_progress (
        query_name      TEXT        PRIMARY KEY,
        sim_id          BIGINT      NOT NULL,
        batch_id        BIGINT      NOT NULL,
        max_event_time  TIMESTAMPTZ,              -- latest simulated time processed
        updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    COMMENT ON TABLE ops.stream_progress IS
      'How far each streaming query has got, in simulated time. The batch layer '
      'waits until the archive has passed the end of a day before settling it.'
    """,
)

# -- Batch layer ----------------------------------------------------------------
# Every settlement RUN writes its own rows; nothing is updated in place. A
# restated day therefore sits beside its original, and the `current_*` views
# expose the latest successful run per business date -- which is what the
# serving layer reads.
BATCH_DDL = (
    """
    CREATE TABLE IF NOT EXISTS ops.settlement_runs (
        run_id               BIGSERIAL   PRIMARY KEY,
        sim_id               BIGINT      NOT NULL,
        business_date        DATE        NOT NULL,
        status               TEXT        NOT NULL
                             CHECK (status IN ('running', 'succeeded', 'failed')),
        drop_version         INTEGER     NOT NULL,
        trigger              TEXT        NOT NULL,     -- scheduled | restatement | manual
        reason               TEXT,
        airflow_run_id       TEXT,
        readings_archived    INTEGER,                  -- rows in the archive for the day
        readings_settled     INTEGER,                  -- after deduplication and re-validation
        duplicates_removed   INTEGER,
        readings_rejected    INTEGER,                  -- archived, but invalid under today's rules
        households_billed    INTEGER,
        total_billed         NUMERIC(14, 2),
        snapshot_path        TEXT,                     -- exactly what was settled
        error                TEXT,
        started_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
        finished_at          TIMESTAMPTZ
    )
    """,
    "CREATE INDEX IF NOT EXISTS settlement_runs_date_idx "
    "ON ops.settlement_runs (business_date, run_id DESC)",
    """
    CREATE TABLE IF NOT EXISTS batch.bills (
        run_id                 BIGINT         NOT NULL REFERENCES ops.settlement_runs (run_id),
        business_date          DATE           NOT NULL,
        household_id           TEXT           NOT NULL,
        tariff_tier            TEXT           NOT NULL,
        readings               INTEGER        NOT NULL,
        gross_consumption_kwh  NUMERIC(12, 3) NOT NULL,
        solar_generation_kwh   NUMERIC(12, 3) NOT NULL,
        net_import_kwh         NUMERIC(12, 3) NOT NULL,
        net_export_kwh         NUMERIC(12, 3) NOT NULL,
        energy_charge          NUMERIC(12, 2) NOT NULL,
        fixed_charge           NUMERIC(12, 2) NOT NULL,
        export_credit          NUMERIC(12, 2) NOT NULL,
        subsidy_amount         NUMERIC(12, 2) NOT NULL,
        total_payable          NUMERIC(12, 2) NOT NULL,
        subsidy_applied        BOOLEAN        NOT NULL,
        currency               TEXT           NOT NULL,
        PRIMARY KEY (run_id, household_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS batch.zone_metrics (
        run_id            BIGINT           NOT NULL REFERENCES ops.settlement_runs (run_id),
        grid_zone         TEXT             NOT NULL,
        window_start      TIMESTAMPTZ      NOT NULL,
        window_end        TIMESTAMPTZ      NOT NULL,
        consumption_kwh   DOUBLE PRECISION NOT NULL,
        generation_kwh    DOUBLE PRECISION NOT NULL,
        net_kwh           DOUBLE PRECISION NOT NULL,
        grid_load_kw      DOUBLE PRECISION NOT NULL,
        solar_kw          DOUBLE PRECISION NOT NULL,
        renewable_share   DOUBLE PRECISION,
        readings          INTEGER          NOT NULL,
        meters_reporting  INTEGER          NOT NULL,
        PRIMARY KEY (run_id, grid_zone, window_start)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS batch.zone_daily (
        run_id               BIGINT           NOT NULL REFERENCES ops.settlement_runs (run_id),
        business_date        DATE             NOT NULL,
        grid_zone            TEXT             NOT NULL,
        consumption_kwh      DOUBLE PRECISION NOT NULL,
        generation_kwh       DOUBLE PRECISION NOT NULL,
        renewable_share      DOUBLE PRECISION,
        peak_load_kw         DOUBLE PRECISION NOT NULL,
        readings             INTEGER          NOT NULL,
        meters_reporting     INTEGER          NOT NULL,
        forecast_irradiance  DOUBLE PRECISION,           -- from the daily drop
        PRIMARY KEY (run_id, grid_zone)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ops.quality_gate_results (
        id             BIGSERIAL   PRIMARY KEY,
        sim_id         BIGINT      NOT NULL,
        business_date  DATE        NOT NULL,
        drop_version   INTEGER,
        passed         BOOLEAN     NOT NULL,
        findings       JSONB       NOT NULL,
        airflow_run_id TEXT,
        checked_at     TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ops.reconciliation (
        run_id            BIGINT           NOT NULL REFERENCES ops.settlement_runs (run_id),
        business_date     DATE             NOT NULL,
        grid_zone         TEXT             NOT NULL,
        speed_kwh         DOUBLE PRECISION,             -- what the real-time view showed
        batch_kwh         DOUBLE PRECISION NOT NULL,    -- what settlement found
        delta_kwh         DOUBLE PRECISION,
        delta_pct         DOUBLE PRECISION,             -- (speed - batch) / batch
        speed_readings    INTEGER,
        batch_readings    INTEGER          NOT NULL,
        missed_by_speed   INTEGER,                      -- late readings only the batch layer saw
        PRIMARY KEY (run_id, grid_zone)
    )
    """,
    """
    COMMENT ON TABLE ops.reconciliation IS
      'The speed layer''s approximation error, measured: per zone and day, what the '
      'real-time view reported against what settlement found (ADR-0001).'
    """,
    """
    CREATE TABLE IF NOT EXISTS ops.settlement_triggers (
        sim_id          BIGINT      NOT NULL,
        business_date   DATE        NOT NULL,
        airflow_run_id  TEXT        NOT NULL,
        triggered_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (sim_id, business_date)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ops.daily_reports (
        run_id         BIGINT      PRIMARY KEY REFERENCES ops.settlement_runs (run_id),
        business_date  DATE        NOT NULL,
        object_key     TEXT        NOT NULL,
        created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    # The latest SUCCESSFUL settlement of each day: what the serving layer reads.
    """
    CREATE OR REPLACE VIEW batch.current_runs AS
    SELECT DISTINCT ON (business_date) business_date, run_id, drop_version, trigger, finished_at
    FROM ops.settlement_runs
    WHERE status = 'succeeded'
    ORDER BY business_date, run_id DESC
    """,
    """
    CREATE OR REPLACE VIEW batch.current_bills AS
    SELECT b.* FROM batch.bills b JOIN batch.current_runs r USING (run_id)
    """,
    """
    CREATE OR REPLACE VIEW batch.current_zone_daily AS
    SELECT z.* FROM batch.zone_daily z JOIN batch.current_runs r USING (run_id)
    """,
    """
    CREATE OR REPLACE VIEW batch.current_zone_metrics AS
    SELECT z.* FROM batch.zone_metrics z JOIN batch.current_runs r USING (run_id)
    """,
)

# Tables whose rows belong to one simulation and are cleared by a reset.
# Children before parents, because of the foreign keys.
SIMULATION_TABLES = (
    "speed.zone_metrics",
    "ops.ingest_batches",
    "ops.quarantine_counts",
    "ops.stream_progress",
    "ops.reconciliation",
    "ops.daily_reports",
    "batch.bills",
    "batch.zone_metrics",
    "batch.zone_daily",
    "ops.settlement_runs",
    "ops.quality_gate_results",
    "ops.settlement_triggers",
)


def ensure_schema(dsn: str) -> None:
    with psycopg.connect(dsn, connect_timeout=10) as conn:
        for statement in (*SPEED_DDL, *OPS_DDL, *BATCH_DDL):
            conn.execute(statement)
