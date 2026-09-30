"""
Serving-store schema, created idempotently by whichever component needs it.

Tables live in the three schemas created at first boot (ADR-0005):

    speed.*   provisional, low-latency views from the speed layer
    batch.*   settled views from the batch layer (Section 7)
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

# Tables whose rows belong to one simulation and are cleared by a reset.
SIMULATION_TABLES = (
    "speed.zone_metrics",
    "ops.ingest_batches",
    "ops.quarantine_counts",
    "ops.stream_progress",
)


def ensure_schema(dsn: str) -> None:
    with psycopg.connect(dsn, connect_timeout=10) as conn:
        for statement in (*SPEED_DDL, *OPS_DDL):
            conn.execute(statement)
