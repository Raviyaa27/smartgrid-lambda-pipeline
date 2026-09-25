-- Speed-layer output: rolling per-zone grid load / renewable mix.
-- Add this to wherever the repo's Postgres init scripts live (infra/).

CREATE TABLE IF NOT EXISTS speed.zone_load (
    zone                TEXT        NOT NULL,
    window_start        TIMESTAMPTZ NOT NULL,
    window_end          TIMESTAMPTZ NOT NULL,
    total_consumption_kwh   DOUBLE PRECISION NOT NULL,
    total_solar_kwh         DOUBLE PRECISION NOT NULL,
    renewable_pct            DOUBLE PRECISION NOT NULL,
    reading_count            INTEGER NOT NULL,
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (zone, window_start)
);