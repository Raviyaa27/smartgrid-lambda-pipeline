# smartgrid-lambda-pipeline

A **Lambda-architecture data platform** for smart-grid energy monitoring and
billing, built for EC8203 Applied Big Data Engineering.

The system ingests two sources — a continuous stream of smart-meter readings
and a once-daily tariff and weather feed — and answers one business question
with two very different halves:

> What is the current grid load and renewable contribution by zone, and what
> will each household's bill look like once daily tariff data is applied to
> their consumption?

## Why Lambda

Those two halves have incompatible service requirements. Real-time grid
visibility tolerates approximation and needs sub-minute freshness. Billing
tolerates no approximation at all: it is regulated, auditable, disputable,
and must be recomputable years later when a tariff is revised retroactively.

A speed layer serves the first from the live stream. A batch layer serves the
second by recomputing from an immutable Parquet master dataset. A serving
layer merges them under an explicit rule, and every figure is labelled
`PROVISIONAL` or `SETTLED`.

The full argument, including why Kappa was rejected and the conditions under
which that rejection stops being correct, is in
**[ADR-0001](docs/adr/0001-lambda-over-kappa.md)**.

## Architecture

```
meter_simulator ──▶ Kafka ──┬──▶ SPEED  Spark Structured Streaming
                            │           validate → dedupe → window by zone
                            │           └─▶ postgres  speed.*   (PROVISIONAL)
                            │
                            └──▶ Parquet on MinIO  dt=/zone=    (master dataset)
                                          │
batch_dropper ──▶ MinIO raw/ ──┐          │
                               ▼          ▼
                     Airflow  daily_settlement
                     sensor → quality gate → Spark batch
                     → tiered billing → reconciliation
                     └─▶ postgres  batch.*  ops.*   (SETTLED)
                                   │
              FastAPI + dashboards ◀┘
```

| Layer | Technology | Decision record |
|---|---|---|
| Ingestion | Apache Kafka (KRaft) | [ADR-0003](docs/adr/0003-kafka-topic-and-retention-design.md) |
| Stream processing | Spark Structured Streaming | [ADR-0002](docs/adr/0002-spark-structured-streaming-over-storm.md) |
| Batch processing | Spark, orchestrated by Airflow | [ADR-0006](docs/adr/0006-airflow-for-orchestration.md) |
| Master dataset | Parquet on MinIO (S3 API) | [ADR-0005](docs/adr/0005-parquet-master-dataset-postgres-serving.md) |
| Serving store | PostgreSQL (`speed` / `batch` / `ops`) | [ADR-0005](docs/adr/0005-parquet-master-dataset-postgres-serving.md) |
| Shared logic | One module imported by both layers | [ADR-0004](docs/adr/0004-shared-transformation-module.md) |
| Simulated time | 1 day = 300 s (288×) | [ADR-0007](docs/adr/0007-simulated-clock-and-time-compression.md) |

## Simulated clock

**One simulated day is compressed into 300 real seconds (288×)**, starting
`2026-01-01`. Nothing in the pipeline calls `datetime.now()` to decide what
day it is; all domain time derives from `SimulatedClock`, which is what makes
the batch layer reproducible and replayable. Configured via `SIM_DAY_SECONDS`
and `SIM_START_DATE`. See [ADR-0007](docs/adr/0007-simulated-clock-and-time-compression.md).

## Quick start

Requires Docker Desktop (~8 GB free RAM) and Python 3.12.

```bash
cp .env.example .env
docker compose up -d
```

Wait for the health checks, then verify the infrastructure:

```bash
python scripts/smoke_test.py
```

Set up the Python environment and run the test suite:

```bash
py -3.12 -m venv .venv && .venv\Scripts\activate
pip install -r requirements-dev.txt && pip install -e .
pytest
```

See the foundation modules working end to end, with no infrastructure needed:

```bash
python scripts/section2_demo.py
```

### Task runner

| Task | Windows | Linux / macOS |
|---|---|---|
| Start stack | `.\scripts\dev.ps1 up` | `make up` |
| Stop stack | `.\scripts\dev.ps1 down` | `make down` |
| Wipe volumes | `.\scripts\dev.ps1 reset` | `make reset` |
| Verify | `.\scripts\dev.ps1 verify` | `make verify` |

### Consoles

| Service | URL | Credentials |
|---|---|---|
| Kafka UI | http://localhost:8085 | — |
| MinIO Console | http://localhost:9001 | `minioadmin` / `minioadmin123` |

## Repository layout

```
docs/adr/            architecture decision records
docs/report/         assessment report
infra/               service configuration (postgres init, prometheus, grafana)
src/smartgrid/
  common/            config, logging, clock, domain, schemas, transformations, billing
  producers/         simulated streaming and daily-batch sources
  streaming/         speed layer
  batch/             settlement layer
  serving/           API
  dashboard/         business dashboard
airflow/dags/        orchestration
scripts/             smoke test, task runner, demos
tests/               unit and integration tests
```

## Status

| Component | State |
|---|---|
| Infrastructure (Kafka, PostgreSQL, MinIO) | Complete |
| Foundation package (`smartgrid.common`) | Complete — 46 unit tests |
| Architecture decision records | Complete — 7 records |
| Streaming producer | Not started |
| Daily batch source | Not started |
| Speed layer | Not started |
| Batch settlement layer | Not started |
| Serving API and dashboards | Not started |
| Observability (Prometheus, Grafana, alerts) | Not started |

## Assumptions and simplifications

- Time is compressed 288×; see the simulated clock section above.
- Tariff block limits are published monthly and pro-rated to the daily
  settlement period. A real utility accumulates month-to-date consumption and
  charges the marginal block.
- Single Kafka broker with replication factor 1 — no fault tolerance.
- Airflow shares the serving PostgreSQL instance for its metadata database.
