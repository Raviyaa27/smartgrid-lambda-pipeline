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

Requires Docker Desktop and Python 3.12. The full stack uses about 6 GB of
memory while a day is being settled. With less free, settlement slows from
about a minute and a half to several minutes, and Docker Desktop itself can
fail. Close memory-heavy applications before a demo, and turn off Docker
Desktop's automatic update downloads while recording.

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

### Run the streaming source

Start the simulated smart meters. Every process shares one simulated clock,
anchored in PostgreSQL; `clock-reset` restarts simulated time from
`SIM_START_DATE`.

```bash
python -m smartgrid.common.clock_store reset
python -m smartgrid.producers.meter_simulator
```

In a second terminal, measure what the pipeline's shared validator makes of
the live stream, including every injected fault:

```bash
python scripts/inspect_stream.py --seconds 30
```

Useful simulator options: `--faults none|realistic|chaos`, and
`--silence-zone ZONE-C --silence-after 60 --silence-for 90` to take a zone
offline (the lever for demonstrating the no-data alert). Prometheus metrics
are served on port 9101.

### Run the daily batch source

Publishes one reference drop per simulated day -- the day's tariff schedule,
each household's tier and subsidy, and the weather forecast -- to
`raw/daily/dt=<date>/v=<n>/` in MinIO, at 00:30 simulated time. Drops are
never overwritten; corrections are new versions ([ADR-0008](docs/adr/0008-versioned-immutable-daily-drops.md)).

```bash
python -m smartgrid.producers.daily_batch_source follow
```

Run the batch layer's quality gate over every drop and compare it with the
faults that were injected:

```bash
python scripts/inspect_drops.py
python scripts/inspect_drops.py --date 2026-01-03
```

Publish a retroactive tariff revision -- the scenario that forces the batch
layer to restate a past day:

```bash
python -m smartgrid.producers.daily_batch_source revise --date 2026-01-03 \
    --rate-change 10 --tier DOMESTIC_STD --reason "Regulator backdated revision"
```

Other commands: `publish --date D [--corrupt KIND]` publishes or republishes
one day (republishing is how a bad drop is recovered); `reset --yes` deletes
every drop to begin a new simulation.

### The speed layer

`docker compose up -d` builds and starts it: Spark Structured Streaming in
our own image (`docker/spark.Dockerfile`), reading the readings topic,
validating every message with the shared validator, archiving valid
readings to `s3a://lake/readings/dt=<date>/grid_zone=<zone>/`, sending
rejects to the dead-letter topic, and maintaining provisional per-zone load
and renewable mix in `speed.zone_metrics`. The first `up` builds the image,
which takes a few minutes.

Reconcile every stage -- Kafka, processed, dead-lettered, archived -- to
confirm nothing was lost or double-counted:

```bash
python scripts/inspect_speed_layer.py
```

Follow its logs with `.\scripts\dev.ps1 speed-logs`; run its Spark tests
inside the image with `.\scripts\dev.ps1 spark-test`. The Spark UI is at
http://localhost:4040 and its Prometheus metrics at http://localhost:9103.

Before a demo, start a completely fresh simulation -- clock back to day 1,
the archive, real-time tables and Kafka topics cleared -- with `.\scripts\dev.ps1 sim-reset` (or `make sim-reset`).

### The batch layer

Airflow 3.1 runs in its own container (`docker/airflow.Dockerfile`: the
official image plus the same Spark 3.5.9 runtime as the speed layer). Its UI
is at http://localhost:8080 and needs no login. Two DAGs:

- **`sim_clock_tick`** runs every real minute. It works out which simulated
  days have ended (plus 45 simulated minutes' grace) and triggers
  `daily_settlement` once for each, with the date as a parameter.
- **`daily_settlement`** settles one day:

  ```
  wait_for_drop -> wait_for_archive -> quality_gate -> settle -> reconcile -> report
  ```

  The quality gate fails closed: a drop that fails its checks stops the run
  before any bill is written. `settle` is a Spark job that re-validates the
  archived readings with today's rules, removes retransmissions across the
  whole day, bills every household with the drop's tariff, and writes a
  snapshot of exactly what it settled to `s3a://lake/settled/dt=<date>/run=<id>/`.
  `reconcile` measures the speed layer's error per zone against the settled
  figures, and `report` publishes an HTML report to
  `s3://lake/reports/dt=<date>/run=<id>/daily_report.html`.

Every run writes its own rows, so nothing is overwritten. The serving layer
reads the `batch.current_*` views, which pick the latest successful run for
each day. See the runs, their bills and the speed-vs-batch gap:

```bash
python scripts/inspect_settlement.py
python scripts/inspect_settlement.py --date 2026-01-01
```

**Restating a day** -- after a backdated tariff revision, for example -- is
re-running the DAG for that date. The new run's bills sit beside the
originals, and its report states the change and the reason:

```bash
python -m smartgrid.producers.daily_batch_source revise --date 2026-01-01 \
    --rate-change 10 --tier DOMESTIC_STD --reason "Regulator backdated revision"
python scripts/settle.py --date 2026-01-01 --restate --reason "Regulator backdated revision"
```

### Task runner

| Task | Windows | Linux / macOS |
|---|---|---|
| Start stack | `.\scripts\dev.ps1 up` | `make up` |
| Stop stack | `.\scripts\dev.ps1 down` | `make down` |
| Wipe volumes | `.\scripts\dev.ps1 reset` | `make reset` |
| Verify | `.\scripts\dev.ps1 verify` | `make verify` |
| Run tests | `.\scripts\dev.ps1 test` | `make test` |
| Show / reset simulated clock | `.\scripts\dev.ps1 clock` / `clock-reset` | `make clock` / `make clock-reset` |
| Run meter simulator | `.\scripts\dev.ps1 produce` | `make produce` |
| ...with 10x faults | `.\scripts\dev.ps1 chaos` | `make chaos` |
| Measure fault detection | `.\scripts\dev.ps1 inspect` | `make inspect` |
| Run daily batch source | `.\scripts\dev.ps1 drop` | `make drop` |
| Gate every daily drop | `.\scripts\dev.ps1 drops` | `make drops` |
| Reconcile the speed layer | `.\scripts\dev.ps1 speed` | `make speed` |
| Follow speed-layer logs | `.\scripts\dev.ps1 speed-logs` | `make speed-logs` |
| Spark tests (in the image) | `.\scripts\dev.ps1 spark-test` | `make spark-test` |
| Settlement runs, bills, speed vs batch | `.\scripts\dev.ps1 settlement` | `make settlement` |
| Follow Airflow logs | `.\scripts\dev.ps1 airflow-logs` | `make airflow-logs` |
| Settle / restate a day | `python scripts/settle.py --date D [--restate --reason R]` | same |
| New simulation (everything) | `.\scripts\dev.ps1 sim-reset` | `make sim-reset` |

### Consoles

| Service | URL | Credentials |
|---|---|---|
| Kafka UI | http://localhost:8085 | — |
| MinIO Console | http://localhost:9001 | `minioadmin` / `minioadmin123` |
| Spark UI (speed layer) | http://localhost:4040 | — |
| Airflow | http://localhost:8080 | — (no login; local demo only) |

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
| Foundation package (`smartgrid.common`) | Complete |
| Architecture decision records | Complete — 8 records |
| Streaming producer | Complete — physical model, 8 fault types, 100% measured detection |
| Daily batch source | Complete — versioned drops, tariff as data, 8 fault types, quality gate at 100% |
| Speed layer | Complete — Spark 3.5 in Docker; reconciled to the message across a restart |
| Batch settlement layer | Complete — Airflow 3.1 + Spark; quality gate, bills, reconciliation, daily report, restatement |
| Serving API and dashboards | Not started |
| Observability (Prometheus, Grafana, alerts) | Not started |

## Assumptions and simplifications

- Time is compressed 288×; see the simulated clock section above.
- The tariff schedule is identical every day unless revised; real tariffs
  change rarely, and revisions are published explicitly with `revise`.
- Tariff block limits are published monthly and pro-rated to the daily
  settlement period. A real utility accumulates month-to-date consumption and
  charges the marginal block.
- Single Kafka broker with replication factor 1 — no fault tolerance.
- MinIO runs from `bitnamilegacy/minio`, a pinned but frozen archive image
  that receives no security updates. MinIO withdrew its own images from both
  docker.io and quay.io. Acceptable for a local demo only.
- The speed layer runs Spark in local mode (`local[4]`) inside one container.
  The master dataset is written at-least-once and keeps retransmitted
  readings; the batch layer deduplicates by `event_id` (ADR-0005, amended).
- Smart meters report fixed-interval readings. A meter that is offline
  produces gaps; on reconnection it may upload the missed intervals late.
- Airflow shares the serving PostgreSQL instance for its metadata database.
- Airflow runs as one `airflow standalone` container with LocalExecutor, and
  its UI has no login. Acceptable for a local demo only.
- Settlement runs Spark in local mode as a child process of the Airflow
  task, not on a cluster.
