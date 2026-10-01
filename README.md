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

The project report, **[docs/report/report.pdf](docs/report/report.pdf)**,
presents the design, the measured results and the limitations in 15 pages.
Its LaTeX source is `docs/report/report.tex` (`pdflatex report.tex`, run twice).

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

## Quick start: one command

Requires **Docker** (Desktop on Windows and macOS) and **Python 3.11+**.
Nothing else is installed on the host: every pipeline component, including
the simulated sources, runs in a container.

```bash
python scripts/demo.py          # or: .\scripts\dev.ps1 demo   /   make demo
```

The demo builds and starts the whole stack, begins a fresh simulation, and
walks through what the system does, checking each claim against the
running system:

1. readings flow end to end, every zone is live, and every component is scraped;
2. a scheduled ZONE-C outage fires `ZoneSilent` for that zone alone, and it clears;
3. day 1 is settled on schedule. The real-time view never over-reported, and
   outside the outage it was typically within 2 % of the settled figures. In
   ZONE-C, settlement recovered the backfilled readings the real-time view
   missed. Bills exist only for settled days;
4. a backdated tariff revision restates day 1, and only the revised tier changes;
5. day 2's deliberately corrupt drop is refused, `DropRefused` fires, and
   republishing the drop recovers the day.

It prints which UI to look at, and when, stamping each step with the real time
since the simulation began, then ends with a PASS/FAIL table. The demo video
is recorded from one run of it: [docs/demo-video.md](docs/demo-video.md) is
the scene-by-scene script, with narration and a recording checklist.
The stack is left running. The first run builds five images, taking 10–20
minutes and about 10 GB of disk; the run itself takes about 16 minutes
(`--quick` stops after day 1, in about 8). `.env` is created from
`.env.example` if it is missing.

| Open | URL |
|---|---|
| Business dashboard | http://localhost:8501 |
| Grafana (operations) | http://localhost:3000 |
| Airflow | http://localhost:8080 |
| Serving API docs | http://localhost:8000/docs |

**Resources.** The stack needs about 6 GB of memory while a day is being
settled. With less free, settlement slows from about a minute and a half to
several minutes, and Docker Desktop itself can fail. Close memory-heavy
applications first, and turn off Docker Desktop's automatic update
downloads while recording. Kafka UI is not started by default, to save
memory: `docker compose --profile tools up -d kafka-ui`.

**Without the demo:** `docker compose up -d --build` starts everything,
sources included, and the pipeline runs on its own. `python
scripts/smoke_test.py` then checks every service is provisioned, and
`.\scripts\dev.ps1 sim-reset` begins a fresh simulation.

### For development

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

### The streaming source

The simulated smart meters run as the `meter-simulator` container. Every
process shares one simulated clock, anchored in PostgreSQL. To run the
simulator on the host instead (for debugging), `.\scripts\dev.ps1 produce`
stops the container first, so two copies never both publish:

```bash
docker compose stop meter-simulator
python -m smartgrid.producers.meter_simulator
```

In a second terminal, measure what the pipeline's shared validator makes of
the live stream, including every injected fault:

```bash
python scripts/inspect_stream.py --seconds 30
```

Useful simulator options: `--faults none|realistic|chaos`, and
`--silence-zone ZONE-C --silence-after 60 --silence-for 90` to take a zone
offline (the lever for demonstrating the no-data alert). The container takes
the same outage from `SIM_SILENCE=ZONE-C:60:90`. Prometheus metrics are
served on port 9101.

### The daily batch source

It runs as the `batch-source` container; on the host, it is
`.\scripts\dev.ps1 drop`.

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

### The serving API

One API in front of both layers, at http://localhost:8000. Interactive docs
are at http://localhost:8000/docs. It applies the merge rule (ADR-0001), and
every figure it returns carries `status` (`SETTLED` or `PROVISIONAL`) and
`layer` (`batch` or `speed`):

| Request | Served from |
|---|---|
| Today's zone figures | speed layer, `PROVISIONAL` |
| A settled day's zone figures | batch layer, `SETTLED`; the speed figures for that day are ignored |
| A day that has ended but is not settled yet | speed layer, `PROVISIONAL`, reason "awaiting settlement" |
| Bills | batch layer only; there is no provisional bill |

| Endpoint | Answers |
|---|---|
| `GET /api/v1/zones/live` | Current grid load and renewable mix per zone, with each figure's age |
| `GET /api/v1/zones/daily?date=D` | One day's totals per zone, with the speed-vs-batch gap once settled |
| `GET /api/v1/zones/{zone}/windows?from=D1&to=D2` | 15-minute windows across days, each labelled by source |
| `GET /api/v1/households/{id}/bills?from=D1&to=D2` | Settled bills, and why any other day has none |
| `GET /api/v1/households/{id}/bills/{D}/history` | Every settlement of that bill: original and restatements |
| `GET /api/v1/bills?date=D` | All of a day's bills, with totals by tier (404 until settled) |
| `GET /api/v1/settlements` | Settlement runs: status, trigger, restatement reason, totals |
| `GET /api/v1/reports/latest`, `/reports/{D}/html` | The daily report |
| `GET /health`, `GET /metrics` | Liveness and speed-layer freshness; Prometheus metrics |

Money is returned as decimal strings (`"871.06"`), never floats. Dates are
simulated dates, and "today" is the shared simulated clock's date.

```bash
curl -s localhost:8000/api/v1/zones/live
curl -s "localhost:8000/api/v1/households/HH-00042/bills?from=2026-01-01&to=2026-01-03"
```

### The business dashboard

A Streamlit dashboard at http://localhost:8501 answers the business question
for a non-technical reader. It reads only the serving API, so it shows
exactly what the merge rule decides, and it labels every figure: **blue /
SETTLED** comes from the batch layer, **orange / PROVISIONAL** from the speed
layer.

| Page | Shows |
|---|---|
| Grid now (`/`) | Current load and solar share per zone, refreshed every 10 s; flags stale data, and zones below the solar-share floor between 10:00 and 14:00 |
| Zone history (`/history`) | 15-minute load and solar share across days: settled days and today on one chart, each labelled |
| Settlement (`/settlement`) | A settled day: totals, billing by tier, how far the real-time view was off per zone, the daily report |
| Household bills (`/bills`) | A household's settled bills, why any day has none yet, and each bill's restatement history |
| Settlement runs (`/runs`) | Every settlement, restatement and failure: the audit trail |

Pages accept `?zone=ZONE-B` and `?household=HH-00042`, so a view can be shared
as a link. The dashboard follows the browser's light or dark setting, and its
charts use a colour-blind-safe palette with a table view under each chart.
The solar-share floor and its hours are `RENEWABLE_ALERT_*` in `.env`.

### Observability

Every stage logs structured JSON (with correlation ids from meter to
dead-letter topic, and request ids in the API), and serves Prometheus
metrics. **Prometheus** (http://localhost:9090) scrapes them every 10 s and
evaluates the alert rules. **Grafana** (http://localhost:3000, no login
needed to view) opens on the operations dashboard: pipeline health tiles,
firing alerts, throughput and rejections, the speed layer's lag and
micro-batch times, solar share and load by zone, the settlement backlog, and
API traffic. The design is recorded in ADR-0009.

| Alert | Fires when | Means |
|---|---|---|
| `ScrapeTargetDown` | a component stops answering for 30 s | a source, the speed layer or the API is down |
| `ServingStoreDown` | the API cannot read PostgreSQL | consumers are without data |
| `SpeedLayerStale` | the real-time view is > 60 s behind (R1) | the speed layer is behind or stopped |
| `HighQuarantineRate` | > 5 % of readings rejected for 1 min (baseline 0.7 %) | an upstream firmware or schema fault |
| `ZoneSilent` | one zone's data > 60 s old while the rest is fresh | a feeder or network outage |
| `LowRenewableShare` | a zone's solar share < 30 % between 10:00 and 14:00 | cloud, or solar not reaching the grid |
| `DropRefused` | the quality gate refused a day's drop | no bills for that day until it is republished |
| `SettlementOverdue` | a day unsettled > 360 s after it was due | Airflow, the drop or the Spark job needs attention |

The rules are tested with `promtool`: `.\scripts\dev.ps1 alerts-test` (or
`make alerts-test`). To see `ZoneSilent` fire, take a zone offline:

```bash
python -m smartgrid.producers.meter_simulator --silence-zone ZONE-C --silence-after 60 --silence-for 90
```

The Grafana dashboard is generated: edit `scripts/build_grafana_dashboard.py`,
then run it to rewrite `infra/grafana/dashboards/smartgrid-operations.json`.

### Task runner

| Task | Windows | Linux / macOS |
|---|---|---|
| **Demo: build, run, check every claim** | `.\scripts\dev.ps1 demo` | `make demo` |
| ...up to day 1 only, no rebuild | `.\scripts\dev.ps1 demo-quick` | `make demo-quick` |
| Build and start everything | `.\scripts\dev.ps1 up` | `make up` |
| Stop (data kept) / wipe volumes | `.\scripts\dev.ps1 down` / `reset` | `make down` / `make reset` |
| Smoke-test every service | `.\scripts\dev.ps1 verify` | `make verify` |
| Start Kafka UI (opt-in) | `.\scripts\dev.ps1 tools` | `make tools` |
| New simulation (everything) | `.\scripts\dev.ps1 sim-reset` | `make sim-reset` |
| Show the simulated clock | `.\scripts\dev.ps1 clock` | `make clock` |
| Run a source on the host instead | `.\scripts\dev.ps1 produce` / `chaos` / `drop` | `make produce` / `chaos` / `drop` |
| Settle / restate a day | `python scripts/settle.py --date D [--restate --reason R]` | same |
| Run tests | `.\scripts\dev.ps1 test` | `make test` |
| Spark tests (in the image) | `.\scripts\dev.ps1 spark-test` | `make spark-test` |
| Test the alert rules (promtool) | `.\scripts\dev.ps1 alerts-test` | `make alerts-test` |
| Measure fault detection | `.\scripts\dev.ps1 inspect` | `make inspect` |
| Gate every daily drop | `.\scripts\dev.ps1 drops` | `make drops` |
| Reconcile the speed layer | `.\scripts\dev.ps1 speed` | `make speed` |
| Settlement runs, bills, speed vs batch | `.\scripts\dev.ps1 settlement` | `make settlement` |
| Firing alerts, from the terminal | `.\scripts\dev.ps1 alerts` | `make alerts` |
| Follow logs | `.\scripts\dev.ps1 source-logs` / `speed-logs` / `airflow-logs` / `api-logs` / `dashboard-logs` | `make <same>` |

### Consoles

| Service | URL | Credentials |
|---|---|---|
| Kafka UI (opt-in: `dev.ps1 tools`) | http://localhost:8085 | — |
| MinIO Console | http://localhost:9001 | `minioadmin` / `minioadmin123` |
| Spark UI (speed layer) | http://localhost:4040 | — |
| Airflow | http://localhost:8080 | — (no login; local demo only) |
| Serving API docs | http://localhost:8000/docs | — |
| Business dashboard | http://localhost:8501 | — |
| Grafana (operations) | http://localhost:3000 | view without login; admin: `GRAFANA_ADMIN_*` in `.env` |
| Prometheus (alerts, targets) | http://localhost:9090/alerts | — |

## Repository layout

```
docs/adr/            architecture decision records
docs/report/         project report (report.pdf) and its LaTeX source
docs/demo-video.md   demo video script and recording checklist
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
| Architecture decision records | Complete — 9 records, 8 amendments |
| Streaming producer | Complete — physical model, 8 fault types, 100% measured detection |
| Daily batch source | Complete — versioned drops, tariff as data, 8 fault types, quality gate at 100% |
| Speed layer | Complete — Spark 3.5 in Docker; reconciled to the message across a restart |
| Batch settlement layer | Complete — Airflow 3.1 + Spark; quality gate, bills, reconciliation, daily report, restatement |
| Serving API | Complete — FastAPI; merge rule, provisional/settled labels, bill history, metrics |
| Business dashboard | Complete — Streamlit over the API; provisional/settled labelling, bill history, audit trail |
| Observability | Complete — Prometheus with 8 promtool-tested alert rules; Grafana operations dashboard |
| Packaging | Complete — everything in Docker Compose, sources included; one-command demo that checks its own results |
| Report | Complete — `docs/report/report.pdf`, 15 pages plus cover |

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
