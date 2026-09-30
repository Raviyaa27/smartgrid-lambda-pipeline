# ADR-0006: Use Airflow to orchestrate the batch layer

- **Status:** Accepted
- **Date:** 2026-09-20
- **Deciders:** Project team

## Context

The batch layer runs once per simulated day and must:

1. wait for the daily tariff and weather files to land in object storage;
2. refuse to proceed if those files fail data-quality checks;
3. recompute the day's consumption from the Parquet master dataset,
   including readings that arrived too late for the speed layer;
4. apply the tiered tariff and write settled bills;
5. reconcile the settled figures against what the speed layer reported;
6. publish the consolidated daily report.

Steps depend on each other, any step can fail transiently, and — critically —
**the whole chain must be re-runnable for an arbitrary past date**.
[ADR-0001](0001-lambda-over-kappa.md) establishes restatement as a core
requirement, and this DAG is the mechanism that delivers it.

## Decision

**Use Apache Airflow, with `catchup` and backfill as the restatement
mechanism.**

The usual reasons to reach for Airflow — dependency graphs, retries,
scheduling, a UI — are real but generic; on their own they would not
distinguish it from several alternatives. The decisive reason is specific to
this architecture:

> **Airflow's backfill semantics are exactly the restatement requirement.**

A DAG parameterised by its logical date, reading `dt={{ ds }}` from the
master dataset and writing `dt={{ ds }}` to the serving store, makes
"recompute 14 March" a single command. Retroactive tariff change, late meter
reads, subsidy reclassification — all four restatement scenarios in ADR-0001
reduce to re-running the DAG for the affected dates. The architecture's
central claim, that restatement is cheap and targeted, is delivered by a
feature of the orchestrator rather than by code we write.

Supporting reasons:

- **Sensors express the dependency on an external file** properly. The tariff
  file arriving is an event the DAG waits on, with a timeout that becomes an
  alert, rather than a `sleep` and a hope.
- **Task-level retries** distinguish a transient object-storage error from a
  genuine data-quality failure. The first should retry; the second must stop
  the run and page someone.
- **The quality gate is a first-class task.** A run that would produce wrong
  bills must not produce bills at all. Expressing that as a task with
  downstream dependencies makes "fail closed" the default rather than
  something remembered.
- **Run history is the audit trail.** Which date was settled, when, by which
  code, with what outcome — recorded without extra work, which matters for an
  auditable billing process.

## Alternatives considered

### `cron` plus a shell script

**What it gives.** Nothing to deploy. For a linear job, genuinely adequate.

**Why rejected.** No dependency semantics, no per-task retry, no backfill, no
run history. Re-running one past date means writing the date-handling by hand
— which is to say, reimplementing the feature that decided this record, worse.

### Dagster or Prefect

**What they give.** Both are modern, arguably more ergonomic than Airflow,
with better typing and local development stories. Prefect's dynamic workflows
and Dagster's asset-oriented model are both good fits for data pipelines.

**Why rejected.** Outside the module's named stack. On merit the gap is
narrow — Dagster's software-defined assets would arguably model the
partitioned master dataset more naturally than Airflow's task graph. This is
a scope decision, not a claim that Airflow is superior.

### Spark's own scheduling, or a long-running job that sleeps until midnight

**What it gives.** One fewer component.

**Why rejected.** Conflates orchestration with computation. A sleeping job
has no retry story, no backfill, no run history, and it fails as a unit —
a transient MinIO error in step 3 loses the whole run rather than retrying
one task. It also means the batch layer can only ever run forward, never
for a past date, which fails the restatement requirement outright.

### Trigger settlement from the streaming job at day rollover

**Why rejected.** Couples the two Lambda layers, which ADR-0001 deliberately
keeps independent. A speed-layer restart would then affect settlement, and a
settlement failure would have nowhere sensible to surface.

## Consequences

**Positive**

- Restatement is a command, not a project.
- Failures are isolated to tasks, with retries where retries are appropriate.
- The quality gate fails closed by construction.
- Run history doubles as the settlement audit trail.
- The DAG graph in the UI is a readable picture of the batch layer — useful
  in the demo video and the report.

**Negative**

- Airflow is heavy: a scheduler, a webserver and a metadata database to
  support one daily DAG.
- It needs its own PostgreSQL database, adding a component whose failure
  stops settlement.
- Its scheduling model assumes real calendar time, which does not match our
  compressed simulated clock (see [ADR-0007](0007-simulated-clock-and-time-compression.md)).

**Mitigations**

- `LocalExecutor` rather than Celery or Kubernetes: no broker, no workers,
  appropriate for one DAG at demo scale.
- Airflow's metadata database is a second database on the PostgreSQL instance
  already running for the serving layer, so no new container.
- The simulated-clock mismatch is resolved by triggering the DAG externally
  on simulated-day rollover and passing the simulated date as a parameter,
  rather than relying on Airflow's own schedule. The DAG stays
  date-parameterised, so backfill still works exactly as intended — which is
  the property we chose Airflow for in the first place.

## Revisit if

- The stack constraint is lifted and asset-oriented orchestration is
  preferred — Dagster models a partitioned master dataset more directly.
- Settlement needs to run at a cadence Airflow's scheduler handles poorly,
  such as sub-minute.
- The DAG count stays at one indefinitely, at which point the operational
  weight may genuinely exceed the benefit — though the backfill argument
  would still need answering.

## Amendments

### Amendment 1 — 2026-09-30: Airflow 3.1, and restatement by date parameter rather than backfill

Building the batch layer changed four details. The decision itself, Airflow
with restatement as a single command, is unchanged.

**Airflow 3.1, not 2.x.** Airflow 2 reached end of life in April 2026, so a
new project should not start on it. Version 3 moves the task API to
`airflow.sdk`, replaces the webserver with an API server, and reserves some
names in the task context. `reason` and `run_id` are among them, and a task
argument with either name is rejected at run time. Our DAG files are
written for 3.1.

**Restatement is re-triggering the DAG with a date, not `airflow backfill`.**
The Decision above assumed the DAG would read the day from its logical date
(`{{ ds }}`). An Airflow logical date is wall-clock time, while a simulated
day lasts 300 real seconds (ADR-0007). Mapping one onto the other would
bring every logical date into conflict with Airflow's calendar. So
`daily_settlement` takes the day as a `business_date` parameter and has no
schedule of its own:

- A `sim_clock_tick` DAG runs every real minute. It triggers settlement once
  for each simulated day that has ended, plus 45 simulated minutes' grace
  for the stream's watermark.
- Each trigger uses a deterministic run id, `settle__<sim_id>__<date>`, with
  `skip_when_already_exists`, and is recorded in `ops.settlement_triggers`
  after it succeeds. A crash between the two steps cannot settle a day twice.
- Restating a day is triggering the same DAG with the same date and a
  reason: `python scripts/settle.py --date D --restate --reason R`.
- Every run carries the `sim_id` of the simulation it was triggered for.
  The quality gate and the Spark job refuse, without retrying, a run whose
  simulation has since been reset. This was found by running it: after a
  Docker restart, Airflow resumed a run interrupted before a reset, and that
  run settled the *new* simulation's half-finished day under its own name.

The property the Decision relied on survives. Restatement is one command
against one date partition, with retries, sensors and run history. Only the
command is different.

**Runs are append-only in the serving store.** A settlement run inserts its
own bills, zone figures and reconciliation, keyed by `run_id`. It never
overwrites an earlier run. The `batch.current_*` views expose the latest
*successful* run for each day. A restated day therefore keeps its original
bills beside the new ones, and the database itself holds the audit trail,
not only Airflow's run history. A failed run changes nothing that is
served. See ADR-0005, Amendment 2.

**Settlement runs as a child process, on the speed layer's Spark version.**
The Airflow image adds a Java runtime and PySpark 3.5.9 with the same
connector jars as the speed layer, so both layers run the same engine and
the same shared validation code (ADR-0004). Airflow 3.1's dependency
constraints pin PySpark 4.0.1, so PySpark is installed separately from
them. The `settle` task starts Spark with `python -m
smartgrid.batch.settlement` as a subprocess, passing arguments as a list
and never through a shell. The JVM therefore starts clean for every run and
exits with it, rather than living on in a long-running Airflow worker.

**Sensors reschedule; they do not block.** `wait_for_drop` and
`wait_for_archive` use `mode="reschedule"`, so a waiting sensor holds no
worker slot. The archive sensor waits until `ops.stream_progress` shows the
speed layer has archived readings past the end of the day plus the grace
period.

**Measured on the running system.** A simulated day of 28,245 readings in
309 archive files settled end to end in 89 s. From the clock tick's trigger
to the published report, the settle task took 74 s. The Spark job's own
steps took 50 s:

| Step | Seconds |
|---|---|
| List the archive | 9.1 |
| Read and re-validate | 24.9 |
| Deduplicate | 2.9 |
| Aggregate | 9.7 |
| Write the snapshot | 2.8 |
| Bill and commit | 0.1 |

The remaining 24 s is starting the JVM and Python in the child process.
This is well inside the 300 s of one simulated day, so settlement keeps up
with the clock. Reading many small Parquet files dominates, as ADR-0005
anticipated. On a host short of memory the same job took over 7 minutes, so
the host needs about 6 GB free for the stack (see the README).
