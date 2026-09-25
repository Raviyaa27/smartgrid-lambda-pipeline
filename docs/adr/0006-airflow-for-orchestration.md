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
