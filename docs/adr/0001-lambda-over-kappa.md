# ADR-0001: Adopt a Lambda architecture, not Kappa

- **Status:** Accepted
- **Date:** 2026-09-20
- **Deciders:** Project team

## Context

The system must answer the use case's business question:

> What is the current grid load and renewable contribution by zone, and what
> will each household's bill look like once daily tariff data is applied to
> their consumption?

That is not one question. It is two, and they have incompatible service
requirements. Decomposing them is the whole basis of this decision.

| | **Q1 — Grid operations** | **Q2 — Billing** |
|---|---|---|
| Question | Current load and renewable mix by zone | Household charge after tariff is applied |
| Freshness target | ≤ 60 s | T+1, available by 06:00 next day |
| Tolerance for approximation | **High** — a dropped or late meter read moves a zone aggregate by a fraction of a percent and changes no decision | **Zero** — the output is a monetary charge that is regulated, auditable and disputable by the customer |
| Correctness standard | Directionally right | Exact to 0.01 LKR, and reproducible on demand |
| Retention | Hours to days | Years (regulatory retention, dispute window) |
| Consumers | Operations dashboard, threshold alerts | Billing system, regulator, customer |
| Cost of being wrong | An operator looks at a slightly stale number | An incorrect charge, a compliance breach, a refund |

A single processing model serving both means either degrading Q1's latency
to Q2's rigour, or degrading Q2's rigour to Q1's latency. Neither is
acceptable, and the mismatch is not incidental to the domain — it is
intrinsic to a utility, where the control room and the billing department
have always been different systems with different guarantees.

### The restatement requirement

Billing outputs are not write-once. Four scenarios force recomputation of
already-published figures:

1. **Retroactive tariff revision.** A regulator backdates a rate change; every
   affected bill in the period must be reissued.
2. **Late or missing meter reads.** A meter is offline for hours and backfills
   on reconnection. Its consumption belongs to the day it occurred, not the
   day it arrived.
3. **Subsidy reclassification.** A household's means-tested status changes
   with retroactive effect.
4. **Defective meter correction.** A meter is found to have been
   mis-registering; its readings are corrected and downstream bills restated.

The restatement window is therefore the dispute and regulatory retention
window — **years**, not days. Any architecture that cannot cheaply recompute
an arbitrary historical day is disqualified.

### Volume, to scale the cost argument

Production scale for a utility of this kind, as order-of-magnitude estimates:

- 10⁶ smart meters × 1 reading/minute = **1.44 × 10⁹ events/day**
- at ~200 bytes/event ≈ **288 GB/day** raw
- over a 7-year retention window ≈ **736 TB**, before replication

Columnar Parquet with compression on this largely numeric, highly repetitive
data realistically achieves 8–10×, giving roughly **30 GB/day** or **75 TB**
over the same window.

## Decision

**Adopt a Lambda architecture.** A speed layer serves Q1 from the live
stream; a batch layer serves Q2 by recomputing from an immutable master
dataset; a serving layer merges them under an explicit, stated rule.

The justification is not that Lambda is more capable. It is that this use
case contains **two genuinely different consistency contracts**, and Lambda
is the architecture that lets each be satisfied on its own terms rather than
forcing a compromise between them.

The four axes the decision turns on:

| Axis | Requirement | Kappa | Lambda |
|---|---|---|---|
| **Latency** | <60 s for Q1, T+1 for Q2 | Meets both from one stream. **Kappa's strongest axis** | Speed layer meets Q1; batch layer meets Q2 |
| **Replay** | Recompute arbitrary historical days for years | Requires Kafka retention across the full restatement window, or an external archive — which is a batch layer under another name. Restating one tariff row means reprocessing the whole log from that point | Re-run one Airflow DAG day against the date-partitioned Parquet for that day. Targeted and bounded |
| **Cost** | See volume above | ~736 TB of broker-attached storage, replicated 3× ≈ **2.2 PB** of hot disk, to serve a handful of restatements per year | ~75 TB of cold object storage with built-in redundancy. Roughly an **order of magnitude cheaper**, on cheaper media |
| **Consistency** | Bills exact, reproducible, auditable | Achievable with transactional processing, but the serving store is continuously mutated by the stream. Proving at audit time that a specific bill is reproducible means reasoning about the stream's state at that instant | The batch layer is the declared system of record: deterministic, idempotent, versioned output derived from immutable input. Reproducing a bill is re-running one job |

### The merge rule

Lambda answers that lose marks are the ones that never say how the two views
are reconciled. Ours is explicit and implemented in the serving layer:

```
zone_metrics(zone, d) =
    batch.zone_settled(zone, d)     when d <  current simulated date   -- SETTLED
    speed.zone_metrics(zone, d)     when d == current simulated date   -- PROVISIONAL

household_bill(household, d) =
    batch.daily_billing(household, d)                                  -- SETTLED only
    (no provisional bill is ever published)
```

Two properties follow, and both are deliberate:

- **The batch view always wins.** Once a day is settled, the speed layer's
  figures for it are discarded, not merged or averaged. The speed layer is a
  temporary stand-in, never a contributor to the record.
- **There is no provisional bill.** Q2's tolerance for approximation is zero,
  so publishing an estimated charge would be a category error. The API and
  dashboard expose today's *consumption* in real time, but a *bill* only
  after settlement.

The dashboard labels every figure `PROVISIONAL` or `SETTLED` accordingly. A
number whose reliability is not stated is a number that will eventually be
misused.

### Quantifying the speed layer's error

The nightly settlement DAG compares, per zone, what the speed layer reported
for the day against what the batch layer settles, and records the delta in
`ops.reconciliation`. This turns "the speed layer is approximate" from an
assertion into a measured quantity, surfaced on the dashboard and alertable
when it drifts beyond threshold. It is also the honest way to report results:
the project can state its approximation error rather than claim it away.

## Alternatives considered

### Kappa — single stream-processing path

**What it gives.** One codebase, one engine, one operational surface. No
possibility of the two paths disagreeing, because there is only one path.
Reprocessing is "replay the log with a new consumer group", which is
conceptually clean. This is a real and serious advantage; Kappa is the
correct default for most streaming systems and the burden of proof is on
anyone choosing otherwise.

**Why rejected.** It fails the replay and cost axes together. Serving a
years-long restatement window requires either years of Kafka retention —
roughly 2.2 PB of replicated broker storage for a capability exercised a few
times a year — or a separate long-term archive that is read by a separate
batch process. The second option is Lambda with the labels removed, and
pretending otherwise would be dishonest about what was built.

The secondary problem is audit. Kappa's serving store is continuously
mutated by the stream. Demonstrating to a regulator that a bill issued in
March is reproducible in November requires reasoning about the stream's
state at a past instant, which is materially harder than re-running a
deterministic job over an immutable partition.

**Where Kappa would win.** If the requirement were only Q1 — real-time grid
load and renewable mix — Kappa would be the correct choice and Lambda would
be over-engineering. The batch layer exists solely to serve Q2. Remove
billing from the scope and this ADR should be superseded.

### Kappa with the daily feed as a compacted topic

**What it gives.** The daily tariff file is published to a log-compacted
Kafka topic and joined to the reading stream as a stream–table join. This
handles the two-source join elegantly and entirely within one engine, and it
is the strongest version of the Kappa case here.

**Why rejected.** It solves the join, which was never the hard part. It does
not solve restatement: recomputing March's bills after a backdated tariff
change still requires replaying March's readings, which still requires
retaining them. The cost and audit objections above are untouched.

### Batch-only, no speed layer

**What it gives.** Simplest possible system. Q2 is fully satisfied.

**Why rejected.** Fails Q1 outright. A ≤60 s freshness target cannot be met
by a daily job, and the threshold alerting the use case requires — renewable
contribution dropping below a floor in a zone — is worthless at T+1.

## Consequences

**Positive**

- Each question is served by a layer designed for its actual requirements;
  neither compromises for the other.
- Restatement is a first-class, cheap operation: re-run one DAG day.
- The master dataset is immutable and append-only, so the batch layer is
  deterministic and its output reproducible by anyone with the data.
- Storage cost tracks the cheap tier, not the expensive one.
- The speed-vs-batch delta is measurable, so the system reports its own
  approximation error instead of asserting correctness.

**Negative**

- **Two processing paths to build, test and maintain.** This is Lambda's
  real and well-documented weakness — Jay Kreps' "Questioning the Lambda
  Architecture" (2014) argues precisely that duplicated logic across two
  engines inevitably diverges, and that argument is correct as stated.
- Higher operational surface: a streaming job, an orchestrator, and a batch
  job, rather than one long-running application.
- Consumers must understand the provisional/settled distinction. A number
  read without that context can mislead.

**Mitigations**

- The dual-path objection is addressed structurally, not by discipline: all
  validation, normalisation and enrichment lives in **one module imported by
  both layers** (`src/smartgrid/common/transformations.py`), with rules
  declared once as data in `schemas.py`. See [ADR-0004](0004-shared-transformation-module.md).
  A unit test asserts the row-at-a-time and vectorised validators return
  identical verdicts for identical input, so drift fails the build rather
  than reaching production.
- Both layers run on the same engine (Spark), so the two paths share a
  dialect as well as a rule set. See [ADR-0002](0002-spark-structured-streaming-over-storm.md).
- The provisional/settled distinction is enforced in the storage layer
  itself: separate PostgreSQL schemas (`speed`, `batch`), commented in the
  DDL, so it cannot be lost in transit to a consumer.

## Revisit if

- **Billing leaves the scope.** With only Q1 remaining, Kappa is correct and
  this record should be superseded.
- **The restatement window collapses to days.** If regulation allowed
  disputes only within, say, 7 days, Kafka retention could cover the whole
  window and the cost argument against Kappa disappears.
- **Streaming engines gain cheap, durable, queryable long-term state.** A
  stream processor backed directly by an immutable columnar archive with
  targeted partition reprocessing would collapse this distinction. Flink with
  an Iceberg or Delta sink is already close; if that path matures, the batch
  layer becomes redundant.
- **The shared-module mitigation fails in practice.** If the two layers
  diverge despite it, the dual-path cost has been underestimated and the
  trade-off must be re-argued.
