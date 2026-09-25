# ADR-0002: Use Spark Structured Streaming for the speed layer

- **Status:** Accepted
- **Date:** 2026-09-20
- **Deciders:** Project team

## Context

[ADR-0001](0001-lambda-over-kappa.md) commits us to two processing paths and
identifies logic drift between them as the principal risk. The stream engine
choice is therefore not only about stream processing — it is about how much
of the batch path can share machinery with the speed path.

Three properties are required:

1. **Event-time windowing with late-data handling.** Meter readings arrive
   out of order and late. A zone aggregate must be attributed to the window
   in which the reading was *taken*, not the one in which it arrived.
2. **Exactly-once effects on the sink.** The speed layer upserts into
   PostgreSQL. Duplicate application of a window aggregate after a restart
   would silently corrupt the operational view.
3. **Reuse across layers.** Whatever runs the stream should also run the
   batch settlement, so the two paths share an execution model.

The module's preferred stack names Apache Spark (Structured Streaming) or
Apache Storm.

## Decision

**Use Spark Structured Streaming for the speed layer, and Spark batch for
the settlement layer.**

The deciding factor is the third requirement. Spark is the only option that
lets the speed layer and the batch layer be the same engine, the same API
and the same deployment artefact, differing only in whether the source is a
stream or a set of Parquet partitions. That collapses a large part of the
dual-path risk that ADR-0001 accepts.

On the first two requirements Spark is also the better fit:

- **Event time is native.** `withWatermark` plus windowed aggregation is a
  first-class construct. Late records within the watermark are folded into
  the correct window; records beyond it are dropped deterministically and
  countably.
- **Exactly-once is supported end to end** via checkpointed offsets plus an
  idempotent sink. Our sink upserts on `(zone, window_start)`, so replay
  after failure converges rather than accumulates.

## Alternatives considered

### Apache Storm

**What it gives.** Genuine per-event processing with very low latency —
lower than Spark's micro-batch model. For a pure event-at-a-time workload
with sub-second requirements, Storm is the stronger engine.

**Why rejected.**

- **Delivery semantics.** Core Storm is at-least-once. Exactly-once requires
  Trident, which adds its own abstraction and state model on top.
- **No native event-time windowing.** Windowing is processing-time oriented;
  event-time semantics and watermarking must be built by hand. Given that
  late meter reads are a central concern of this use case, hand-rolling that
  logic would be both effortful and a correctness risk.
- **No batch reuse whatsoever.** Choosing Storm means the batch layer is a
  separate engine with a separate API, maximising exactly the drift risk
  ADR-0001 is trying to contain.
- Our latency requirement is ≤60 s. Storm's advantage is real but lands well
  below the threshold that matters here, so we would be paying its costs for
  headroom we cannot use.

### Apache Flink

**What it gives.** Arguably the best pure stream processor available: true
streaming rather than micro-batch, the most mature event-time and state
model, and excellent exactly-once support.

**Why rejected.** Only on scope. Flink is outside the module's named stack,
and its batch API — while capable — would not give the same
one-artefact-two-modes property that Spark does for a team of this size on
this timeline. Noted in the report's limitations as the engine we would
evaluate first at production scale, particularly paired with an Iceberg sink
(see ADR-0001's revisit conditions).

### Kafka Streams

**What it gives.** No cluster to operate; the application *is* the
processor. Excellent for stream–table joins, which we need.

**Why rejected.** JVM-only, which conflicts with a Python codebase, and it
offers nothing for the batch layer. The operational simplicity is real but
does not outweigh losing engine reuse.

## Consequences

**Positive**

- One engine, one API, one set of deployment knowledge across both Lambda
  layers.
- Event-time correctness and late-data handling come from the framework
  rather than from code we would have to defend line by line.
- The shared transformation module ([ADR-0004](0004-shared-transformation-module.md))
  can be applied identically in both paths, because both paths are Spark.

**Negative**

- Micro-batch means latency is bounded below by the trigger interval. We
  cannot go meaningfully below a second, and do not try to.
- Spark is heavyweight for the demo's data volume — a JVM cluster to
  aggregate a few hundred readings per second is disproportionate.
- Applying the shared Python rules inside Spark requires pandas UDFs rather
  than native Catalyst expressions, which costs throughput. That cost is
  accepted deliberately; see ADR-0004.

**Mitigations**

- Spark runs in Docker in `local[2]` mode. This is not incidental: it avoids
  the `winutils.exe` / `HADOOP_HOME` failure mode that Spark-on-Windows
  otherwise imposes, and it keeps the whole stack reproducible from
  `docker compose up`.
- Trigger interval is set to the freshness requirement, not to the lowest
  achievable value, so we are not paying for latency headroom we do not need.

## Revisit if

- The freshness requirement drops below ~1 s, at which point micro-batch
  becomes the binding constraint and Flink or Storm deserve re-evaluation.
- The batch layer is removed (see ADR-0001's revisit conditions), which
  eliminates the engine-reuse argument that decided this record.
- Throughput grows to where pandas-UDF overhead in the speed layer dominates,
  forcing the rule-compilation approach described in ADR-0004.
