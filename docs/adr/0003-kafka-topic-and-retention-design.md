# ADR-0003: Kafka topic, partitioning and retention design

- **Status:** Accepted
- **Date:** 2026-09-20
- **Deciders:** Project team

## Context

Kafka is the ingestion bus. Three design choices need justifying, and the
third is where the architecture argument from [ADR-0001](0001-lambda-over-kappa.md)
becomes visible in configuration rather than prose.

## Decision

### Topics

| Topic | Partitions | Retention | Purpose |
|---|---|---|---|
| `meter.readings.v1` | 6 | 7 days | Validated and unvalidated meter readings |
| `meter.readings.dlq.v1` | 3 | 14 days | Records that failed validation |

**Version suffix in the topic name.** A breaking schema change publishes to
`v2` and runs both topics in parallel until consumers migrate. The
alternative — reinterpreting the bytes on an existing topic — silently
breaks every consumer that has not been updated, and breaks replay of
historical data permanently. The cost is a naming convention; the benefit is
that schema evolution is never a coordinated flag day.

**DLQ retention exceeds source retention.** A poison message is only
interesting once someone notices the error rate rose, which may be days
later. Expiring the evidence before the investigation starts defeats the
purpose of having a DLQ.

### Partitioning

Six partitions, **keyed by `meter_id`**.

- **Keyed, not round-robin**, because Kafka guarantees ordering only within
  a partition. Keying by meter means all readings from one meter land in one
  partition in order, which is what deduplication and per-meter sequencing
  depend on.
- **`meter_id` rather than `grid_zone`**, despite zone being the natural
  aggregation key. Zones are few and unequally populated, so keying by zone
  would produce hot partitions and cap parallelism at the number of zones.
  Meter id hashes evenly. The zone aggregation is a shuffle inside Spark,
  which is the right place to pay for it.
- **Six** gives headroom above the demo's consumer parallelism without
  making rebalances slow. Partition count can be increased later but never
  decreased, and increasing it changes the key-to-partition mapping — so it
  is chosen with room to grow.

### Retention: 7 days

This is the configuration line that encodes ADR-0001.

In this design **Kafka is transport and short-term recovery, not the system
of record.** Seven days covers the operational need — a consumer that fails
over a long weekend can still resume without data loss — and nothing more.
Long-horizon replay is the batch layer's job, served from immutable Parquet
in object storage.

The contrast with Kappa is exactly here. Under Kappa, Kafka retention must
span the restatement window, which ADR-0001 establishes as years: roughly
736 TB before replication, on broker-attached disk. Under Lambda, retention
spans the recovery window: days, a few GB. The same requirement is met by
cheap cold storage instead of expensive hot storage, and the config file
says so.

### Explicit topic creation

`KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`. Topics are created by a declarative
one-shot init job in `docker-compose.yml`.

Auto-creation produces topics with default partition counts and default
retention, silently, on first reference — including from a typo in a topic
name, which then looks like a working pipeline producing into a topic nobody
reads. Explicit creation makes topic configuration reviewable and makes a
misspelled topic fail loudly.

## Alternatives considered

### RabbitMQ or another classical message broker

Rejected on replay. A queue deletes a message once acknowledged; there is no
log to re-read. Even the 7-day recovery window we do keep would not be
available, and the speed layer could not be restarted from an earlier offset
after a bad deploy.

### Amazon Kinesis

Functionally close to Kafka and operationally simpler. Rejected for local
reproducibility: the grader must be able to run the entire system from
`docker compose up` with no cloud account. Vendor coupling in the ingestion
layer would also make the object-storage abstraction in
[ADR-0005](0005-parquet-master-dataset-postgres-serving.md) inconsistent —
we would be portable in storage and locked in at ingest.

### Longer Kafka retention "just in case"

Rejected because it quietly converts the design into Kappa without the
benefits. Retaining months of readings means paying Kappa's storage cost
while still running a batch layer. If the log is the system of record, say so
and delete the batch layer; if it is not, retain only what recovery needs.
Half-measures here are the expensive option.

## Consequences

**Positive**

- Per-meter ordering is guaranteed where deduplication needs it, with even
  partition load.
- Storage cost is bounded by the recovery window, not the retention window.
- Schema evolution has a defined, non-breaking path.
- Topic configuration is declarative, reviewable and reproducible.

**Negative**

- A single broker in the demo means replication factor 1: no fault tolerance
  at all. A broker loss loses up to 7 days of untransferred readings.
- Six partitions is a guess calibrated to demo scale, not a measurement.
- Recovering from an incident older than 7 days requires the batch layer and
  cannot be done by rewinding a consumer.

**Mitigations**

- The speed layer writes validated readings to Parquet continuously, so the
  master dataset is never more than a micro-batch behind Kafka. The exposure
  window for broker loss is seconds, not days.
- Replication factor 1 is a demo constraint and is stated as such in the
  report's limitations; production would use 3 with `min.insync.replicas=2`.

## Revisit if

- Throughput grows past what 6 partitions sustain — increase before the
  consumer becomes the bottleneck, accepting the key-mapping change.
- Per-meter ordering stops being required (for example, if deduplication
  moves to a content hash), which would free the key for a different choice.
- The deployment gains more than one broker, at which point replication
  factor and `min.insync.replicas` need setting deliberately.
