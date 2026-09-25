# ADR-0005: Parquet on object storage as master dataset, PostgreSQL as serving store

- **Status:** Accepted
- **Date:** 2026-09-20
- **Deciders:** Project team

## Context

[ADR-0001](0001-lambda-over-kappa.md) requires an immutable master dataset
that the batch layer can recompute from, and a queryable store that the API
and dashboard can serve from. These are different jobs with different access
patterns, and trying to satisfy both with one technology compromises both.

| | Master dataset | Serving store |
|---|---|---|
| Written by | Speed layer, continuously appending | Both layers, small result sets |
| Read by | Batch layer, whole partitions at a time | API and dashboard, point and range queries |
| Access pattern | Full scan of one date partition | Indexed lookup, ad-hoc joins |
| Mutability | Append-only, never updated | Upserted and overwritten |
| Volume | ~30 GB/day at production scale | Aggregates only; orders of magnitude smaller |
| Retention | Years | Current window plus recent history |

The module's preferred stack allows PostgreSQL or Cassandra for the database
and HDFS or an S3 bucket with Parquet for the file system.

## Decision

**Both, each for its own job.**

- **Master dataset: Parquet on MinIO (S3-compatible object storage)**,
  partitioned by `dt=YYYY-MM-DD` and sub-partitioned by `grid_zone`.
- **Serving store: PostgreSQL**, with three schemas — `speed` (provisional),
  `batch` (settled, authoritative) and `ops` (run log, data-quality results,
  reconciliation).

### Why Parquet, partitioned by date

- **Columnar and compressed.** Meter readings are numeric, repetitive and
  wide; columnar encoding achieves 8–10× on this data, which is the
  difference between 288 GB/day and ~30 GB/day at production scale.
- **Date partitioning *is* the restatement mechanism.** Recomputing one day
  means reading exactly one partition. This is the property that makes
  ADR-0001's replay argument concrete: without partition pruning, "re-run one
  day" would mean scanning years.
- **Immutable and append-only**, so the batch layer is deterministic. The
  same input partition produces the same output, which is what makes a bill
  reproducible at audit time.

### Why S3-compatible object storage, not HDFS

MinIO speaks the S3 API, so every path and client call in the codebase is
identical to what a real deployment against S3 would use. Moving to AWS is a
change of endpoint and credentials, not a change of code. HDFS would have
required a NameNode and DataNode for a demo with no need for either, and
would have coupled the storage layer to a Hadoop deployment we otherwise do
not want.

### Why PostgreSQL for serving

Our serving queries are **relational and ad-hoc**: household joined to
tariff joined to zone, filtered by date, aggregated for a report. That is the
access pattern relational databases exist for. The serving volume is
aggregates — zone-minute metrics and daily bills — not raw readings, so it is
orders of magnitude smaller than the master dataset and comfortably within
what a single PostgreSQL instance handles.

The three-schema split enforces ADR-0001's merge rule in the storage layer
itself. `speed` and `batch` are different namespaces with different
guarantees, documented in `COMMENT ON SCHEMA` so the distinction survives
contact with anyone reading the database directly. A consumer cannot
accidentally treat a provisional figure as settled, because it has to name
the schema to read it.

Reusing the same instance for Airflow's metadata database is a demo
convenience, stated as such.

## Alternatives considered

### Cassandra as the serving store

**What it gives.** Excellent write throughput, linear scaling, and a data
model built for exactly the shape of per-meter time series we are producing.
At 10⁶ meters writing every minute, Cassandra is the right answer for raw
reading storage.

**Why rejected here.** Cassandra requires the query patterns to be known in
advance, because the table design *is* the query plan. It does not do ad-hoc
joins. Our serving layer needs household ⋈ tariff ⋈ zone with filters that
vary by report, and the daily settlement report is exactly the kind of
ad-hoc relational aggregation Cassandra is poor at. At our serving volume —
aggregates, not raw readings — its scaling advantage is not exercised, so we
would be paying its modelling rigidity for throughput we do not need.

**Where it would win.** If the serving layer had to hold raw per-meter
readings for interactive query at production scale, Cassandra (or a
purpose-built time-series store) would replace PostgreSQL for that table
while PostgreSQL kept the relational aggregates.

### One store for both jobs

**Parquet only**, with the API querying files directly: rejected because
every dashboard refresh becomes a file scan, and there are no indexes,
upserts or concurrent-writer semantics.

**PostgreSQL only**, holding raw readings as the master dataset: rejected on
cost and shape. Row-oriented storage on attached disk for ~288 GB/day, with
full-partition scans for every restatement, discards the compression and
partition-pruning benefits that make ADR-0001's replay argument work.

### A table format (Iceberg or Delta Lake) over the Parquet files

**What it gives.** ACID transactions, schema evolution, time travel and
snapshot isolation on top of the same Parquet — genuinely better than raw
Parquet for a master dataset, and it would make restatement transactional
rather than a directory overwrite.

**Why rejected.** Scope and added dependency weight for a two-week project.
This is the clearest single upgrade path and is recorded as the first item
of future work in the report's limitations section; it also appears in
ADR-0001's revisit conditions, because a mature Iceberg-backed stream
processor is one of the conditions under which the batch layer becomes
redundant.

## Consequences

**Positive**

- Each store does the job it is good at; neither is stretched.
- Restatement reads exactly one date partition.
- Storage cost tracks the cheap tier for the large dataset and the
  convenient tier for the small one.
- S3 API means no lock-in and no code change to move to a cloud deployment.
- The provisional/settled distinction is structural, not conventional.

**Negative**

- Two storage technologies to run, back up and reason about.
- Raw Parquet has no transactions: a batch job that fails midway can leave a
  partially written partition.
- Single PostgreSQL instance is a single point of failure and a scaling
  ceiling.
- Small-file proliferation is a known Parquet failure mode when a streaming
  job writes frequently.

**Mitigations**

- Batch writes are **overwrite-by-partition**, so a failed run leaves the
  previous partition intact and a re-run is idempotent rather than additive.
- The speed layer's Parquet writes are compacted by the daily settlement DAG,
  which rewrites the day's partition as part of settling it — small-file
  cleanup happens as a side effect of work already being done.
- PostgreSQL's scaling limit is documented rather than engineered around; the
  report states the volume at which it would need replacing.

## Revisit if

- Serving volume grows past a single PostgreSQL instance, or the serving
  layer must hold raw readings — introduce Cassandra or a time-series store
  for that table specifically.
- Restatement needs to be transactional and concurrent with reads — adopt
  Iceberg or Delta.
- Small-file pressure appears before the daily compaction runs, which would
  mean the compaction interval is too coarse for the write rate.
