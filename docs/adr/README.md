# Architecture Decision Records

Each record captures one decision, the context that forced it, the
alternatives that were genuinely considered, and the consequences the team
accepted. Records are immutable once accepted: a decision that turns out to
be wrong is superseded by a new record rather than edited in place, so the
reasoning history stays auditable.

These records were written **before** the pipeline was implemented. The
architecture drove the build rather than being reverse-engineered from it.

| ADR | Decision | Status |
|-----|----------|--------|
| [0001](0001-lambda-over-kappa.md) | Lambda architecture, not Kappa | Accepted |
| [0002](0002-spark-structured-streaming-over-storm.md) | Spark Structured Streaming for stream processing | Accepted |
| [0003](0003-kafka-topic-and-retention-design.md) | Kafka topic, partitioning and retention design | Accepted |
| [0004](0004-shared-transformation-module.md) | One shared transformation module for both layers | Accepted |
| [0005](0005-parquet-master-dataset-postgres-serving.md) | Parquet on object storage as master dataset, PostgreSQL as serving store | Accepted |
| [0006](0006-airflow-for-orchestration.md) | Airflow for batch orchestration | Accepted |
| [0007](0007-simulated-clock-and-time-compression.md) | Simulated clock at 288x compression | Accepted |

ADR-0001 is the load-bearing record. Everything else follows from it.

New records use [0000-template.md](0000-template.md).
