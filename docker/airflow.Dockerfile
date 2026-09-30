# ═══════════════════════════════════════════════════════════════════════
#  Airflow 3.1 with the batch layer's Spark runtime (ADR-0006, ADR-0002).
#
#  The official image, plus exactly what the Spark image carries -- Java,
#  PySpark 3.5.9 and the same pinned Kafka/S3A connector jars -- so the
#  nightly settlement runs the same engine, the same code and the same
#  connector versions as the speed layer. Both images run Python 3.11.
#
#  Airflow 3.1 rather than 2.x: the 2.x line reached end of life in April
#  2026, and pinning an unsupported orchestrator would be a choice to defend
#  rather than a default.
# ═══════════════════════════════════════════════════════════════════════
FROM apache/airflow:3.1.0-python3.11

ARG AIRFLOW_VERSION=3.1.0
ARG PYTHON_VERSION=3.11
ARG SPARK_VERSION=3.5.9
ARG SCALA_BINARY=2.12
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL2_VERSION=2.11.1
ARG HADOOP_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.262

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jre-headless curl \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java
COPY docker/log4j2.properties /opt/spark-conf/log4j2.properties
ENV SPARK_CONF_DIR=/opt/spark-conf

USER airflow
COPY docker/airflow-requirements.txt /tmp/airflow-requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/airflow-requirements.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# PySpark separately, OUTSIDE the constraints. Airflow 3.1's constraints pin
# pyspark 4.0.1 for its optional Spark provider; the speed layer runs 3.5.9,
# and the two Lambda layers must run the same engine (ADR-0002). This image
# does not include the Spark provider, so nothing in Airflow imports PySpark;
# its only dependency is py4j.
RUN pip install --no-cache-dir "pyspark==${SPARK_VERSION}"

# The same connectors as docker/spark.Dockerfile, pinned to the same versions.
RUN set -eux; \
    JARS="$(python -c 'import os, pyspark; print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))')"; \
    MAVEN=https://repo1.maven.org/maven2; \
    fetch() { curl -fsSL --retry 3 -o "$JARS/$(basename "$1")" "$MAVEN/$1"; }; \
    fetch "org/apache/spark/spark-sql-kafka-0-10_${SCALA_BINARY}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_BINARY}-${SPARK_VERSION}.jar"; \
    fetch "org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_BINARY}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_BINARY}-${SPARK_VERSION}.jar"; \
    fetch "org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar"; \
    fetch "org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar"; \
    fetch "org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar"; \
    fetch "com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar"

RUN java -version 2>&1 | head -n 1 \
 && python -c "import pyspark, pandas, pyarrow, psycopg; print('pyspark', pyspark.__version__, '| pandas', pandas.__version__)" \
 && airflow version

# The repository is mounted read-only at /app (settings resolve /app/.env).
ENV PYTHONPATH=/app/src
