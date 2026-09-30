# ═══════════════════════════════════════════════════════════════════════
#  Spark runtime for the Lambda layers (ADR-0002).
#
#  Why build our own image rather than use a published Spark one:
#    * The code needs Python >= 3.11; PySpark 3.5 supports up to 3.11, so
#      the interpreter is pinned to 3.11 here.
#    * The Kafka and S3A connectors are baked in at BUILD time. Nothing is
#      downloaded when a job starts, so a job cannot fail at 3 a.m. because
#      Maven Central was unreachable, and every run uses identical jars.
#    * Running Spark in Linux avoids the winutils.exe / HADOOP_HOME failure
#      mode of Spark on Windows entirely.
#
#  Versions are pinned together: the Kafka connector must match Spark
#  exactly, and hadoop-aws must match the Hadoop version PySpark bundles.
# ═══════════════════════════════════════════════════════════════════════
FROM python:3.11-slim-bookworm

ARG SPARK_VERSION=3.5.9
ARG SCALA_BINARY=2.12
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL2_VERSION=2.11.1
ARG HADOOP_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.262

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Java 17 (Debian bookworm's default JRE) runs Spark's JVM.
RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jre-headless curl procps \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

COPY docker/spark-requirements.txt /tmp/spark-requirements.txt
RUN pip install -r /tmp/spark-requirements.txt

# Connectors, dropped straight onto PySpark's classpath.
RUN set -eux; \
    JARS="$(python -c 'import os, pyspark; print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))')"; \
    MAVEN=https://repo1.maven.org/maven2; \
    fetch() { curl -fsSL --retry 3 -o "$JARS/$(basename "$1")" "$MAVEN/$1"; }; \
    fetch "org/apache/spark/spark-sql-kafka-0-10_${SCALA_BINARY}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_BINARY}-${SPARK_VERSION}.jar"; \
    fetch "org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_BINARY}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_BINARY}-${SPARK_VERSION}.jar"; \
    fetch "org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar"; \
    fetch "org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar"; \
    fetch "org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar"; \
    fetch "com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar"; \
    ls -la "$JARS" | grep -E "kafka|pool2|hadoop-aws|aws-java-sdk"

# Quiet the JVM so the job's structured JSON logs are the signal.
COPY docker/log4j2.properties /opt/spark-conf/log4j2.properties
ENV SPARK_CONF_DIR=/opt/spark-conf

# Fail the build, not the job, if the toolchain is broken.
RUN java -version 2>&1 | head -n 1 && python -c "import pyspark, pandas, pyarrow; print('pyspark', pyspark.__version__)"

# The repository is mounted at /app by docker-compose, so code changes need
# a restart, not a rebuild. The image carries dependencies only.
WORKDIR /app
ENV PYTHONPATH=/app/src
