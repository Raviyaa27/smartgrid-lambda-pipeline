# ─────────────────────────────────────────────────────────────────────────
#  Simulated sources: the smart-meter simulator and the daily batch source.
#
#  Containerised so that `docker compose up` runs the whole pipeline with
#  nothing installed on the host. The same image also runs one-off commands
#  (simulation reset, republishing or revising a drop). Source is mounted
#  at /app by docker compose, as for every other service.
# ─────────────────────────────────────────────────────────────────────────
FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

COPY docker/sources-requirements.txt /tmp/sources-requirements.txt
RUN pip install -r /tmp/sources-requirements.txt \
 && python -c "import confluent_kafka, boto3, psycopg, pandas; print('sources image OK')"

RUN useradd --create-home --uid 1000 sources
USER sources

WORKDIR /app
ENV PYTHONPATH=/app/src
CMD ["python", "-m", "smartgrid.producers.meter_simulator"]
