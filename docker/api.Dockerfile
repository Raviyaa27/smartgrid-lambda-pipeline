# ─────────────────────────────────────────────────────────────────────────
#  Serving API: FastAPI over the serving store (Section 8).
#
#  The same Python 3.11 base as the Spark image, so its layers are shared,
#  with only the API's own small dependency set. The source is mounted at
#  /app by docker compose, as for the other services, so code changes need
#  a restart, not a rebuild.
# ─────────────────────────────────────────────────────────────────────────
FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

COPY docker/api-requirements.txt /tmp/api-requirements.txt
RUN pip install -r /tmp/api-requirements.txt \
 && python -c "import fastapi, uvicorn, psycopg, psycopg_pool, boto3; print('api image OK')"

# Nothing in this container needs root.
RUN useradd --create-home --uid 1000 api
USER api

WORKDIR /app
ENV PYTHONPATH=/app/src
EXPOSE 8000
CMD ["python", "-m", "smartgrid.serving.api"]
