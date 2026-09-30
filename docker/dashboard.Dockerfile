# ─────────────────────────────────────────────────────────────────────────
#  Business dashboard: Streamlit over the serving API (Section 9).
#
#  Same Python 3.11 base as the other images. The source and the Streamlit
#  theme (.streamlit/config.toml) are mounted at /app by docker compose.
# ─────────────────────────────────────────────────────────────────────────
FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

COPY docker/dashboard-requirements.txt /tmp/dashboard-requirements.txt
RUN pip install -r /tmp/dashboard-requirements.txt \
 && python -c "import streamlit, plotly, pandas, requests; print('dashboard image OK', streamlit.__version__)"

RUN useradd --create-home --uid 1000 dashboard
USER dashboard

WORKDIR /app
ENV PYTHONPATH=/app/src
EXPOSE 8501
CMD ["streamlit", "run", "src/smartgrid/dashboard/app.py", \
     "--server.port", "8501", "--server.address", "0.0.0.0"]
