.PHONY: help demo demo-quick up down reset logs ps verify build tools test spark-test alerts-test sim-reset clock produce chaos drop inspect drops speed settlement alerts speed-logs airflow-logs api-logs dashboard-logs source-logs
.DEFAULT_GOAL := help

help:        ; @echo "targets: demo demo-quick | up down reset logs ps verify build tools | test spark-test alerts-test | sim-reset clock produce chaos drop | inspect drops speed settlement alerts | speed-logs airflow-logs api-logs dashboard-logs source-logs"

# -- One command -----------------------------------------------------------
demo:        ; python scripts/demo.py
demo-quick:  ; python scripts/demo.py --quick --no-build

# -- Stack -------------------------------------------------------------------
up:          ; docker compose up -d --build --remove-orphans && docker compose ps
down:        ; docker compose down
reset:       ; docker compose down -v
logs:        ; docker compose logs -f --tail=100
ps:          ; docker compose ps
verify:      ; python scripts/smoke_test.py
build:       ; docker compose build
tools:       ; docker compose --profile tools up -d kafka-ui

# -- Development -------------------------------------------------------------
test:        ; python -m pytest
spark-test:  ; docker compose run --rm --no-deps speed-layer python -m pytest tests/spark -p no:cacheprovider
alerts-test: ; docker run --rm --entrypoint promtool -v "$(CURDIR)/infra/prometheus:/etc/prometheus:ro" prom/prometheus:v3.5.1 test rules /etc/prometheus/tests/rules_test.yml

# -- Simulation --------------------------------------------------------------
# The sources run in containers; the host-side targets stop the container
# first, so two copies never both publish.
sim-reset:   ; docker compose stop meter-simulator batch-source speed-layer airflow && docker compose run --rm --no-deps meter-simulator python -m smartgrid.common.simulation reset --yes && docker compose start meter-simulator batch-source speed-layer airflow
clock:       ; python -m smartgrid.common.clock_store show
produce:     ; docker compose stop meter-simulator && python -m smartgrid.producers.meter_simulator
chaos:       ; docker compose stop meter-simulator && python -m smartgrid.producers.meter_simulator --faults chaos
drop:        ; docker compose stop batch-source && python -m smartgrid.producers.daily_batch_source follow

# -- Inspect -------------------------------------------------------------------
inspect:     ; python scripts/inspect_stream.py --seconds 30
drops:       ; python scripts/inspect_drops.py
speed:       ; python scripts/inspect_speed_layer.py
settlement:  ; python scripts/inspect_settlement.py
alerts:      ; python scripts/alerts.py --targets
speed-logs:  ; docker compose logs -f --tail=100 speed-layer
airflow-logs: ; docker compose logs -f --tail=100 airflow
api-logs:    ; docker compose logs -f --tail=100 api
dashboard-logs: ; docker compose logs -f --tail=100 dashboard
source-logs: ; docker compose logs -f --tail=100 meter-simulator batch-source
