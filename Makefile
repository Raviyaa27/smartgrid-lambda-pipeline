.PHONY: help up down reset logs ps verify test clock clock-reset produce chaos inspect drop drops speed speed-logs spark-test build settlement airflow-logs api-logs sim-reset
.DEFAULT_GOAL := help

help:        ; @echo "targets: up down reset logs ps verify | test | clock clock-reset produce chaos inspect drop drops speed speed-logs spark-test build settlement airflow-logs api-logs sim-reset"

# -- Infrastructure --------------------------------------------------------
up:          ; docker compose up -d --remove-orphans && docker compose ps
down:        ; docker compose down
reset:       ; docker compose down -v
logs:        ; docker compose logs -f --tail=100
ps:          ; docker compose ps
verify:      ; python scripts/smoke_test.py

# -- Development -----------------------------------------------------------
test:        ; python -m pytest

# -- Pipeline --------------------------------------------------------------
clock:       ; python -m smartgrid.common.clock_store show
clock-reset: ; python -m smartgrid.common.clock_store reset
produce:     ; python -m smartgrid.producers.meter_simulator
chaos:       ; python -m smartgrid.producers.meter_simulator --faults chaos
inspect:     ; python scripts/inspect_stream.py --seconds 30
drop:        ; python -m smartgrid.producers.daily_batch_source follow
drops:       ; python scripts/inspect_drops.py
speed:       ; python scripts/inspect_speed_layer.py
speed-logs:  ; docker compose logs -f --tail=100 speed-layer
spark-test:  ; docker compose run --rm --no-deps speed-layer python -m pytest tests/spark -p no:cacheprovider
build:       ; docker compose build speed-layer airflow api
settlement:  ; python scripts/inspect_settlement.py
airflow-logs: ; docker compose logs -f --tail=100 airflow
api-logs:    ; docker compose logs -f --tail=100 api
sim-reset:   ; docker compose stop speed-layer airflow && python -m smartgrid.common.simulation reset --yes && docker compose start speed-layer airflow
