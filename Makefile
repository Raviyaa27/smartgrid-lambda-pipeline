.PHONY: help up down reset logs ps verify test clock clock-reset produce chaos inspect drop drops sim-reset
.DEFAULT_GOAL := help

help:        ; @echo "targets: up down reset logs ps verify | test | clock clock-reset produce chaos inspect drop drops sim-reset"

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
sim-reset:   ; python -m smartgrid.common.clock_store reset && python -m smartgrid.producers.daily_batch_source reset --yes
