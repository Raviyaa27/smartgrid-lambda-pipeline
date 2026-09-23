.PHONY: help up down reset logs ps verify
.DEFAULT_GOAL := help

help:   ; @echo "targets: up down reset logs ps verify"
up:     ; docker compose up -d && docker compose ps
down:   ; docker compose down
reset:  ; docker compose down -v
logs:   ; docker compose logs -f --tail=100
ps:     ; docker compose ps
verify: ; python scripts/smoke_test.py