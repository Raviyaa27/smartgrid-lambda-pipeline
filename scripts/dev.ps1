<#
.SYNOPSIS
    Task runner for smartgrid-lambda-pipeline (Windows).
    Linux/macOS equivalent lives in the Makefile.
.EXAMPLE
    .\scripts\dev.ps1 demo
#>
param([Parameter(Position = 0)][string]$Task = "help")

$ErrorActionPreference = "Stop"
Push-Location (Split-Path -Parent $PSScriptRoot)
try {
    switch ($Task.ToLower()) {
        # -- One command -------------------------------------------------------
        "demo"        { python scripts/demo.py }
        "demo-quick"  { python scripts/demo.py --quick --no-build }

        # -- Stack -------------------------------------------------------------
        "up"          { docker compose up -d --build --remove-orphans; docker compose ps }
        "down"        { docker compose down }
        "reset"       {
            Write-Host "Destroying all containers AND volumes (data will be lost)..." -ForegroundColor Yellow
            docker compose down -v
        }
        "logs"        { docker compose logs -f --tail=100 }
        "ps"          { docker compose ps }
        "verify"      { python scripts/smoke_test.py }
        "build"       { docker compose build }
        "tools"       { docker compose --profile tools up -d kafka-ui }

        # -- Development -------------------------------------------------------
        "test"        { python -m pytest }
        "spark-test"  { docker compose run --rm --no-deps speed-layer python -m pytest tests/spark -p no:cacheprovider }
        "alerts-test" { docker run --rm --entrypoint promtool -v "${PWD}/infra/prometheus:/etc/prometheus:ro" prom/prometheus:v3.5.1 test rules /etc/prometheus/tests/rules_test.yml }

        # -- Simulation --------------------------------------------------------
        "sim-reset"   {
            Write-Host "Starting a NEW simulation: clock to day 1; drops, archive, settlements, tables and Kafka topics cleared." -ForegroundColor Yellow
            docker compose stop meter-simulator batch-source speed-layer airflow
            docker compose run --rm --no-deps meter-simulator python -m smartgrid.common.simulation reset --yes
            if ($?) { docker compose start meter-simulator batch-source speed-layer airflow }
        }
        "clock"       { python -m smartgrid.common.clock_store show }
        # The sources run in containers. To run one on the host (to debug it),
        # its container is stopped first, so two copies never both publish.
        "produce"     { docker compose stop meter-simulator; python -m smartgrid.producers.meter_simulator }
        "chaos"       { docker compose stop meter-simulator; python -m smartgrid.producers.meter_simulator --faults chaos }
        "drop"        { docker compose stop batch-source; python -m smartgrid.producers.daily_batch_source follow }

        # -- Inspect -----------------------------------------------------------
        "inspect"     { python scripts/inspect_stream.py --seconds 30 }
        "drops"       { python scripts/inspect_drops.py }
        "speed"       { python scripts/inspect_speed_layer.py }
        "settlement"  { python scripts/inspect_settlement.py }
        "alerts"      { python scripts/alerts.py --targets }
        "speed-logs"  { docker compose logs -f --tail=100 speed-layer }
        "airflow-logs" { docker compose logs -f --tail=100 airflow }
        "api-logs"    { docker compose logs -f --tail=100 api }
        "dashboard-logs" { docker compose logs -f --tail=100 dashboard }
        "source-logs" { docker compose logs -f --tail=100 meter-simulator batch-source }

        default  {
            Write-Host ""
            Write-Host "Usage: .\scripts\dev.ps1 <task>"
            Write-Host ""
            Write-Host "  One command"
            Write-Host "    demo           build, start, run a fresh simulation and check every claim (~16 min)"
            Write-Host "    demo-quick     the same up to day 1's settlement, without rebuilding (~8 min)"
            Write-Host ""
            Write-Host "  Stack"
            Write-Host "    up             build and start everything"
            Write-Host "    down           stop it (volumes preserved)"
            Write-Host "    reset          stop it and DELETE all volumes"
            Write-Host "    logs / ps      follow all logs / show service status"
            Write-Host "    verify         smoke-test every service"
            Write-Host "    build          rebuild every image"
            Write-Host "    tools          start Kafka UI (http://localhost:8085)"
            Write-Host ""
            Write-Host "  Development"
            Write-Host "    test           run the test suite"
            Write-Host "    spark-test     run the Spark tests inside the Spark image"
            Write-Host "    alerts-test    unit-test the alert rules with promtool"
            Write-Host ""
            Write-Host "  Simulation"
            Write-Host "    sim-reset      new simulation: clock, drops, archive, settlements, tables, topics"
            Write-Host "    clock          show the shared simulated clock"
            Write-Host "    produce        run the meter simulator on the host instead (debugging)"
            Write-Host "    chaos          ... with ~10x the realistic fault rate"
            Write-Host "    drop           run the daily batch source on the host instead (debugging)"
            Write-Host ""
            Write-Host "  Inspect"
            Write-Host "    inspect        measure fault detection on the live stream"
            Write-Host "    drops          run the quality gate over every daily drop"
            Write-Host "    speed          reconcile the speed layer: Kafka, archive, dead letters"
            Write-Host "    settlement     settlement runs, bills and the speed-vs-batch gap"
            Write-Host "    alerts         alerts firing or pending, and scrape targets"
            Write-Host "    *-logs         speed-logs, airflow-logs, api-logs, dashboard-logs, source-logs"
            Write-Host ""
        }
    }
}
finally { Pop-Location }
