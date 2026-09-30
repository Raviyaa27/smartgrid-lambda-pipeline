<#
.SYNOPSIS
    Task runner for smartgrid-lambda-pipeline (Windows).
    Linux/macOS equivalent lives in the Makefile.
.EXAMPLE
    .\scripts\dev.ps1 up
#>
param([Parameter(Position = 0)][string]$Task = "help")

$ErrorActionPreference = "Stop"
Push-Location (Split-Path -Parent $PSScriptRoot)
try {
    switch ($Task.ToLower()) {
        # -- Infrastructure --------------------------------------------------
        "up"          { docker compose up -d --remove-orphans; docker compose ps }
        "down"        { docker compose down }
        "reset"       {
            Write-Host "Destroying all containers AND volumes (data will be lost)..." -ForegroundColor Yellow
            docker compose down -v
        }
        "logs"        { docker compose logs -f --tail=100 }
        "ps"          { docker compose ps }
        "verify"      { python scripts/smoke_test.py }

        # -- Development -----------------------------------------------------
        "test"        { python -m pytest }

        # -- Pipeline --------------------------------------------------------
        "clock"       { python -m smartgrid.common.clock_store show }
        "clock-reset" { python -m smartgrid.common.clock_store reset }
        "produce"     { python -m smartgrid.producers.meter_simulator }
        "chaos"       { python -m smartgrid.producers.meter_simulator --faults chaos }
        "inspect"     { python scripts/inspect_stream.py --seconds 30 }
        "drop"        { python -m smartgrid.producers.daily_batch_source follow }
        "drops"       { python scripts/inspect_drops.py }
        "speed-logs"  { docker compose logs -f --tail=100 speed-layer }
        "speed"       { python scripts/inspect_speed_layer.py }
        "spark-test"  { docker compose run --rm --no-deps speed-layer python -m pytest tests/spark -p no:cacheprovider }
        "build"       { docker compose build speed-layer }
        "sim-reset"   {
            Write-Host "Starting a NEW simulation: clock to day 1; drops, archive, real-time tables and Kafka topics cleared." -ForegroundColor Yellow
            Write-Host "Stop the meter simulator and daily batch source first; restart them afterwards." -ForegroundColor Yellow
            docker compose stop speed-layer
            python -m smartgrid.common.simulation reset --yes
            if ($?) { docker compose start speed-layer }
        }

        default  {
            Write-Host ""
            Write-Host "Usage: .\scripts\dev.ps1 <task>"
            Write-Host ""
            Write-Host "  Infrastructure"
            Write-Host "    up           start the stack"
            Write-Host "    down         stop it (volumes preserved)"
            Write-Host "    reset        stop it and DELETE all volumes"
            Write-Host "    logs         follow logs from all services"
            Write-Host "    ps           show service status"
            Write-Host "    verify       run the infrastructure smoke test"
            Write-Host ""
            Write-Host "  Development"
            Write-Host "    test         run the test suite"
            Write-Host ""
            Write-Host "  Pipeline"
            Write-Host "    clock        show the shared simulated clock"
            Write-Host "    clock-reset  restart simulated time from SIM_START_DATE"
            Write-Host "    produce      run the meter simulator (Ctrl+C to stop)"
            Write-Host "    chaos        run it with ~10x the realistic fault rate"
            Write-Host "    inspect      measure fault detection on the live stream"
            Write-Host "    drop         run the daily batch source (Ctrl+C to stop)"
            Write-Host "    drops        run the quality gate over every daily drop"
            Write-Host "    speed-logs   follow the speed layer's logs"
            Write-Host "    speed        reconcile the speed layer: Kafka, archive, dead letters"
            Write-Host "    spark-test   run the Spark tests inside the Spark image"
            Write-Host "    build        rebuild the Spark image"
            Write-Host "    sim-reset    new simulation: clock, drops, archive, tables and topics"
            Write-Host ""
        }
    }
}
finally { Pop-Location }
