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
        "up"     { docker compose up -d; docker compose ps }
        "down"   { docker compose down }
        "reset"  {
            Write-Host "Destroying all containers AND volumes (data will be lost)..." -ForegroundColor Yellow
            docker compose down -v
        }
        "logs"   { docker compose logs -f --tail=100 }
        "ps"     { docker compose ps }
        "verify" { python scripts/smoke_test.py }
        default  {
            Write-Host ""
            Write-Host "Usage: .\scripts\dev.ps1 <task>"
            Write-Host ""
            Write-Host "  up       start the infrastructure stack"
            Write-Host "  down     stop it (volumes preserved)"
            Write-Host "  reset    stop it and DELETE all volumes"
            Write-Host "  logs     follow logs from all services"
            Write-Host "  ps       show service status"
            Write-Host "  verify   run the infrastructure smoke test"
            Write-Host ""
        }
    }
}
finally { Pop-Location }