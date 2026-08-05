$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot
uv run uvicorn edgeops_collector_simulation.main:create_app --factory --host 0.0.0.0 --port 8097
