param(
    [string]$BaseUrl = "http://localhost:8097",
    [Parameter(Mandatory = $true)][string]$ReadKey,
    [Parameter(Mandatory = $true)][string]$AdminKey
)

$ErrorActionPreference = "Stop"
$readHeaders = @{ "X-EdgeOps-Key" = $ReadKey }
$adminHeaders = @{ "X-EdgeOps-Key" = $AdminKey }

Invoke-RestMethod "$BaseUrl/health" | Out-Null
$meta = Invoke-RestMethod "$BaseUrl/v1/meta" -Headers $readHeaders
if ($meta.mode -ne "simulation") { throw "Unexpected collector mode: $($meta.mode)" }
$body = @{
    scenario_id = "ingestion-backpressure"
    seed = 42
    speed = 20
    variant_id = "degraded"
} | ConvertTo-Json
$run = Invoke-RestMethod "$BaseUrl/v1/simulation/runs" -Method Post -Headers $adminHeaders -ContentType "application/json" -Body $body
if ($run.simulation_contract_version -ne 2) { throw "Unexpected simulation contract version" }
Invoke-RestMethod "$BaseUrl/v1/metrics/ingestion" -Headers $readHeaders | Out-Null
Invoke-RestMethod "$BaseUrl/v1/simulation/runs/current" -Method Delete -Headers $adminHeaders | Out-Null
