# EdgeOps Collector Simulation

`edgeops-collector-simulation` is an independent, simulation-only collector for training and
evaluation. It has no Prometheus reader, Docker SDK, production URL, mode switch, or import from a
sibling repository. Its internal port is `8095`; the later Compose phase will publish it as host
port `8097`.

Docker Compose integration is intentionally not included in this change.

## Start locally

```powershell
Copy-Item .env.example .env
uv sync --locked --dev
uv run uvicorn edgeops_collector_simulation.main:create_app --factory --host 0.0.0.0 --port 8097
```

Set different values for `SIMULATION_COLLECTOR_API_KEY` and
`SIMULATION_COLLECTOR_ADMIN_API_KEY`. Both are sent in the `X-EdgeOps-Key` header; never append a key
to the URL.

## API

The simulation service retains the production-compatible read routes and always exposes its admin
routes:

| Method | Path | Key |
| --- | --- | --- |
| `GET` | `/health` | None |
| `GET` | `/v1/meta` | Read |
| `GET` | `/v1/metrics/ingestion` | Read |
| `GET` | `/v1/devices/{device_id}` | Read |
| `GET` | `/v1/services` | Read |
| `GET` | `/v1/services/{service}` | Read |
| `GET` | `/v1/services/{service}/logs` | Read |
| `GET` | `/v1/simulation/scenarios` | Admin |
| `POST` | `/v1/simulation/runs` | Admin |
| `GET` | `/v1/simulation/runs/current` | Admin |
| `DELETE` | `/v1/simulation/runs/current` | Admin |

Start a contract-v2 run with an explicit variant:

```json
{
  "scenario_id": "ingestion-backpressure",
  "seed": 42,
  "speed": 20,
  "variant_id": "degraded"
}
```

Valid variants are `mild`, `degraded`, and `severe`. If omitted, the variant is selected from the
sorted IDs using `seed modulo 3`. Unknown scenarios or variants return `422`; only a valid request
that overlaps an active run returns `409`.

Run responses include `simulation_contract_version: 2`, the selected variant, resolved numeric
effects, workload scale, and the sorted optional metric groups masked for the whole run. Read
envelopes always report `collector_mode: "simulation"` and `simulated: true`; run provenance is
populated through `normal`, the incident phase, and the full explicit `recovery` phase.

## Simulation mechanics

- Numeric ranges resolve once from SHA-256-derived random streams.
- Workload scale is sampled once from `[0.65, 1.50]`.
- A fixed one-second simulated step uses correlated workload noise.
- Observation timestamps advance in simulated seconds, so `speed` accelerates collection
  without inflating the per-second rates used by the ML model.
- Healthy heartbeat ages cycle between readings, using the profile's
  `heartbeat_interval_seconds` (default `2.0`, matching the ESP32 publishing interval).
  Incident delay is added to that age; it is not the only source of nonzero heartbeat age.
- Normal windows include seeded label-independent nuisance events.
- Incident effects ramp during the first 20% of the incident.
- Recovery interpolates for 60% of its phase while preserving the `recovery` provenance.
- `service_states` effects (for example `emqx: {status: exited, health: null}`) replace a
  service's profile status and health for the whole incident phase only.
- Counters remain monotonic and queues use conservation with bounded depth.
- Optional A2 groups are masked per run at 15%, with at most three groups; keys remain present as
  `null`, while required A1 fields are never masked.

Restart or rebuild this collector before retraining model version 0.4. Regenerate old datasets:
the previous always-zero healthy heartbeat ages and wall-clock rate timestamps cannot be
corrected by changing a CSV schema or model version string.

## Postman

Use these variables:

```text
baseUrl=http://localhost:8097
readKey=<simulation read key>
adminKey=<simulation admin key>
scenarioId=ingestion-backpressure
variantId=degraded
```

For `POST {{baseUrl}}/v1/simulation/runs`, add `X-EdgeOps-Key: {{adminKey}}` and the JSON body above.
For telemetry, use `X-EdgeOps-Key: {{readKey}}`. Assert the contract version is `2`, the returned
variant matches, and `resolved_effects`, `workload_scale`, and `unavailable_metric_groups` exist.
Poll a read route until `metadata.simulation_phase === "recovery"` to assert that recovery remains
explicit provenance. Send keys only as HTTP headers; query-string keys are rejected.

## Quality checks

```powershell
uv run ruff check .
uv run ruff format --check .
uv run pytest
```
