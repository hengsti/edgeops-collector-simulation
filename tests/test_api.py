from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient

from edgeops_collector_simulation.config import Settings
from edgeops_collector_simulation.main import create_app


@pytest.fixture
def client(settings: Settings) -> Iterator[TestClient]:
    with TestClient(create_app(settings=settings)) as test_client:
        yield test_client


def test_health_is_public_and_read_routes_use_read_key(client: TestClient) -> None:
    assert client.get("/health").status_code == 200
    assert client.get("/v1/meta").status_code == 401
    response = client.get("/v1/meta", headers={"X-EdgeOps-Key": "read-test-key"})
    assert response.status_code == 200
    assert response.json()["mode"] == "simulation"


def test_read_envelope_has_fixed_simulation_metadata(client: TestClient) -> None:
    response = client.get("/v1/metrics/ingestion", headers={"X-EdgeOps-Key": "read-test-key"})
    assert response.status_code == 200
    metadata = response.json()["metadata"]
    assert metadata["collector_mode"] == "simulation"
    assert metadata["simulated"] is True
    assert metadata["scenario_id"] is None


def test_admin_routes_always_exist_and_use_admin_key(client: TestClient) -> None:
    route = "/v1/simulation/scenarios"
    assert client.get(route).status_code == 401
    assert client.get(route, headers={"X-EdgeOps-Key": "read-test-key"}).status_code == 401
    response = client.get(route, headers={"X-EdgeOps-Key": "admin-test-key"})
    assert response.status_code == 200
    assert len(response.json()) == 5


def test_run_api_status_codes_and_provenance(client: TestClient) -> None:
    admin = {"X-EdgeOps-Key": "admin-test-key"}
    unknown = client.post("/v1/simulation/runs", headers=admin, json={"scenario_id": "unknown"})
    assert unknown.status_code == 422
    started = client.post(
        "/v1/simulation/runs",
        headers=admin,
        json={
            "scenario_id": "ingestion-backpressure",
            "seed": 42,
            "speed": 20,
            "variant_id": "degraded",
        },
    )
    assert started.status_code == 201
    body = started.json()
    assert body["simulation_contract_version"] == 2
    assert body["variant_id"] == "degraded"
    assert "resolved_effects" in body
    assert "workload_scale" in body
    assert "unavailable_metric_groups" in body
    assert (
        client.post(
            "/v1/simulation/runs",
            headers=admin,
            json={"scenario_id": "ingestion-backpressure", "variant_id": "severe"},
        ).status_code
        == 409
    )
    current = client.get("/v1/simulation/runs/current", headers=admin)
    stopped = client.delete("/v1/simulation/runs/current", headers=admin)
    assert current.status_code == 200
    assert stopped.status_code == 200
    assert current.json()["run_id"] == stopped.json()["run_id"]


def test_unknown_variant_returns_422(client: TestClient) -> None:
    response = client.post(
        "/v1/simulation/runs",
        headers={"X-EdgeOps-Key": "admin-test-key"},
        json={"scenario_id": "ingestion-backpressure", "variant_id": "unknown"},
    )
    assert response.status_code == 422
