from fastapi.testclient import TestClient

from edgeops_collector_simulation.config import Settings
from edgeops_collector_simulation.main import create_app


def test_read_and_admin_keys_cannot_be_interchanged(settings: Settings) -> None:
    read = {"X-EdgeOps-Key": "read-test-key"}
    admin = {"X-EdgeOps-Key": "admin-test-key"}
    with TestClient(create_app(settings=settings)) as client:
        assert client.get("/v1/meta", headers=read).status_code == 200
        assert client.get("/v1/meta", headers=admin).status_code == 401
        assert client.get("/v1/simulation/scenarios", headers=admin).status_code == 200
        assert client.get("/v1/simulation/scenarios", headers=read).status_code == 401
