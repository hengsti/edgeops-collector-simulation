from pathlib import Path

import pytest

from edgeops_collector_simulation.config import Settings
from edgeops_collector_simulation.simulation.engine import SimulationEngine
from edgeops_collector_simulation.simulation.profile import load_profile
from edgeops_collector_simulation.simulation.scenario import ScenarioRepository


class FakeClock:
    def __init__(self) -> None:
        self.value = 100.0

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


@pytest.fixture
def settings() -> Settings:
    root = Path(__file__).parent.parent
    return Settings(
        api_key="read-test-key",
        admin_api_key="admin-test-key",
        source_host="simulation-test",
        profile_path=root / "config" / "simulation-profile.json",
        scenario_path=root / "config" / "scenarios",
    )


def build_engine(settings: Settings, clock: FakeClock) -> SimulationEngine:
    return SimulationEngine(
        settings=settings,
        profile=load_profile(
            settings.profile_path,
            allowed_services=settings.allowed_service_names,
        ),
        scenarios=ScenarioRepository.load(
            settings.scenario_path,
            allowed_services=settings.allowed_service_names,
        ),
        clock=clock,
    )
