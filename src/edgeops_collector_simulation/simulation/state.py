from dataclasses import dataclass
from datetime import datetime
from typing import Any

from edgeops_collector_simulation.simulation.scenario import ScenarioDefinition


@dataclass
class ActiveRun:
    run_id: str
    scenario: ScenarioDefinition
    seed: int
    speed: float
    variant_id: str
    started_at: datetime
    started_tick: float
    resolved_effects: dict[str, Any]
    workload_scale: float
    unavailable_metric_groups: tuple[str, ...]
    completed_steps: int = 0
    workload_noise: float = 0.0
