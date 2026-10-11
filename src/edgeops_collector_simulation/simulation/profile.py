import json
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, model_validator


class SimulatedService(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: str = "running"
    health: str | None = "healthy"
    restart_count: int = Field(default=0, ge=0)


class SimulationProfile(BaseModel):
    model_config = ConfigDict(extra="forbid")

    base_message_rate: float = Field(gt=0, allow_inf_nan=False)
    heartbeat_interval_seconds: float = Field(default=2.0, gt=0, allow_inf_nan=False)
    queue_initial_depth: float = Field(ge=0, allow_inf_nan=False)
    queue_capacity: float = Field(gt=0, allow_inf_nan=False)
    services: dict[str, SimulatedService]

    @model_validator(mode="after")
    def validate_queue(self) -> "SimulationProfile":
        if self.queue_initial_depth > self.queue_capacity:
            raise ValueError("initial queue depth cannot exceed capacity")
        return self


def load_profile(path: Path, *, allowed_services: frozenset[str]) -> SimulationProfile:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ValueError(f"Unable to read simulation profile: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON simulation profile: {path}") from exc

    profile = SimulationProfile.model_validate(raw)
    actual = frozenset(profile.services)
    if actual != allowed_services:
        missing = sorted(allowed_services - actual)
        unknown = sorted(actual - allowed_services)
        raise ValueError(
            f"Simulation profile service mismatch: missing={missing}; unknown={unknown}"
        )
    return profile
