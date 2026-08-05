import random
from pathlib import Path
from typing import Annotated, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

from edgeops_collector_simulation.schemas import (
    INGESTION_COUNTER_NAMES,
    SimulationScenarioSummary,
)

FiniteNonNegative = Annotated[float, Field(ge=0, allow_inf_nan=False)]


class NumericRange(BaseModel):
    model_config = ConfigDict(extra="forbid")

    min: FiniteNonNegative
    max: FiniteNonNegative

    @model_validator(mode="after")
    def validate_order(self) -> "NumericRange":
        if self.min > self.max:
            raise ValueError("numeric effect range minimum cannot exceed maximum")
        return self


NumericEffect = FiniteNonNegative | NumericRange
SCENARIO_GAUGE_NAMES = frozenset({"influxdb_healthy", "wal_forwarder_retry_outage_active"})


class DeviceEffect(BaseModel):
    model_config = ConfigDict(extra="forbid")

    availability: Literal["available", "delayed", "intermittent", "offline"] = "available"
    heartbeat_delay_seconds: NumericEffect = 0.0
    rssi_dbm: int | None = None


class ScenarioEffects(BaseModel):
    model_config = ConfigDict(extra="forbid")

    counter_rate_multipliers: dict[str, NumericEffect] = Field(default_factory=dict)
    counter_rate_additions: dict[str, NumericEffect] = Field(default_factory=dict)
    gauge_values: dict[str, NumericEffect] = Field(default_factory=dict)
    device: DeviceEffect | None = None
    service_logs: dict[str, list[str]] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_names(self) -> "ScenarioEffects":
        counter_names = set(self.counter_rate_multipliers) | set(self.counter_rate_additions)
        unknown_counters = sorted(counter_names - INGESTION_COUNTER_NAMES)
        unknown_gauges = sorted(set(self.gauge_values) - SCENARIO_GAUGE_NAMES)
        if unknown_counters:
            raise ValueError(f"unknown scenario counter metrics: {unknown_counters}")
        if unknown_gauges:
            raise ValueError(f"unknown scenario gauge metrics: {unknown_gauges}")
        for gauge in ("influxdb_healthy", "wal_forwarder_retry_outage_active"):
            value = self.gauge_values.get(gauge)
            bounds = (value.min, value.max) if isinstance(value, NumericRange) else (value, value)
            if value is not None and bounds[1] is not None and bounds[1] > 1:
                raise ValueError(f"{gauge} must remain between zero and one")
        return self


class ScenarioVariant(BaseModel):
    model_config = ConfigDict(extra="forbid")

    effects: ScenarioEffects
    incident_duration_multiplier: NumericEffect = 1.0


class ScenarioPhase(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(min_length=1, pattern=r"^[a-z0-9][a-z0-9-]*$")
    duration_seconds: float = Field(gt=0, allow_inf_nan=False)


class ScenarioDefinition(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[2]
    id: str = Field(min_length=1, pattern=r"^[a-z0-9][a-z0-9-]*$")
    description: str = Field(min_length=1)
    phases: list[ScenarioPhase]
    variants: dict[str, ScenarioVariant]

    @model_validator(mode="after")
    def validate_contract(self) -> "ScenarioDefinition":
        phase_ids = [phase.id for phase in self.phases]
        if len(phase_ids) != 3 or phase_ids[0] != "normal" or phase_ids[-1] != "recovery":
            raise ValueError("scenario phases must be normal, one incident phase, and recovery")
        if len(set(phase_ids)) != 3:
            raise ValueError("scenario phase IDs must be unique")
        if sorted(self.variants) != ["degraded", "mild", "severe"]:
            raise ValueError("scenario variants must be exactly degraded, mild, and severe")
        return self

    @property
    def incident_phase(self) -> ScenarioPhase:
        return self.phases[1]

    def summary(self) -> SimulationScenarioSummary:
        return SimulationScenarioSummary(
            id=self.id,
            description=self.description,
            phases=[phase.id for phase in self.phases],
            variants=sorted(self.variants),
        )


def resolve_numeric(value: NumericEffect, rng: random.Random) -> float:
    if isinstance(value, NumericRange):
        return rng.uniform(value.min, value.max)
    return float(value)


def resolve_effects(effects: ScenarioEffects, rng: random.Random) -> dict[str, object]:
    resolved: dict[str, object] = {
        "counter_rate_multipliers": {
            name: resolve_numeric(value, rng)
            for name, value in sorted(effects.counter_rate_multipliers.items())
        },
        "counter_rate_additions": {
            name: resolve_numeric(value, rng)
            for name, value in sorted(effects.counter_rate_additions.items())
        },
        "gauge_values": {
            name: resolve_numeric(value, rng)
            for name, value in sorted(effects.gauge_values.items())
        },
        "service_logs": {name: list(lines) for name, lines in sorted(effects.service_logs.items())},
    }
    if effects.device is not None:
        resolved["device"] = {
            "availability": effects.device.availability,
            "heartbeat_delay_seconds": resolve_numeric(effects.device.heartbeat_delay_seconds, rng),
            "rssi_dbm": effects.device.rssi_dbm,
        }
    return resolved


def resolve_variant(variant: ScenarioVariant, rng: random.Random) -> dict[str, object]:
    resolved = resolve_effects(variant.effects, rng)
    resolved["incident_duration_multiplier"] = resolve_numeric(
        variant.incident_duration_multiplier, rng
    )
    return resolved


class ScenarioRepository:
    def __init__(self, scenarios: dict[str, ScenarioDefinition]) -> None:
        self._scenarios = scenarios

    @classmethod
    def load(cls, directory: Path, *, allowed_services: frozenset[str]) -> "ScenarioRepository":
        if not directory.is_dir():
            raise ValueError(f"Unable to read simulation scenario directory: {directory}")
        scenarios: dict[str, ScenarioDefinition] = {}
        paths = sorted((*directory.glob("*.yaml"), *directory.glob("*.yml")))
        if not paths:
            raise ValueError(f"No simulation scenarios found in: {directory}")
        for path in paths:
            try:
                raw = yaml.safe_load(path.read_text(encoding="utf-8"))
            except (OSError, yaml.YAMLError) as exc:
                raise ValueError(f"Unable to load simulation scenario: {path}") from exc
            scenario = ScenarioDefinition.model_validate(raw)
            if scenario.id in scenarios:
                raise ValueError(f"Duplicate simulation scenario ID: {scenario.id}")
            referenced = {
                service
                for variant in scenario.variants.values()
                for service in variant.effects.service_logs
            }
            unknown = sorted(referenced - allowed_services)
            if unknown:
                raise ValueError(f"Scenario {scenario.id} references unknown services: {unknown}")
            scenarios[scenario.id] = scenario
        return cls(scenarios)

    def list(self) -> list[SimulationScenarioSummary]:
        return [self._scenarios[key].summary() for key in sorted(self._scenarios)]

    def get(self, scenario_id: str) -> ScenarioDefinition | None:
        return self._scenarios.get(scenario_id)
