import random
from pathlib import Path

import pytest
from pydantic import ValidationError

from edgeops_collector_simulation.config import Settings
from edgeops_collector_simulation.simulation.scenario import (
    ScenarioDefinition,
    ScenarioRepository,
    resolve_variant,
)


def test_all_scenarios_use_contract_v2_and_all_variants(settings: Settings) -> None:
    repository = ScenarioRepository.load(
        settings.scenario_path,
        allowed_services=settings.allowed_service_names,
    )
    summaries = repository.list()
    assert len(summaries) == 4
    assert all(item.variants == ["degraded", "mild", "severe"] for item in summaries)
    assert all(item.phases[0] == "normal" and item.phases[-1] == "recovery" for item in summaries)


def test_schema_rejects_unknown_keys_and_unordered_ranges() -> None:
    invalid = {
        "schema_version": 2,
        "id": "invalid",
        "description": "invalid range",
        "phases": [
            {"id": "normal", "duration_seconds": 1},
            {"id": "incident", "duration_seconds": 1},
            {"id": "recovery", "duration_seconds": 1},
        ],
        "variants": {
            name: {
                "effects": {
                    "counter_rate_additions": {
                        "ingest_transform_failed_total": {"min": 2, "max": 1}
                    }
                }
            }
            for name in ("mild", "degraded", "severe")
        },
        "unexpected": True,
    }
    with pytest.raises(ValidationError):
        ScenarioDefinition.model_validate(invalid)


def test_repository_does_not_depend_on_yaml_order(settings: Settings) -> None:
    repository = ScenarioRepository.load(
        Path(settings.scenario_path),
        allowed_services=settings.allowed_service_names,
    )
    assert [item.id for item in repository.list()] == sorted(item.id for item in repository.list())


def test_enriched_signals_remain_stronger_than_legacy_failure_signatures(
    settings: Settings,
) -> None:
    repository = ScenarioRepository.load(
        settings.scenario_path,
        allowed_services=settings.allowed_service_names,
    )
    malformed = resolve_variant(
        repository.get("malformed-sensor-payload").variants["severe"], random.Random(7)
    )
    influx = resolve_variant(
        repository.get("influxdb-write-failure").variants["severe"], random.Random(7)
    )

    malformed_additions = malformed["counter_rate_additions"]
    assert malformed_additions["ingest_transform_failed_total"] <= 0.20
    assert malformed_additions["ingest_incoming_invalid_json_total"] >= 0.50
    influx_additions = influx["counter_rate_additions"]
    assert influx_additions["influx_write_failed_total"] <= 0.35
    assert influx_additions["wal_forwarder_retry_total"] >= 0.50
