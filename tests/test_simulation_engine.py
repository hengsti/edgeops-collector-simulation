import pytest
from conftest import FakeClock, build_engine

from edgeops_collector_simulation.config import Settings
from edgeops_collector_simulation.errors import ResourceNotFoundError, RunConflictError
from edgeops_collector_simulation.schemas import INGESTION_COUNTER_NAMES, SimulationRunRequest


@pytest.mark.parametrize(
    ("scenario_id", "variant_id"),
    [
        (scenario, variant)
        for scenario in (
            "ingestion-backpressure",
            "influxdb-write-failure",
            "malformed-sensor-payload",
            "missing-device-heartbeat",
        )
        for variant in ("mild", "degraded", "severe")
    ],
)
async def test_all_scenario_variant_combinations_start(
    settings: Settings, scenario_id: str, variant_id: str
) -> None:
    engine = build_engine(settings, FakeClock())
    state = await engine.start_run(
        SimulationRunRequest(scenario_id=scenario_id, seed=7, variant_id=variant_id)
    )
    assert state.variant_id == variant_id
    assert state.simulation_contract_version == 2
    assert 0.65 <= state.workload_scale <= 1.50
    assert len(state.unavailable_metric_groups) <= 3


async def test_implicit_variant_is_sorted_seed_modulo_selection(settings: Settings) -> None:
    for seed, expected in enumerate(("degraded", "mild", "severe")):
        engine = build_engine(settings, FakeClock())
        state = await engine.start_run(
            SimulationRunRequest(scenario_id="ingestion-backpressure", seed=seed)
        )
        assert state.variant_id == expected


async def test_replay_is_identical_and_different_seed_varies(settings: Settings) -> None:
    snapshots = []
    for seed in (23, 23, 24):
        clock = FakeClock()
        engine = build_engine(settings, clock)
        state = await engine.start_run(
            SimulationRunRequest(
                scenario_id="malformed-sensor-payload",
                seed=seed,
                variant_id="degraded",
                speed=2,
            )
        )
        clock.advance(77.5)
        metrics = await engine.get_ingestion_metrics()
        snapshots.append((state, metrics))
    assert snapshots[0] == snapshots[1]
    assert snapshots[0][0].resolved_effects != snapshots[2][0].resolved_effects
    assert snapshots[0][1].data != snapshots[2][1].data


async def test_incident_ramps_and_recovery_keeps_explicit_provenance(settings: Settings) -> None:
    clock = FakeClock()
    engine = build_engine(settings, clock)
    before = await engine.get_ingestion_metrics()
    state = await engine.start_run(
        SimulationRunRequest(scenario_id="ingestion-backpressure", seed=42, variant_id="severe")
    )
    clock.advance(121)
    ramp = await engine.get_ingestion_metrics()
    incident_duration = 180 * float(state.resolved_effects["incident_duration_multiplier"])
    clock.advance(incident_duration + 15)
    recovery_start = await engine.get_ingestion_metrics()
    clock.advance(70)
    recovery = await engine.get_ingestion_metrics()
    assert before.metadata.simulation_phase is None
    assert ramp.metadata.simulation_phase == "overloaded"
    assert recovery_start.metadata.simulation_phase == "recovery"
    assert recovery.metadata.simulation_phase == "recovery"
    assert recovery.metadata.scenario_id == "ingestion-backpressure"
    assert ramp.data.ingest_queue_depth is None or ramp.data.ingest_queue_depth >= 20


async def test_device_read_advances_time_and_recovers_with_consistent_age(
    settings: Settings,
) -> None:
    clock = FakeClock()
    engine = build_engine(settings, clock)
    state = await engine.start_run(
        SimulationRunRequest(scenario_id="missing-device-heartbeat", seed=42, variant_id="severe")
    )
    clock.advance(150)
    incident = await engine.get_device(settings.device_id)
    assert incident.metadata.simulation_phase == "heartbeat-missing"
    assert incident.data.available is False
    assert incident.data.last_seen is not None
    assert incident.data.heartbeat_age_seconds == pytest.approx(
        (incident.metadata.captured_at - incident.data.last_seen).total_seconds()
    )

    incident_duration = 180 * float(state.resolved_effects["incident_duration_multiplier"])
    clock.advance(incident_duration + 15)
    recovery = await engine.get_device(settings.device_id)
    assert recovery.metadata.simulation_phase == "recovery"
    assert recovery.data.heartbeat_age_seconds is not None
    assert recovery.data.heartbeat_age_seconds < incident.data.heartbeat_age_seconds


@pytest.mark.parametrize("speed", [1, 50, 100])
async def test_accelerated_counters_use_simulated_elapsed_seconds(
    settings: Settings, speed: int
) -> None:
    clock = FakeClock()
    engine = build_engine(settings, clock)
    await engine.start_run(
        SimulationRunRequest(scenario_id="ingestion-backpressure", seed=42, speed=speed)
    )
    before = await engine.get_ingestion_metrics()
    clock.advance(5 / speed)
    after = await engine.get_ingestion_metrics()
    elapsed = (after.metadata.captured_at - before.metadata.captured_at).total_seconds()
    assert elapsed == pytest.approx(5)
    rate = (
        after.data.ingest_messages_enqueued_total - before.data.ingest_messages_enqueued_total
    ) / elapsed
    assert 2 <= rate <= 9


async def test_healthy_device_age_advances_between_periodic_readings(settings: Settings) -> None:
    clock = FakeClock()
    engine = build_engine(settings, clock)
    await engine.start_run(
        SimulationRunRequest(scenario_id="missing-device-heartbeat", seed=42, speed=10)
    )
    for advance, expected_age in [(0.05, 0.5), (0.1, 1.5), (0.1, 0.5)]:
        clock.advance(advance)
        device = await engine.get_device(settings.device_id)
        repeated = await engine.get_device(settings.device_id)
        assert device.data.available is True
        assert device.data.heartbeat_age_seconds == pytest.approx(expected_age)
        assert repeated == device
        assert (
            device.metadata.captured_at - device.data.last_seen
        ).total_seconds() == pytest.approx(expected_age)


@pytest.mark.parametrize("finish_run", [False, True])
async def test_timestamps_stay_monotonic_after_accelerated_run(
    settings: Settings, finish_run: bool
) -> None:
    clock = FakeClock()
    engine = build_engine(settings, clock)
    await engine.start_run(
        SimulationRunRequest(scenario_id="ingestion-backpressure", seed=42, speed=100)
    )
    clock.advance(0.5)
    before = await engine.get_ingestion_metrics()
    if finish_run:
        clock.advance(10)
    else:
        await engine.stop_run()
    after = await engine.get_ingestion_metrics()
    clock.advance(1)
    idle = await engine.get_ingestion_metrics()
    assert after.metadata.captured_at >= before.metadata.captured_at
    assert (
        idle.metadata.captured_at - after.metadata.captured_at
    ).total_seconds() == pytest.approx(1)


async def test_counters_are_monotonic_and_queue_is_bounded(settings: Settings) -> None:
    clock = FakeClock()
    engine = build_engine(settings, clock)
    await engine.start_run(
        SimulationRunRequest(scenario_id="ingestion-backpressure", seed=51, variant_id="severe")
    )
    previous = await engine.get_ingestion_metrics()
    for _ in range(8):
        clock.advance(50)
        current = await engine.get_ingestion_metrics()
        for name in INGESTION_COUNTER_NAMES:
            old = getattr(previous.data, name)
            new = getattr(current.data, name)
            if old is not None and new is not None:
                assert new >= old
        if current.data.ingest_queue_depth is not None:
            assert current.data.ingest_queue_capacity is not None
            assert 0 <= current.data.ingest_queue_depth <= current.data.ingest_queue_capacity
        previous = current


async def test_unknown_inputs_precede_overlap_conflict(settings: Settings) -> None:
    engine = build_engine(settings, FakeClock())
    await engine.start_run(
        SimulationRunRequest(scenario_id="ingestion-backpressure", variant_id="mild")
    )
    with pytest.raises(ResourceNotFoundError):
        await engine.start_run(SimulationRunRequest(scenario_id="unknown"))
    with pytest.raises(ResourceNotFoundError):
        await engine.start_run(
            SimulationRunRequest(scenario_id="ingestion-backpressure", variant_id="unknown")
        )
    with pytest.raises(RunConflictError):
        await engine.start_run(
            SimulationRunRequest(scenario_id="ingestion-backpressure", variant_id="severe")
        )


async def test_missingness_is_deterministic_and_required_a1_is_never_masked(
    settings: Settings,
) -> None:
    seen_mask = False
    required = {
        "ingest_messages_enqueued_total",
        "ingest_messages_processed_total",
        "ingest_transform_success_total",
        "ingest_transform_failed_total",
        "influx_lines_written_total",
        "influx_write_failed_total",
        "ingest_pipeline_duration_seconds_sum",
        "ingest_pipeline_duration_seconds_count",
        "influxdb_healthy",
    }
    for seed in range(20):
        first = build_engine(settings, FakeClock())
        second = build_engine(settings, FakeClock())
        request = SimulationRunRequest(
            scenario_id="influxdb-write-failure", seed=seed, variant_id="degraded"
        )
        first_state = await first.start_run(request)
        second_state = await second.start_run(request)
        assert first_state.unavailable_metric_groups == second_state.unavailable_metric_groups
        assert len(first_state.unavailable_metric_groups) <= 3
        metrics = (await first.get_ingestion_metrics()).data.model_dump()
        assert all(metrics[name] is not None for name in required)
        seen_mask |= bool(first_state.unavailable_metric_groups)
    assert seen_mask
