import asyncio
import hashlib
import math
import random
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from edgeops_collector_simulation.config import Settings
from edgeops_collector_simulation.errors import ResourceNotFoundError, RunConflictError
from edgeops_collector_simulation.schemas import (
    INGESTION_COUNTER_NAMES,
    CaptureMetadata,
    CollectorEnvelope,
    DeviceState,
    IngestionMetrics,
    ServiceLogs,
    ServiceState,
    SimulationRunRequest,
    SimulationRunState,
    SimulationScenarioSummary,
)
from edgeops_collector_simulation.simulation.profile import SimulationProfile
from edgeops_collector_simulation.simulation.scenario import ScenarioRepository, resolve_variant
from edgeops_collector_simulation.simulation.state import ActiveRun

SIMULATION_CONTRACT_VERSION = 2
SIMULATION_EPOCH = datetime(2024, 1, 1, tzinfo=UTC)
OPTIONAL_METRIC_GROUPS: dict[str, frozenset[str]] = {
    "decode/validation": frozenset(
        {
            "mqtt_messages_received_total",
            "ingest_event_queue_full_total",
            "ingest_decode_success_total",
            "ingest_incoming_oversized_total",
            "ingest_incoming_non_utf8_total",
            "ingest_incoming_invalid_json_total",
            "ingest_decode_payload_bytes_sum",
            "ingest_decode_payload_bytes_count",
            "ingest_validate_raw_success_total",
            "ingest_validate_raw_ignored_total",
            "ingest_validate_raw_failed_total",
            "ingest_validate_business_success_total",
            "ingest_validate_business_failed_total",
        }
    ),
    "input/WAL pressure": frozenset(
        {
            "ingest_wal_queue_full_total",
            "ingest_queue_closed_total",
            "ingest_durability_ack_failed_total",
            "ingest_queue_depth",
            "ingest_queue_capacity",
        }
    ),
    "DLQ": frozenset(
        {
            "dlq_messages_published_total",
            "dlq_publish_errors_total",
            "ingest_dlq_publish_duration_seconds_sum",
            "ingest_dlq_publish_duration_seconds_count",
        }
    ),
    "WAL/Influx": frozenset(
        {
            "influx_write_success_total",
            "wal_forwarder_committed_total",
            "wal_forwarder_drop_total",
            "wal_forwarder_retry_total",
            "wal_forwarder_commit_retry_total",
            "wal_forwarder_retry_outage_seconds_sum",
            "wal_forwarder_retry_outage_seconds_count",
            "wal_subscription_corrupt_skipped_total",
            "wal_writer_fatal_total",
            "wal_forwarder_retry_outage_active",
        }
    ),
    "stage latency": frozenset(
        {
            name
            for name in INGESTION_COUNTER_NAMES
            if "duration_seconds_" in name and not name.startswith("ingest_pipeline_")
        }
    ),
}

_ZERO_RATE_MARKERS = (
    "failed",
    "errors",
    "drop",
    "retry",
    "full",
    "ignored",
    "oversized",
    "non_utf8",
    "invalid_json",
    "fatal",
    "corrupt",
    "outage_seconds",
)


def stable_seed(scenario_id: str, seed: int, variant_id: str, purpose: str) -> int:
    material = f"{scenario_id}\0{seed}\0{variant_id}\0{purpose}".encode()
    return int.from_bytes(hashlib.sha256(material).digest()[:8], "big")


def _base_rate(name: str, messages_per_second: float) -> float:
    if any(marker in name for marker in _ZERO_RATE_MARKERS):
        return 0.0
    if name.endswith("_duration_seconds_sum"):
        return messages_per_second * 0.001
    if name.endswith("_duration_seconds_count"):
        return messages_per_second
    if name == "ingest_decode_payload_bytes_sum":
        return messages_per_second * 500.0
    if name == "influx_write_success_total":
        return messages_per_second / 10.0
    return messages_per_second


class SimulationEngine:
    def __init__(
        self,
        *,
        settings: Settings,
        profile: SimulationProfile,
        scenarios: ScenarioRepository,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._settings = settings
        self._profile = profile
        self._scenarios = scenarios
        self._clock = clock
        self._clock_origin = clock()
        self._timestamp_tick = self._clock_origin
        self._timestamp_seconds = 0.0
        self._last_idle_tick = self._clock_origin
        self._lock = asyncio.Lock()
        self._active_run: ActiveRun | None = None
        self._workload_rng = random.Random(settings.seed)
        self._nuisance_rng = random.Random(settings.seed)
        self._counters = {
            name: (0.0 if _base_rate(name, profile.base_message_rate) == 0 else 100_000.0)
            for name in INGESTION_COUNTER_NAMES
        }
        self._queue_depth = profile.queue_initial_depth
        self._gauges = {
            "influxdb_healthy": 1.0,
            "wal_forwarder_retry_outage_active": 0.0,
        }

    def _now(self) -> datetime:
        tick = self._clock()
        speed = self._active_run.speed if self._active_run else 1.0
        self._timestamp_seconds += (tick - self._timestamp_tick) * speed
        self._timestamp_tick = tick
        return SIMULATION_EPOCH + timedelta(seconds=self._timestamp_seconds)

    def _phase(self, run: ActiveRun) -> tuple[str, float] | None:
        elapsed = run.completed_steps
        start = 0.0
        for phase in run.scenario.phases:
            duration = phase.duration_seconds
            if phase.id == run.scenario.incident_phase.id:
                duration *= float(run.resolved_effects["incident_duration_multiplier"])
            end = start + duration
            if elapsed < end:
                return phase.id, (elapsed - start) / duration
            start = end
        return None

    def _effect_strength(self, phase: str, progress: float, run: ActiveRun) -> float:
        if phase == run.scenario.incident_phase.id:
            return min(1.0, progress / 0.2)
        if phase == "recovery":
            return max(0.0, 1.0 - progress / 0.6)
        return 0.0

    def _advance_idle(self, now: float) -> None:
        steps = max(0, math.floor(now - self._last_idle_tick))
        for _ in range(steps):
            self._advance_counters(workload=1.0, strength=0.0, effects={}, nuisance=False)
        if steps:
            self._last_idle_tick += steps

    def _advance_counters(
        self,
        *,
        workload: float,
        strength: float,
        effects: dict[str, Any],
        nuisance: bool,
        recovery: bool = False,
    ) -> None:
        multipliers = effects.get("counter_rate_multipliers", {})
        additions = effects.get("counter_rate_additions", {})
        rates: dict[str, float] = {}
        for name in INGESTION_COUNTER_NAMES:
            baseline = _base_rate(name, self._profile.base_message_rate) * workload
            multiplier = 1.0 + (float(multipliers.get(name, 1.0)) - 1.0) * strength
            addition = float(additions.get(name, 0.0)) * strength
            rates[name] = max(0.0, baseline * multiplier + addition)

        if nuisance:
            event = self._nuisance_rng.random()
            if event < 0.012:
                rates["ingest_event_queue_full_total"] += 0.2
            elif event < 0.024:
                rates["ingest_incoming_invalid_json_total"] += 0.15
                rates["ingest_validate_raw_failed_total"] += 0.1
            elif event < 0.034:
                rates["wal_forwarder_retry_total"] += 0.1
            elif event < 0.044:
                rates["ingest_pipeline_duration_seconds_sum"] *= 2.5

        if recovery:
            rates["ingest_messages_processed_total"] *= 1.35
            rates["ingest_sensor_messages_processed_total"] *= 1.35
            rates["ingest_status_messages_processed_total"] *= 1.35

        accepted = rates["ingest_messages_enqueued_total"]
        processed = min(rates["ingest_messages_processed_total"], self._queue_depth + accepted)
        capacity = self._profile.queue_capacity
        next_depth = min(capacity, max(0.0, self._queue_depth + accepted - processed))
        rejected = max(0.0, self._queue_depth + accepted - processed - capacity)
        if rejected:
            accepted -= rejected
            rates["ingest_messages_enqueued_total"] = max(0.0, accepted)
            rates["ingest_event_queue_full_total"] += rejected
        self._queue_depth = next_depth
        for name, rate in rates.items():
            self._counters[name] += max(0.0, rate)

    def _advance_run(self, run: ActiveRun) -> None:
        target = max(0, math.floor((self._clock() - run.started_tick) * run.speed))
        total = math.ceil(
            sum(
                phase.duration_seconds
                * (
                    float(run.resolved_effects["incident_duration_multiplier"])
                    if phase.id == run.scenario.incident_phase.id
                    else 1.0
                )
                for phase in run.scenario.phases
            )
        )
        target = min(target, total)
        while run.completed_steps < target:
            phase_info = self._phase(run)
            if phase_info is None:
                break
            phase, progress = phase_info
            strength = self._effect_strength(phase, progress, run)
            run.workload_noise = 0.85 * run.workload_noise + self._workload_rng.gauss(0.0, 0.03)
            noise_multiplier = min(1.15, max(0.85, 1.0 + run.workload_noise))
            self._advance_counters(
                workload=run.workload_scale * noise_multiplier,
                strength=strength,
                effects=run.resolved_effects,
                nuisance=phase == "normal",
                recovery=phase == "recovery" and progress < 0.6,
            )
            gauge_values = run.resolved_effects.get("gauge_values", {})
            for name, baseline in (
                ("influxdb_healthy", 1.0),
                ("wal_forwarder_retry_outage_active", 0.0),
            ):
                target_value = float(gauge_values.get(name, baseline))
                resolved = baseline + (target_value - baseline) * strength
                self._gauges[name] = min(1.0, max(0.0, resolved))
            run.completed_steps += 1
        if run.completed_steps >= total:
            self._now()
            self._active_run = None
            self._last_idle_tick = self._clock()

    def _metadata(self) -> CaptureMetadata:
        run = self._active_run
        phase_info = self._phase(run) if run else None
        return CaptureMetadata(
            captured_at=self._now(),
            source_host=self._settings.source_host,
            scenario_id=run.scenario.id if run and phase_info else None,
            simulation_run_id=run.run_id if run and phase_info else None,
            simulation_seed=run.seed if run and phase_info else None,
            simulation_phase=phase_info[0] if phase_info else None,
        )

    def _run_state(self, run: ActiveRun, phase: str) -> SimulationRunState:
        return SimulationRunState(
            run_id=run.run_id,
            scenario_id=run.scenario.id,
            seed=run.seed,
            speed=run.speed,
            started_at=run.started_at,
            phase=phase,
            variant_id=run.variant_id,
            simulation_contract_version=SIMULATION_CONTRACT_VERSION,
            resolved_effects=run.resolved_effects,
            workload_scale=run.workload_scale,
            unavailable_metric_groups=list(run.unavailable_metric_groups),
        )

    def _mask_metrics(self, values: dict[str, float | None], run: ActiveRun | None) -> None:
        if run is None:
            return
        for group in run.unavailable_metric_groups:
            for name in OPTIONAL_METRIC_GROUPS[group]:
                values[name] = None

    async def list_scenarios(self) -> list[SimulationScenarioSummary]:
        return self._scenarios.list()

    async def start_run(self, request: SimulationRunRequest) -> SimulationRunState:
        scenario = self._scenarios.get(request.scenario_id)
        if scenario is None:
            raise ResourceNotFoundError(f"Simulation scenario not found: {request.scenario_id}")
        seed = request.seed if request.seed is not None else self._settings.seed
        variants = sorted(scenario.variants)
        variant_id = request.variant_id or variants[seed % len(variants)]
        if variant_id not in scenario.variants:
            raise ResourceNotFoundError(f"Simulation variant not found: {variant_id}")

        async with self._lock:
            if self._active_run is not None:
                self._advance_run(self._active_run)
            if self._active_run is not None:
                raise RunConflictError("A simulation run is already active")
            now = self._clock()
            self._advance_idle(now)
            effect_rng = random.Random(stable_seed(scenario.id, seed, variant_id, "effects"))
            workload_rng = random.Random(
                stable_seed(scenario.id, seed, variant_id, "workload-scale")
            )
            mask_rng = random.Random(stable_seed(scenario.id, seed, variant_id, "optional-groups"))
            selected = [
                group for group in sorted(OPTIONAL_METRIC_GROUPS) if mask_rng.random() < 0.15
            ]
            if len(selected) > 3:
                selected = sorted(selected, key=lambda group: mask_rng.random())[:3]
            identity = f"{scenario.id}:{seed}:{variant_id}:{request.speed}".encode()
            digest = hashlib.sha256(identity).digest()
            run = ActiveRun(
                run_id=str(UUID(bytes=digest[:16])),
                scenario=scenario,
                seed=seed,
                speed=request.speed,
                variant_id=variant_id,
                started_at=self._now(),
                started_tick=now,
                resolved_effects=resolve_variant(scenario.variants[variant_id], effect_rng),
                workload_scale=workload_rng.uniform(0.65, 1.50),
                unavailable_metric_groups=tuple(sorted(selected)),
            )
            self._active_run = run
            self._workload_rng = random.Random(stable_seed(scenario.id, seed, variant_id, "noise"))
            self._nuisance_rng = random.Random(
                stable_seed(scenario.id, seed, variant_id, "nuisance")
            )
            return self._run_state(run, "normal")

    async def get_current_run(self) -> SimulationRunState | None:
        async with self._lock:
            run = self._active_run
            if run is None:
                return None
            self._advance_run(run)
            if self._active_run is None:
                return None
            phase = self._phase(run)
            return self._run_state(run, phase[0]) if phase else None

    async def stop_run(self) -> SimulationRunState | None:
        async with self._lock:
            run = self._active_run
            if run is None:
                return None
            self._advance_run(run)
            if self._active_run is None:
                return None
            phase = self._phase(run)
            state = self._run_state(run, phase[0]) if phase else None
            self._now()
            self._active_run = None
            self._last_idle_tick = self._clock()
            return state

    async def get_ingestion_metrics(self) -> CollectorEnvelope[IngestionMetrics]:
        async with self._lock:
            if self._active_run is not None:
                self._advance_run(self._active_run)
            else:
                self._advance_idle(self._clock())
            run = self._active_run
            values: dict[str, float | None] = dict(self._counters)
            values.update(
                {
                    "ingest_queue_depth": self._queue_depth,
                    "ingest_queue_capacity": self._profile.queue_capacity,
                    **self._gauges,
                }
            )
            self._mask_metrics(values, run)
            return CollectorEnvelope(
                metadata=self._metadata(),
                data=IngestionMetrics.model_validate(values),
            )

    def _service_state(self, service: str) -> ServiceState:
        if service not in self._settings.allowed_service_names:
            raise ResourceNotFoundError(f"Service is not allowlisted: {service}")
        configured = self._profile.services[service]
        health = configured.health
        if service == "influxdb" and self._gauges["influxdb_healthy"] < 0.5:
            health = "unhealthy"
        return ServiceState(
            service=service,
            status=configured.status,
            health=health,
            restart_count=configured.restart_count,
        )

    async def get_services(self) -> CollectorEnvelope[list[ServiceState]]:
        async with self._lock:
            if self._active_run is not None:
                self._advance_run(self._active_run)
            return CollectorEnvelope(
                metadata=self._metadata(),
                data=[
                    self._service_state(name)
                    for name in sorted(self._settings.allowed_service_names)
                ],
            )

    async def get_service(self, service: str) -> CollectorEnvelope[ServiceState]:
        async with self._lock:
            if self._active_run is not None:
                self._advance_run(self._active_run)
            return CollectorEnvelope(metadata=self._metadata(), data=self._service_state(service))

    async def get_service_logs(
        self, service: str, *, tail: int, contains: str | None
    ) -> CollectorEnvelope[ServiceLogs]:
        async with self._lock:
            if self._active_run is not None:
                self._advance_run(self._active_run)
            self._service_state(service)
            lines = [f"{self._now().isoformat()} INFO service={service} source=simulation"]
            run = self._active_run
            phase = self._phase(run) if run else None
            if run and phase and phase[0] == run.scenario.incident_phase.id:
                configured = run.resolved_effects.get("service_logs", {}).get(service, [])
                lines.extend(f"{self._now().isoformat()} {line}" for line in configured)
            if contains:
                lines = [line for line in lines if contains.casefold() in line.casefold()]
            return CollectorEnvelope(
                metadata=self._metadata(),
                data=ServiceLogs(service=service, lines=lines[-tail:]),
            )

    async def get_device(self, device_id: str) -> CollectorEnvelope[DeviceState]:
        async with self._lock:
            if self._active_run is not None:
                self._advance_run(self._active_run)
            if device_id != self._settings.device_id:
                raise ResourceNotFoundError(f"Device not found: {device_id}")
            run = self._active_run
            phase = self._phase(run) if run else None
            device = run.resolved_effects.get("device") if run else None
            phase_name = phase[0] if phase else None
            active_effect = bool(
                run and phase and phase_name in {run.scenario.incident_phase.id, "recovery"}
            )
            strength = self._effect_strength(*phase, run) if run and phase else 0.0
            availability = (
                device.get("availability", "available") if device and active_effect else "available"
            )
            if availability == "intermittent" and run is not None:
                available = (run.completed_steps // 15) % 2 == 0 or strength < 0.5
            elif availability == "offline":
                available = strength < 0.5
            else:
                available = True
            configured_delay = float(device.get("heartbeat_delay_seconds", 0.0)) if device else 0.0
            # Healthy devices publish periodically; zero age on every read leaks the label.
            elapsed = (self._now() - SIMULATION_EPOCH).total_seconds()
            delay = elapsed % self._profile.heartbeat_interval_seconds + configured_delay * strength
            if not available and run is not None:
                incident_start = run.scenario.phases[0].duration_seconds
                delay = max(delay, float(run.completed_steps - incident_start))
            last_seen = self._now() - timedelta(seconds=max(0.0, delay))
            return CollectorEnvelope(
                metadata=self._metadata(),
                data=DeviceState(
                    device_id=device_id,
                    available=available,
                    last_seen=last_seen,
                    heartbeat_age_seconds=max(0.0, delay),
                    rssi_dbm=device.get("rssi_dbm", -51) if device else -51,
                    fw_version="0.0.6-simulated",
                    raw={"source": "simulation"},
                ),
            )

    async def close(self) -> None:
        return None
