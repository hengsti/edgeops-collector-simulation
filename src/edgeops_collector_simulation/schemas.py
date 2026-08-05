from datetime import UTC, datetime
from typing import Any, Literal, TypeVar

from pydantic import BaseModel, ConfigDict, Field, model_validator

T = TypeVar("T")


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class CaptureMetadata(StrictModel):
    captured_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    collector_mode: Literal["simulation"] = "simulation"
    simulated: Literal[True] = True
    source_host: str
    scenario_id: str | None = None
    simulation_run_id: str | None = None
    simulation_seed: int | None = None
    simulation_phase: str | None = None


class CollectorEnvelope[T](StrictModel):
    metadata: CaptureMetadata
    data: T


class CollectorMeta(StrictModel):
    version: str
    mode: Literal["simulation"] = "simulation"
    source_host: str
    allowed_services: list[str]


class IngestionMetrics(StrictModel):
    mqtt_messages_received_total: float | None = Field(default=None, ge=0)
    ingest_event_queue_full_total: float | None = Field(default=None, ge=0)
    ingest_decode_success_total: float | None = Field(default=None, ge=0)
    ingest_incoming_oversized_total: float | None = Field(default=None, ge=0)
    ingest_incoming_non_utf8_total: float | None = Field(default=None, ge=0)
    ingest_incoming_invalid_json_total: float | None = Field(default=None, ge=0)
    ingest_decode_payload_bytes_sum: float | None = Field(default=None, ge=0)
    ingest_decode_payload_bytes_count: float | None = Field(default=None, ge=0)
    ingest_decode_duration_seconds_sum: float | None = Field(default=None, ge=0)
    ingest_decode_duration_seconds_count: float | None = Field(default=None, ge=0)
    ingest_validate_raw_success_total: float | None = Field(default=None, ge=0)
    ingest_validate_raw_ignored_total: float | None = Field(default=None, ge=0)
    ingest_validate_raw_failed_total: float | None = Field(default=None, ge=0)
    ingest_validate_raw_duration_seconds_sum: float | None = Field(default=None, ge=0)
    ingest_validate_raw_duration_seconds_count: float | None = Field(default=None, ge=0)
    ingest_transform_attempt_total: float | None = Field(default=None, ge=0)
    ingest_transform_success_total: float = Field(ge=0)
    ingest_transform_failed_total: float = Field(ge=0)
    ingest_transform_deserialize_failed_total: float | None = Field(default=None, ge=0)
    ingest_transform_ignored_total: float | None = Field(default=None, ge=0)
    ingest_transform_duration_seconds_sum: float | None = Field(default=None, ge=0)
    ingest_transform_duration_seconds_count: float | None = Field(default=None, ge=0)
    ingest_validate_business_success_total: float | None = Field(default=None, ge=0)
    ingest_validate_business_failed_total: float | None = Field(default=None, ge=0)
    ingest_validate_business_duration_seconds_sum: float | None = Field(default=None, ge=0)
    ingest_validate_business_duration_seconds_count: float | None = Field(default=None, ge=0)
    ingest_cache_updates_total: float | None = Field(default=None, ge=0)
    ingest_messages_enqueued_total: float = Field(ge=0)
    ingest_wal_queue_full_total: float | None = Field(default=None, ge=0)
    ingest_queue_closed_total: float | None = Field(default=None, ge=0)
    ingest_durability_ack_failed_total: float | None = Field(default=None, ge=0)
    ingest_persist_duration_seconds_sum: float | None = Field(default=None, ge=0)
    ingest_persist_duration_seconds_count: float | None = Field(default=None, ge=0)
    dlq_messages_published_total: float | None = Field(default=None, ge=0)
    dlq_publish_errors_total: float | None = Field(default=None, ge=0)
    ingest_dlq_publish_duration_seconds_sum: float | None = Field(default=None, ge=0)
    ingest_dlq_publish_duration_seconds_count: float | None = Field(default=None, ge=0)
    ingest_messages_processed_total: float = Field(ge=0)
    ingest_sensor_messages_processed_total: float | None = Field(default=None, ge=0)
    ingest_status_messages_processed_total: float | None = Field(default=None, ge=0)
    influx_lines_written_total: float = Field(ge=0)
    influx_write_success_total: float | None = Field(default=None, ge=0)
    influx_write_failed_total: float = Field(ge=0)
    influx_write_duration_seconds_sum: float | None = Field(default=None, ge=0)
    influx_write_duration_seconds_count: float | None = Field(default=None, ge=0)
    ingest_pipeline_duration_seconds_sum: float = Field(ge=0)
    ingest_pipeline_duration_seconds_count: float = Field(ge=0)
    wal_forwarder_committed_total: float | None = Field(default=None, ge=0)
    wal_forwarder_drop_total: float | None = Field(default=None, ge=0)
    wal_forwarder_retry_total: float | None = Field(default=None, ge=0)
    wal_forwarder_commit_retry_total: float | None = Field(default=None, ge=0)
    wal_forwarder_retry_outage_seconds_sum: float | None = Field(default=None, ge=0)
    wal_forwarder_retry_outage_seconds_count: float | None = Field(default=None, ge=0)
    wal_subscription_corrupt_skipped_total: float | None = Field(default=None, ge=0)
    wal_writer_fatal_total: float | None = Field(default=None, ge=0)
    ingest_queue_depth: float | None = Field(default=None, ge=0)
    ingest_queue_capacity: float | None = Field(default=None, gt=0)
    wal_forwarder_retry_outage_active: float | None = Field(default=None, ge=0, le=1)
    influxdb_healthy: float = Field(ge=0, le=1)

    @model_validator(mode="after")
    def validate_queue(self) -> "IngestionMetrics":
        if (self.ingest_queue_depth is None) != (self.ingest_queue_capacity is None):
            raise ValueError("queue depth and capacity availability must match")
        if (
            self.ingest_queue_depth is not None
            and self.ingest_queue_capacity is not None
            and self.ingest_queue_depth > self.ingest_queue_capacity
        ):
            raise ValueError("queue depth cannot exceed capacity")
        return self


INGESTION_METRIC_NAMES = frozenset(IngestionMetrics.model_fields)
INGESTION_COUNTER_NAMES = frozenset(
    name for name in INGESTION_METRIC_NAMES if name.endswith(("_total", "_sum", "_count"))
)


class ServiceState(StrictModel):
    service: str
    container_id: None = None
    status: str
    health: str | None = None
    restart_count: int = Field(ge=0)


class ServiceLogs(StrictModel):
    service: str
    lines: list[str]


class DeviceState(StrictModel):
    device_id: str
    available: bool
    last_seen: datetime | None = None
    heartbeat_age_seconds: float | None = Field(default=None, ge=0)
    rssi_dbm: int | None = None
    fw_version: str | None = None
    raw: dict[str, object] = Field(default_factory=dict)


class SimulationScenarioSummary(StrictModel):
    id: str
    description: str
    phases: list[str]
    variants: list[str]


class SimulationRunRequest(StrictModel):
    scenario_id: str
    seed: int | None = None
    speed: float = Field(default=1.0, gt=0, le=100)
    variant_id: str | None = None


class SimulationRunState(StrictModel):
    run_id: str
    scenario_id: str
    seed: int
    speed: float
    started_at: datetime
    phase: str
    variant_id: str
    simulation_contract_version: Literal[2] = 2
    resolved_effects: dict[str, Any]
    workload_scale: float = Field(ge=0.65, le=1.5)
    unavailable_metric_groups: list[str]
