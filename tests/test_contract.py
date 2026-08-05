from conftest import FakeClock, build_engine

from edgeops_collector_simulation.config import Settings
from edgeops_collector_simulation.schemas import INGESTION_METRIC_NAMES


async def test_read_contract_emits_every_ingestion_key(settings: Settings) -> None:
    metrics = await build_engine(settings, FakeClock()).get_ingestion_metrics()
    assert set(metrics.data.model_dump()) == INGESTION_METRIC_NAMES
    assert metrics.metadata.collector_mode == "simulation"
    assert metrics.metadata.simulated is True


async def test_required_a1_contract_fields_are_populated(settings: Settings) -> None:
    metrics = await build_engine(settings, FakeClock()).get_ingestion_metrics()
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
    values = metrics.data.model_dump()
    assert all(values[name] is not None for name in required)
