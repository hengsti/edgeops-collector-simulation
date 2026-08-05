from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from edgeops_collector_simulation import __version__
from edgeops_collector_simulation.config import Settings, get_settings
from edgeops_collector_simulation.errors import ResourceNotFoundError, RunConflictError
from edgeops_collector_simulation.schemas import (
    CollectorEnvelope,
    CollectorMeta,
    DeviceState,
    IngestionMetrics,
    ServiceLogs,
    ServiceState,
    SimulationRunRequest,
    SimulationRunState,
    SimulationScenarioSummary,
)
from edgeops_collector_simulation.security import build_secret_dependency
from edgeops_collector_simulation.simulation.engine import SimulationEngine
from edgeops_collector_simulation.simulation.profile import load_profile
from edgeops_collector_simulation.simulation.scenario import ScenarioRepository


def create_app(
    *, settings: Settings | None = None, engine: SimulationEngine | None = None
) -> FastAPI:
    resolved_settings = settings or get_settings()
    resolved_engine = engine or SimulationEngine(
        settings=resolved_settings,
        profile=load_profile(
            resolved_settings.profile_path,
            allowed_services=resolved_settings.allowed_service_names,
        ),
        scenarios=ScenarioRepository.load(
            resolved_settings.scenario_path,
            allowed_services=resolved_settings.allowed_service_names,
        ),
    )
    require_read_key = build_secret_dependency(resolved_settings.api_key, "simulation collector")
    require_admin_key = build_secret_dependency(resolved_settings.admin_api_key, "simulation admin")

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        try:
            yield
        finally:
            await resolved_engine.close()

    app = FastAPI(
        title="EdgeOps Collector Simulation",
        version=__version__,
        lifespan=lifespan,
    )

    @app.exception_handler(ResourceNotFoundError)
    async def not_found(_: Request, exc: ResourceNotFoundError) -> JSONResponse:
        return JSONResponse(status_code=404, content={"detail": str(exc)})

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok", "version": __version__}

    @app.get("/v1/meta", dependencies=[Depends(require_read_key)], response_model=CollectorMeta)
    async def meta() -> CollectorMeta:
        return CollectorMeta(
            version=__version__,
            source_host=resolved_settings.source_host,
            allowed_services=sorted(resolved_settings.allowed_service_names),
        )

    @app.get("/v1/metrics/ingestion", dependencies=[Depends(require_read_key)])
    async def ingestion_metrics() -> CollectorEnvelope[IngestionMetrics]:
        return await resolved_engine.get_ingestion_metrics()

    @app.get("/v1/services", dependencies=[Depends(require_read_key)])
    async def services() -> CollectorEnvelope[list[ServiceState]]:
        return await resolved_engine.get_services()

    @app.get("/v1/services/{service}", dependencies=[Depends(require_read_key)])
    async def service_state(service: str) -> CollectorEnvelope[ServiceState]:
        return await resolved_engine.get_service(service)

    @app.get("/v1/services/{service}/logs", dependencies=[Depends(require_read_key)])
    async def service_logs(
        service: str,
        tail: Annotated[int, Query(ge=1, le=500)] = 200,
        contains: Annotated[str | None, Query(max_length=100)] = None,
    ) -> CollectorEnvelope[ServiceLogs]:
        return await resolved_engine.get_service_logs(service, tail=tail, contains=contains)

    @app.get("/v1/devices/{device_id}", dependencies=[Depends(require_read_key)])
    async def device_state(device_id: str) -> CollectorEnvelope[DeviceState]:
        return await resolved_engine.get_device(device_id)

    @app.get(
        "/v1/simulation/scenarios",
        dependencies=[Depends(require_admin_key)],
        response_model=list[SimulationScenarioSummary],
    )
    async def simulation_scenarios() -> list[SimulationScenarioSummary]:
        return await resolved_engine.list_scenarios()

    @app.post(
        "/v1/simulation/runs",
        status_code=201,
        dependencies=[Depends(require_admin_key)],
        response_model=SimulationRunState,
    )
    async def start_run(request: SimulationRunRequest) -> SimulationRunState:
        try:
            return await resolved_engine.start_run(request)
        except ResourceNotFoundError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        except RunConflictError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.get(
        "/v1/simulation/runs/current",
        dependencies=[Depends(require_admin_key)],
        response_model=SimulationRunState,
    )
    async def current_run() -> SimulationRunState:
        state = await resolved_engine.get_current_run()
        if state is None:
            raise ResourceNotFoundError("No simulation run is active")
        return state

    @app.delete(
        "/v1/simulation/runs/current",
        dependencies=[Depends(require_admin_key)],
        response_model=SimulationRunState | None,
    )
    async def stop_run() -> SimulationRunState | None:
        return await resolved_engine.stop_run()

    return app


def run() -> None:
    uvicorn.run(
        "edgeops_collector_simulation.main:create_app",
        host="0.0.0.0",
        port=8095,
        reload=False,
        factory=True,
    )
