from __future__ import annotations

import asyncio
import logging
import os
import shutil
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from importlib.metadata import version
from uuid import UUID

from fastapi import FastAPI, HTTPException, Query, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from dbt_metricflow_service import __version__
from dbt_metricflow_service.adapters.support import (
    METRICFLOW_PACKAGE_VERSION,
    METRICFLOW_SUPPORTED_ADAPTERS,
)
from dbt_metricflow_service.api.limits import RequestBodyLimitMiddleware
from dbt_metricflow_service.execution.commands import (
    DBT_EXECUTABLE,
    METRICFLOW_EXECUTABLE,
)
from dbt_metricflow_service.execution.models import DbtJobRequest, JobRecord, MetricFlowJobRequest
from dbt_metricflow_service.execution.runner import JobRunner, ProjectBusyError
from dbt_metricflow_service.platform.bindings import load_bindings
from dbt_metricflow_service.platform.models import PlatformQueryRequest, PlatformRunRequest
from dbt_metricflow_service.platform.queries import PlatformQueryCoordinator
from dbt_metricflow_service.platform.runs import PlatformRunCoordinator
from dbt_metricflow_service.platform.store import PlatformJobStore
from dbt_metricflow_service.projects import (
    InvalidManifestError,
    InvalidProjectError,
    ManifestNotFoundError,
    ProjectNotFoundError,
    ProjectRegistry,
)
from dbt_metricflow_service.resources.commands import (
    build_dbt_command,
    build_metricflow_command,
)
from dbt_metricflow_service.settings import Settings

logger = logging.getLogger(__name__)

SERVICE_TITLE = "dbt MetricFlow Service"
LIVE_PATH = "/health/live"
READY_PATH = "/health/ready"
VERSIONS_PATH = "/v1/versions"
DBT_JOBS_PATH = "/v1/dbt/jobs"
METRICFLOW_JOBS_PATH = "/v1/metricflow/jobs"
JOB_PATH = "/v1/jobs/{job_id}"
PLATFORM_RUNS_PATH = "/v1/project-runs"
PLATFORM_QUERIES_PATH = "/v1/query-jobs"
METRICS_QUERY = Query(default=[])
VERSION_DISTRIBUTIONS = (
    "dbt-core",
    "dbt-starrocks",
    "dbt-duckdb",
    "dbt-metricflow",
    "metricflow",
)
def _error_detail(code: str, message: str) -> dict[str, dict[str, str]]:
    """Build the stable error envelope shared by handlers and routes."""
    return {"detail": {"code": code, "message": message}}


def create_app(settings: Settings, registry: ProjectRegistry, runner: JobRunner) -> FastAPI:
    """Create one dependency-injected service application."""
    # PostgreSQL 模式使用共享任务和产物，不初始化本地 SQLite 或内存公开队列。
    if settings.database_url:
        from dbt_metricflow_service.api.runtime import create_runtime_app
        return create_runtime_app(settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        prepare_artifacts = getattr(app.state.runner, "prepare_artifacts", None)
        if prepare_artifacts is not None:
            prepare_artifacts()
        yield
        close = getattr(app.state.runner, "close", None)
        if close is not None:
            await close()

    app = FastAPI(title=SERVICE_TITLE, version=__version__, lifespan=lifespan)
    app.add_middleware(RequestBodyLimitMiddleware)
    app.state.settings = settings
    app.state.registry = registry
    app.state.runner = runner
    if settings.platform_bindings_file is not None:
        platform_store = PlatformJobStore(settings.platform_db_path)
        app.state.platform_runs = PlatformRunCoordinator(
            platform_store, load_bindings(settings.platform_bindings_file),
            settings.job_artifacts_root / "platform-runs", settings.profiles_dir,
        )
        app.state.platform_queries = PlatformQueryCoordinator(
            platform_store, app.state.platform_runs,
            settings.job_artifacts_root / "platform-queries", settings.profiles_dir,
        )
    else:
        app.state.platform_runs = None
        app.state.platform_queries = None
    register_routes(app)
    register_exception_handlers(app)
    return app


def register_routes(app: FastAPI) -> None:
    """Register health, inspection, submission, and polling routes."""

    @app.get(LIVE_PATH)
    async def live() -> dict[str, str]:
        return {"status": "ok"}

    @app.get(READY_PATH)
    async def ready(request: Request) -> dict[str, str]:
        settings: Settings = request.app.state.settings
        commands_available = all(
            shutil.which(executable) is not None
            for executable in (DBT_EXECUTABLE, METRICFLOW_EXECUTABLE)
        )
        mounts_readable = all(
            path.is_dir() and os.access(path, os.R_OK)
            for path in (
                settings.projects_root,
                settings.profiles_dir,
                settings.job_artifacts_root,
            )
        )
        artifacts_writable = os.access(settings.job_artifacts_root, os.W_OK)
        if not commands_available or not mounts_readable or not artifacts_writable:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail={"code": "not_ready", "message": "required CLI or mount is unavailable"},
            )
        return {"status": "ready"}

    @app.get(VERSIONS_PATH)
    async def versions() -> dict[str, str]:
        return {
            "service": __version__,
            **{distribution: version(distribution) for distribution in VERSION_DISTRIBUTIONS},
        }

    @app.post(DBT_JOBS_PATH, status_code=status.HTTP_202_ACCEPTED)
    async def submit_dbt_job(payload: DbtJobRequest, request: Request) -> JobRecord:
        settings: Settings = request.app.state.settings
        registry: ProjectRegistry = request.app.state.registry
        runner: JobRunner = request.app.state.runner
        project_dir = registry.resolve(payload.project)
        command = build_dbt_command(payload, project_dir, settings.profiles_dir)
        return await runner.submit(payload.project, command)

    @app.post(METRICFLOW_JOBS_PATH, status_code=status.HTTP_202_ACCEPTED)
    async def submit_metricflow_job(
        payload: MetricFlowJobRequest, request: Request
    ) -> JobRecord:
        settings: Settings = request.app.state.settings
        registry: ProjectRegistry = request.app.state.registry
        runner: JobRunner = request.app.state.runner
        project_dir = registry.resolve(payload.project)
        if not payload.resources:
            adapter_type = registry.adapter_type(project_dir)
            if adapter_type not in METRICFLOW_SUPPORTED_ADAPTERS:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                    detail={
                        "code": "metricflow_adapter_not_supported",
                        "message": (
                            f"adapter '{adapter_type}' is not supported by "
                            f"MetricFlow {METRICFLOW_PACKAGE_VERSION}"
                        ),
                    },
                )
        command = build_metricflow_command(payload, project_dir, settings.profiles_dir)
        return await runner.submit(payload.project, command)

    @app.get(JOB_PATH)
    async def get_job(job_id: UUID, request: Request) -> JobRecord:
        runner: JobRunner = request.app.state.runner
        record = await runner.get(job_id)
        if record is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={"code": "job_not_found", "message": "job is not retained"},
            )
        return record

    @app.post(PLATFORM_RUNS_PATH, status_code=status.HTTP_202_ACCEPTED)
    async def submit_platform_run(payload: PlatformRunRequest, request: Request) -> dict[str, str]:
        coordinator: PlatformRunCoordinator | None = request.app.state.platform_runs
        if coordinator is None:
            raise HTTPException(status_code=503, detail={"code": "platform_unconfigured"})
        try:
            return coordinator.submit(payload)
        except ValueError as error:
            raise HTTPException(status_code=422, detail={"code": "invalid_platform_run"}) from error

    @app.get(PLATFORM_RUNS_PATH + "/by-key/{key}")
    async def get_platform_run_by_key(key: str, request: Request) -> dict[str, object]:
        coordinator: PlatformRunCoordinator | None = request.app.state.platform_runs
        if coordinator is None:
            raise HTTPException(status_code=503, detail={"code": "platform_unconfigured"})
        result = coordinator.get_by_key(key)
        if result is None:
            raise HTTPException(status_code=404, detail={"code": "run_not_found"})
        return result

    @app.get(PLATFORM_RUNS_PATH + "/{run_id}")
    async def get_platform_run(run_id: UUID, request: Request) -> dict[str, object]:
        coordinator: PlatformRunCoordinator | None = request.app.state.platform_runs
        if coordinator is None:
            raise HTTPException(status_code=503, detail={"code": "platform_unconfigured"})
        result = coordinator.get(run_id)
        if result is None:
            raise HTTPException(status_code=404, detail={"code": "run_not_found"})
        return result

    @app.get(PLATFORM_RUNS_PATH + "/{run_id}/catalog")
    async def get_platform_catalog(run_id: UUID, request: Request) -> dict[str, object]:
        coordinator: PlatformRunCoordinator | None = request.app.state.platform_runs
        if coordinator is None:
            raise HTTPException(status_code=503, detail={"code": "platform_unconfigured"})
        try:
            return coordinator.catalog(run_id)
        except ValueError as error:
            raise HTTPException(status_code=409, detail={"code": "catalog_unavailable"}) from error

    @app.post(PLATFORM_RUNS_PATH + "/{run_id}:cleanup")
    async def cleanup_platform_run(run_id: UUID, request: Request) -> dict[str, str]:
        coordinator: PlatformRunCoordinator | None = request.app.state.platform_runs
        if coordinator is None:
            raise HTTPException(status_code=503, detail={"code": "platform_unconfigured"})
        try:
            await asyncio.to_thread(coordinator.cleanup_run, run_id)
        except KeyError as error:
            raise HTTPException(status_code=404, detail={"code": "run_not_found"}) from error
        except ValueError as error:
            raise HTTPException(status_code=409, detail={"code": "run_cleanup_blocked"}) from error
        return {"state": "CLEANED"}

    @app.get(PLATFORM_RUNS_PATH + "/{run_id}/query-options")
    async def get_platform_query_options(
        run_id: UUID, request: Request, metrics: list[str] = METRICS_QUERY
    ) -> dict[str, object]:
        coordinator: PlatformQueryCoordinator | None = request.app.state.platform_queries
        if coordinator is None:
            raise HTTPException(status_code=503, detail={"code": "platform_unconfigured"})
        try:
            return await asyncio.to_thread(coordinator.options, run_id, tuple(metrics))
        except ValueError as error:
            raise HTTPException(status_code=422, detail={"code": "query_options_unavailable"}) from error

    @app.post(PLATFORM_QUERIES_PATH, status_code=status.HTTP_202_ACCEPTED)
    async def submit_platform_query(payload: PlatformQueryRequest, request: Request) -> dict[str, str]:
        coordinator: PlatformQueryCoordinator | None = request.app.state.platform_queries
        if coordinator is None:
            raise HTTPException(status_code=503, detail={"code": "platform_unconfigured"})
        try:
            return coordinator.submit(payload)
        except ValueError as error:
            raise HTTPException(status_code=422, detail={"code": "query_unavailable"}) from error

    @app.get(PLATFORM_QUERIES_PATH + "/by-key/{key}")
    async def get_platform_query_by_key(key: str, request: Request) -> dict[str, object]:
        coordinator: PlatformQueryCoordinator | None = request.app.state.platform_queries
        if coordinator is None:
            raise HTTPException(status_code=503, detail={"code": "platform_unconfigured"})
        result = coordinator.get_by_key(key)
        if result is None:
            raise HTTPException(status_code=404, detail={"code": "query_not_found"})
        return result

    @app.get(PLATFORM_QUERIES_PATH + "/{query_id}")
    async def get_platform_query(query_id: UUID, request: Request) -> dict[str, object]:
        coordinator: PlatformQueryCoordinator | None = request.app.state.platform_queries
        if coordinator is None:
            raise HTTPException(status_code=503, detail={"code": "platform_unconfigured"})
        result = coordinator.get(query_id)
        if result is None:
            raise HTTPException(status_code=404, detail={"code": "query_not_found"})
        return result


def register_exception_handlers(app: FastAPI) -> None:
    """Map boundary failures to stable status codes and error envelopes."""

    @app.exception_handler(RequestValidationError)
    async def validation_error(
        request: Request, error: RequestValidationError
    ) -> JSONResponse:
        del request, error
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            content=_error_detail("validation_error", "request validation failed"),
        )

    @app.exception_handler(InvalidProjectError)
    async def invalid_project(request: Request, error: InvalidProjectError) -> JSONResponse:
        del request, error
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            content=_error_detail("invalid_project", "project identifier is invalid"),
        )

    @app.exception_handler(ProjectNotFoundError)
    async def project_not_found(
        request: Request, error: ProjectNotFoundError
    ) -> JSONResponse:
        del request, error
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            content=_error_detail("project_not_found", "dbt project was not found"),
        )

    @app.exception_handler(ProjectBusyError)
    async def project_busy(request: Request, error: ProjectBusyError) -> JSONResponse:
        del request, error
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content=_error_detail("project_busy", "a write job is already active for this project"),
        )

    @app.exception_handler(ManifestNotFoundError)
    async def manifest_not_found(
        request: Request, error: ManifestNotFoundError
    ) -> JSONResponse:
        del request, error
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            content=_error_detail(
                "semantic_manifest_not_generated",
                "run dbt parse before submitting a MetricFlow job",
            ),
        )

    @app.exception_handler(InvalidManifestError)
    async def invalid_manifest(request: Request, error: InvalidManifestError) -> JSONResponse:
        del request, error
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            content=_error_detail("invalid_dbt_manifest", "dbt manifest metadata is invalid"),
        )
