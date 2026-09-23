from __future__ import annotations

import logging
import os
import shutil
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from importlib.metadata import version
from uuid import UUID

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from dbt_metricflow_service import __version__
from dbt_metricflow_service.adapter_support import (
    METRICFLOW_PACKAGE_VERSION,
    METRICFLOW_SUPPORTED_ADAPTERS,
)
from dbt_metricflow_service.commands import (
    DBT_EXECUTABLE,
    METRICFLOW_EXECUTABLE,
    build_dbt_command,
    build_metricflow_command,
)
from dbt_metricflow_service.jobs import JobRunner, ProjectBusyError
from dbt_metricflow_service.models import DbtJobRequest, JobRecord, MetricFlowJobRequest
from dbt_metricflow_service.projects import (
    InvalidManifestError,
    InvalidProjectError,
    ManifestNotFoundError,
    ProjectNotFoundError,
    ProjectRegistry,
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
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        yield
        close = getattr(app.state.runner, "close", None)
        if close is not None:
            await close()

    app = FastAPI(title=SERVICE_TITLE, version=__version__, lifespan=lifespan)
    app.state.settings = settings
    app.state.registry = registry
    app.state.runner = runner
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
            for path in (settings.projects_root, settings.profiles_dir)
        )
        if not commands_available or not mounts_readable:
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
