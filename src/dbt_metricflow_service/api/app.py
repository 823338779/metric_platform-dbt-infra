"""唯一装配入口；应用用例不持有 HTTP 或 Runtime。"""

from __future__ import annotations

import asyncio
import os
import shutil
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from functools import partial
from importlib.metadata import version

from fastapi import FastAPI

from dbt_metricflow_service.settings import Settings

from .. import __version__
from ..application.builds import BuildService
from ..application.catalog import CatalogService
from ..application.deployments import DeploymentService
from ..application.errors import ServiceError
from ..application.queries import QueryService
from ..platform.source import is_ancestor, remote_head
from ..runtime.service import Runtime
from ..storage.builds import BuildStore
from ..storage.deployments import DeploymentStore
from . import builds, catalog, deployments, queries
from .auth import mutation_guard
from .errors import register_exception_handlers
from .limits import RequestBodyLimitMiddleware

VERSION_DISTRIBUTIONS = ("dbt-core", "dbt-starrocks", "dbt-duckdb", "dbt-metricflow", "metricflow")
COMMANDS = ("dbt", "mf")


def create_app(settings: Settings) -> FastAPI:
    if not settings.database_url:
        raise ValueError("SERVICE_DATABASE_URL is required; only PostgreSQL is supported")
    runtime = Runtime(settings)
    build_service = BuildService(BuildStore(runtime.db), runtime.toolchain, settings.command_timeout_seconds,
                                 getattr(settings, "config_version", None))
    catalog_service = CatalogService(build_service.store, runtime.artifacts)
    deployment_service = DeploymentService(DeploymentStore(runtime.db),
                                           partial(remote_head, temp_root=settings.temp_root),
                                           partial(is_ancestor, temp_root=settings.temp_root),
                                           builds=build_service, catalogs=catalog_service)
    query_service = QueryService(build_service, catalog_service, runtime.jobs)
    runtime.deployments = deployment_service

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        await runtime.start()
        try:
            yield
        finally:
            await runtime.close()

    app = FastAPI(title="dbt MetricFlow Service", version=__version__, lifespan=lifespan)
    app.state.runtime = runtime
    app.state.settings = settings
    app.add_middleware(RequestBodyLimitMiddleware)
    register_exception_handlers(app)
    guard = mutation_guard(settings)
    for routes in (builds.router(build_service, guard), deployments.router(deployment_service, guard),
                   catalog.router(catalog_service), queries.router(query_service, guard)):
        app.include_router(routes)

    # 显式保留开放响应契约，避免返回类型注解额外启用响应模型校验。
    @app.get("/health/live", response_model=None)
    async def live() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/health/ready", response_model=None)
    async def ready() -> dict[str, str]:
        await asyncio.to_thread(runtime.db.check)
        if (not all(shutil.which(command) for command in COMMANDS)
                or not (settings.profiles_dir / "profiles.yml").is_file()
                or not os.access(settings.temp_root, os.W_OK)):
            raise ServiceError("EXECUTION_ENVIRONMENT_UNAVAILABLE", "engine prerequisites are unavailable", 503)
        return {"status": "ready"}

    @app.get("/v1/versions", response_model=None)
    async def versions() -> dict[str, str]:
        return {"service": __version__, **{package: version(package) for package in VERSION_DISTRIBUTIONS}}

    return app
