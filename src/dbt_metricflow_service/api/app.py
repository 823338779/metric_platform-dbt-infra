"""唯一装配入口；应用用例不持有 HTTP 或 Runtime。"""

from __future__ import annotations

import asyncio
import os
import shutil
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from functools import partial
from importlib.metadata import version
from typing import Final

from fastapi import FastAPI

from dbt_metricflow_service import constants as strings
from dbt_metricflow_service.settings import Settings

from .. import __version__
from ..application.builds import BuildService
from ..application.catalog import CatalogService
from ..application.deployments import DeploymentService
from ..application.errors import ServiceError
from ..application.queries import QueryService
from ..models.service import HealthView, VersionsView
from ..platform.source import is_ancestor, remote_head
from ..runtime.service import Runtime
from ..storage.builds import BuildStore
from ..storage.deployments import DeploymentStore
from . import builds, catalog, deployments, queries
from .auth import mutation_guard
from .errors import register_exception_handlers
from .limits import RequestBodyLimitMiddleware

POSTGRES_DATABASE_URL_REQUIRED: Final[str] = "SERVICE_DATABASE_URL is required; only PostgreSQL is supported"
ENGINE_PREREQUISITES_UNAVAILABLE: Final[str] = "engine prerequisites are unavailable"
SERVICE_TITLE: Final[str] = strings.SERVICE_TITLE


VERSION_DISTRIBUTIONS = (
    strings.DBT_CORE_PACKAGE,
    strings.DBT_STARROCKS_PACKAGE,
    strings.DBT_DUCKDB_PACKAGE,
    strings.DBT_METRICFLOW_PACKAGE,
    strings.METRICFLOW_PACKAGE,
)
COMMANDS = (strings.DBT_EXECUTABLE, strings.METRICFLOW_EXECUTABLE)


def create_app(settings: Settings) -> FastAPI:
    if not settings.database_url:
        raise ValueError(POSTGRES_DATABASE_URL_REQUIRED)
    runtime = Runtime(settings)
    build_service = BuildService(BuildStore(runtime.db), runtime.toolchain, settings.command_timeout_seconds,
                                 getattr(settings, strings.CONFIG_VERSION_FIELD, None))
    catalog_service = CatalogService(build_service.store, runtime.artifacts)
    deployment_service = DeploymentService(DeploymentStore(runtime.db),
                                           partial(remote_head, temp_root=settings.temp_root),
                                           partial(is_ancestor, temp_root=settings.temp_root),
                                           builds=build_service, catalogs=catalog_service)
    query_service = QueryService(build_service, catalog_service, runtime.execute_query, runtime.available_dimensions)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        await runtime.start(deployments=deployment_service)
        try:
            yield
        finally:
            await runtime.close()

    app = FastAPI(title=SERVICE_TITLE, version=__version__, lifespan=lifespan)
    app.state.runtime = runtime
    app.state.settings = settings
    app.add_middleware(RequestBodyLimitMiddleware)
    register_exception_handlers(app)
    guard = mutation_guard(settings)
    for routes in (builds.router(build_service, guard, settings), deployments.router(deployment_service, guard),
                   catalog.router(catalog_service), queries.router(query_service, guard)):
        app.include_router(routes)

    # 显式保留开放响应契约，避免返回类型注解额外启用响应模型校验。
    @app.get(strings.ROUTE_HEALTH_LIVE, response_model=None)
    async def live() -> HealthView:
        return HealthView(status=strings.HEALTH_OK)

    @app.get(strings.ROUTE_HEALTH_READY, response_model=None)
    async def ready() -> HealthView:
        await asyncio.to_thread(runtime.db.check)
        if (not all(shutil.which(command) for command in COMMANDS)
                or not settings.execution_instances
                or not os.access(settings.temp_root, os.W_OK)):
            raise ServiceError(strings.EXECUTION_ENVIRONMENT_UNAVAILABLE, ENGINE_PREREQUISITES_UNAVAILABLE, 503)
        return HealthView(status=strings.HEALTH_READY)

    @app.get(strings.ROUTE_VERSIONS, response_model=None)
    async def versions() -> VersionsView:
        return VersionsView(
            service=__version__,
            dbt_core=version(strings.DBT_CORE_PACKAGE),
            dbt_starrocks=version(strings.DBT_STARROCKS_PACKAGE),
            dbt_duckdb=version(strings.DBT_DUCKDB_PACKAGE),
            dbt_metricflow=version(strings.DBT_METRICFLOW_PACKAGE),
            metricflow=version(strings.METRICFLOW_PACKAGE),
        )

    return app
