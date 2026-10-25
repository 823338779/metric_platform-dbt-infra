"""唯一装配入口；应用用例不持有 HTTP 或 Runtime。"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from functools import partial
from typing import Final

from fastapi import FastAPI

from dbt_metricflow_service import constants as strings
from dbt_metricflow_service.settings import Settings

from .. import __version__
from ..application.builds import BuildService
from ..application.catalog import CatalogService
from ..application.deployments import DeploymentService
from ..application.queries import QueryService
from ..bootstrap import Runtime
from ..platform.source import is_ancestor, remote_head
from ..storage.builds import BuildStore
from ..storage.deployments import DeploymentStore
from . import builds, catalog, deployments, queries, system
from .auth import mutation_guard
from .errors import register_exception_handlers
from .limits import RequestBodyLimitMiddleware

POSTGRES_DATABASE_URL_REQUIRED: Final[str] = "SERVICE_DATABASE_URL is required; only PostgreSQL is supported"
ENGINE_PREREQUISITES_UNAVAILABLE: Final[str] = "engine prerequisites are unavailable"
SERVICE_TITLE: Final[str] = strings.SERVICE_TITLE


def create_app(settings: Settings) -> FastAPI:
    if not settings.database_url:
        raise ValueError(POSTGRES_DATABASE_URL_REQUIRED)
    runtime = Runtime(settings)
    build_service = BuildService(BuildStore(runtime.db), runtime.toolchain, settings)
    catalog_service = CatalogService(build_service.store, runtime.artifacts,
                                     runtime.queries.lineage, settings.max_result_bytes)
    deployment_service = DeploymentService(DeploymentStore(runtime.db),
                                           partial(remote_head, temp_root=settings.temp_root),
                                           partial(is_ancestor, temp_root=settings.temp_root),
                                           builds=build_service.store, catalogs=catalog_service)
    query_service = QueryService(build_service, runtime.queries.execute_query, runtime.queries.available_dimensions,
                                 settings.max_result_bytes)

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
    for routes in (builds.router(build_service, guard), deployments.router(deployment_service),
                   catalog.router(catalog_service), queries.router(query_service, guard),
                   system.router(settings, runtime.db)):
        app.include_router(routes)

    return app
