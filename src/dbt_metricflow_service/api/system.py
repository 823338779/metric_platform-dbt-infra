"""健康探测和版本信息；就绪只检查实际使用的运行依赖。"""

import asyncio
import os
from importlib.metadata import version

from fastapi import APIRouter

from .. import __version__
from .. import constants as strings
from ..application.errors import ServiceError
from ..models.service import HealthView, VersionsView
from ..settings import Settings
from ..storage.postgres import Database


def router(settings: Settings, database: Database) -> APIRouter:
    routes = APIRouter()

    @routes.get(strings.ROUTE_HEALTH_LIVE, response_model=HealthView)
    async def live() -> HealthView:
        return HealthView(status="ok")

    @routes.get(strings.ROUTE_HEALTH_READY, response_model=HealthView)
    async def ready() -> HealthView:
        await asyncio.to_thread(database.check)
        if not settings.execution_instances or not os.access(settings.temp_root, os.W_OK):
            raise ServiceError(strings.EXECUTION_ENVIRONMENT_UNAVAILABLE, "engine prerequisites are unavailable", 503)
        return HealthView(status="ready")

    @routes.get(strings.ROUTE_VERSIONS, response_model=VersionsView)
    async def versions() -> VersionsView:
        return VersionsView(
            service=__version__, dbt_core=version(strings.DBT_CORE_PACKAGE),
            dbt_starrocks=version(strings.DBT_STARROCKS_PACKAGE), dbt_duckdb=version(strings.DBT_DUCKDB_PACKAGE),
            dbt_metricflow=version(strings.DBT_METRICFLOW_PACKAGE), metricflow=version(strings.METRICFLOW_PACKAGE),
        )

    return routes
