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

    @routes.get(strings.ROUTE_HEALTH_LIVE, response_model=HealthView, summary="探测服务进程是否存活")
    async def live() -> HealthView:
        """进程能响应 HTTP 请求时返回 `status=ok`。需要检查运行依赖时使用 `/health/ready`。"""
        return HealthView(status="ok")

    @routes.get(strings.ROUTE_HEALTH_READY, response_model=HealthView, summary="检查服务是否就绪")
    async def ready() -> HealthView:
        """检查服务数据库、执行实例配置和临时目录写权限。检查通过时返回 `status=ready`。"""
        await asyncio.to_thread(database.check)
        if not settings.execution_instances or not os.access(settings.temp_root, os.W_OK):
            raise ServiceError(strings.EXECUTION_ENVIRONMENT_UNAVAILABLE, "engine prerequisites are unavailable", 503)
        return HealthView(status="ready")

    @routes.get(strings.ROUTE_VERSIONS, response_model=VersionsView, summary="查看服务与引擎版本")
    async def versions() -> VersionsView:
        """返回服务、dbt-core、StarRocks 和 DuckDB 适配器、dbt-metricflow 及 MetricFlow 的已安装版本。"""
        return VersionsView(
            service=__version__, dbt_core=version(strings.DBT_CORE_PACKAGE),
            dbt_starrocks=version(strings.DBT_STARROCKS_PACKAGE), dbt_duckdb=version(strings.DBT_DUCKDB_PACKAGE),
            dbt_metricflow=version(strings.DBT_METRICFLOW_PACKAGE), metricflow=version(strings.METRICFLOW_PACKAGE),
        )

    return routes
