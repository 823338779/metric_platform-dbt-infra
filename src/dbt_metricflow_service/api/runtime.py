"""无状态模式的 HTTP 接口，所有同步数据库操作离开事件循环执行。"""

import asyncio
import os
import shutil
from contextlib import asynccontextmanager
from importlib.metadata import version

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from sqlalchemy.exc import DBAPIError as DatabaseError
from sqlalchemy.exc import TimeoutError as PoolTimeout

from dbt_metricflow_service import __version__
from dbt_metricflow_service.api.limits import RequestBodyLimitMiddleware
from dbt_metricflow_service.runtime.service import Runtime, RuntimeUnavailable
from dbt_metricflow_service.storage.jobs import ProjectBusy, StoreConflict

RUNS = "/v1/project-runs"
QUERIES = "/v1/query-jobs"
RETRY_RESPONSE = {503: {"description": "持久存储不可用、资源容量不足或任务尚未完成；可重试"}}
METRIC_PARAMS = Query(default=[])
COMMANDS = ("dbt", "mf")


def create_runtime_app(settings):
    from dbt_metricflow_service.api.app import VERSION_DISTRIBUTIONS, register_exception_handlers

    runtime = Runtime(settings)

    @asynccontextmanager
    async def lifespan(app):
        await runtime.start()
        try:
            yield
        finally:
            await runtime.close()

    app = FastAPI(title="dbt MetricFlow Service", version=__version__, lifespan=lifespan,
                  responses=RETRY_RESPONSE)
    app.add_middleware(RequestBodyLimitMiddleware)
    app.state.runtime = runtime
    app.state.settings = settings
    register_exception_handlers(app)
    # v2 直接公开服务发布目录；不依赖平台导入确认。
    from dbt_metricflow_service.publications.api import create_publication_router

    app.include_router(create_publication_router(runtime))
    from dbt_metricflow_service.validation.api import create_commit_validation_router

    app.include_router(create_commit_validation_router(runtime))

    # 错误响应不输出数据库驱动异常正文或连接凭据。
    async def unavailable(_request, _error):
        return JSONResponse(status_code=503, content={"detail": {"code": "runtime_unavailable"}})

    app.add_exception_handler(DatabaseError, unavailable)
    app.add_exception_handler(PoolTimeout, unavailable)
    app.add_exception_handler(RuntimeUnavailable, unavailable)

    @app.exception_handler(ProjectBusy)
    async def busy(_request, _error):
        return JSONResponse(status_code=409, content={"detail": {"code": "project_busy"}})

    @app.exception_handler(StoreConflict)
    async def conflict(_request, _error):
        return JSONResponse(status_code=409, content={"detail": {"code": "idempotency_conflict"}})

    @app.get("/health/live")
    async def live():
        return {"status": "ok"}

    @app.get("/health/ready")
    async def ready():
        await asyncio.to_thread(runtime.db.check)
        if not all(shutil.which(command) for command in COMMANDS):
            raise RuntimeUnavailable("CLI is unavailable")
        if not (settings.profiles_dir / "profiles.yml").is_file():
            raise RuntimeUnavailable("profiles are unavailable")
        if not os.access(settings.temp_root, os.W_OK):
            raise RuntimeUnavailable("temporary directory is unavailable")
        return {"status": "ready"}

    @app.get("/v1/versions")
    async def versions():
        return {"service": __version__, **{package: version(package) for package in VERSION_DISTRIBUTIONS}}

    return app
