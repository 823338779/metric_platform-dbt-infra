"""无状态模式的 HTTP 接口，所有同步数据库操作离开事件循环执行。"""

import asyncio
import os
import shutil
from contextlib import asynccontextmanager
from importlib.metadata import version
from uuid import UUID

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from psycopg2 import Error as DatabaseError

from dbt_metricflow_service import __version__
from dbt_metricflow_service.models import DbtJobRequest, JobRecord, MetricFlowJobRequest
from dbt_metricflow_service.platform_models import PlatformQueryRequest, PlatformRunRequest
from dbt_metricflow_service.request_limits import RequestBodyLimitMiddleware
from dbt_metricflow_service.runtime import AdapterUnsupported, Runtime, RuntimeUnavailable
from dbt_metricflow_service.storage.jobs import CleanupBlocked, ProjectBusy, StoreConflict

RUNS = "/v1/project-runs"
QUERIES = "/v1/query-jobs"
RETRY_RESPONSE = {503: {"description": "持久存储不可用、资源容量不足或任务尚未完成；可重试"}}
METRIC_PARAMS = Query(default=[])
COMMANDS = ("dbt", "mf")


def create_runtime_app(settings):
    from dbt_metricflow_service.api import VERSION_DISTRIBUTIONS, register_exception_handlers

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
    from dbt_metricflow_service.publication_api import create_publication_router

    app.include_router(create_publication_router(runtime))
    from dbt_metricflow_service.branch_api import create_branch_router

    app.include_router(create_branch_router(runtime))
    from dbt_metricflow_service.branch_events import create_branch_event_router

    app.include_router(create_branch_event_router(runtime))
    from dbt_metricflow_service.draft_validation_api import create_draft_validation_router

    app.include_router(create_draft_validation_router(runtime))

    # 错误响应不输出数据库驱动异常正文或连接凭据。
    async def unavailable(_request, _error):
        return JSONResponse(status_code=503, content={"detail": {"code": "runtime_unavailable"}})

    app.add_exception_handler(DatabaseError, unavailable)
    app.add_exception_handler(RuntimeUnavailable, unavailable)

    @app.exception_handler(ProjectBusy)
    async def busy(_request, _error):
        return JSONResponse(status_code=409, content={"detail": {"code": "project_busy"}})

    @app.exception_handler(StoreConflict)
    async def conflict(_request, _error):
        return JSONResponse(status_code=409, content={"detail": {"code": "idempotency_conflict"}})

    @app.exception_handler(AdapterUnsupported)
    async def unsupported(_request, _error):
        return JSONResponse(status_code=422, content={"detail": {"code": "metricflow_adapter_not_supported"}})

    async def call(function, *args, invalid="invalid_request", missing="not_found"):
        try:
            return await asyncio.to_thread(function, *args)
        except StoreConflict:
            raise
        except KeyError as error:
            raise HTTPException(404, detail={"code": missing}) from error
        except ValueError as error:
            raise HTTPException(422, detail={"code": invalid}) from error

    def found(value, code):
        if value is None:
            raise HTTPException(404, detail={"code": code})
        return value

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

    @app.post("/v1/dbt/jobs", status_code=202)
    async def dbt(payload: DbtJobRequest) -> JobRecord:
        return await runtime.submit_cli(payload)

    @app.post("/v1/metricflow/jobs", status_code=202)
    async def metricflow(payload: MetricFlowJobRequest) -> JobRecord:
        return await runtime.submit_cli(payload)

    @app.get("/v1/jobs/{job_id}")
    async def job(job_id: UUID) -> JobRecord:
        return found(await asyncio.to_thread(runtime.cli_record, str(job_id)), "job_not_found")

    @app.post(RUNS, status_code=202)
    async def submit_run(payload: PlatformRunRequest):
        return await call(runtime.submit_run, payload, invalid="invalid_platform_run")

    @app.get(RUNS + "/by-key/{key}")
    async def run_by_key(key: str):
        return found(await call(runtime.run_by_key, key), "run_not_found")

    @app.get(RUNS + "/{run_id}")
    async def get_run(run_id: UUID):
        return found(await call(runtime.get_run, str(run_id)), "run_not_found")

    @app.get(RUNS + "/{run_id}/catalog")
    async def catalog(run_id: UUID):
        try:
            return await asyncio.to_thread(runtime.catalog, str(run_id))
        except (KeyError, ValueError) as error:
            raise HTTPException(409, detail={"code": "catalog_unavailable"}) from error

    @app.get(RUNS + "/{run_id}/query-options")
    async def options(run_id: UUID, metrics: list[str] = METRIC_PARAMS):
        return await call(runtime.options, str(run_id), tuple(metrics), invalid="query_options_unavailable")

    @app.post(RUNS + "/{run_id}:cleanup")
    async def cleanup(run_id: UUID):
        try:
            await asyncio.to_thread(runtime.cleanup, str(run_id))
        except KeyError as error:
            raise HTTPException(404, detail={"code": "run_not_found"}) from error
        except (CleanupBlocked, ValueError) as error:
            raise HTTPException(409, detail={"code": "run_cleanup_blocked"}) from error
        return {"state": "CLEANED"}

    @app.post(QUERIES, status_code=202)
    async def query(payload: PlatformQueryRequest):
        return await call(runtime.submit_query, payload, invalid="query_unavailable")

    @app.get(QUERIES + "/by-key/{key}")
    async def query_by_key(key: str):
        return found(await call(runtime.query_by_key, key), "query_not_found")

    @app.get(QUERIES + "/{query_id}")
    async def get_query(query_id: UUID):
        return found(await call(runtime.get_query, str(query_id)), "query_not_found")

    return app
