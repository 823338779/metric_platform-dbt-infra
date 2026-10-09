"""PostgreSQL 应用入口与通用请求错误转换。"""
from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

VERSION_DISTRIBUTIONS = ("dbt-core", "dbt-starrocks", "dbt-duckdb", "dbt-metricflow", "metricflow")


def create_app(settings) -> FastAPI:
    if not settings.database_url:
        raise ValueError("SERVICE_DATABASE_URL is required; only PostgreSQL is supported")
    from .runtime import create_runtime_app
    return create_runtime_app(settings)


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(RequestValidationError)
    async def validation_error(request, error):
        return JSONResponse(status_code=422, content={"detail": {
            "code": "validation_error", "message": "request validation failed"}})
