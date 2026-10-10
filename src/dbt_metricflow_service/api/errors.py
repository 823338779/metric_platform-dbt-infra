"""统一错误转换，驱动异常和请求秘密不进入响应。"""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy.exc import DBAPIError, TimeoutError
from starlette.exceptions import HTTPException
from starlette.requests import Request

from ..application.errors import ServiceError
from ..models.builds import ErrorView


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(ServiceError)
    async def application_error(request: Request, error: ServiceError) -> JSONResponse:
        return JSONResponse({"error": error.error.model_dump(mode="json", by_alias=True)}, status_code=error.status)

    @app.exception_handler(RequestValidationError)
    async def invalid_request(request: Request, error: RequestValidationError) -> JSONResponse:
        return response(422, "INVALID_REQUEST", "request does not match the engine contract")

    @app.exception_handler(HTTPException)
    async def http_error(request: Request, error: HTTPException) -> JSONResponse:
        return response(
            error.status_code,
            "NOT_FOUND" if error.status_code == 404 else "HTTP_ERROR",
            "requested operation is unavailable",
        )

    async def storage_error(request: Request, error: Exception) -> JSONResponse:
        return response(503, "STORAGE_UNAVAILABLE", "persistent storage is temporarily unavailable", True)

    app.add_exception_handler(DBAPIError, storage_error)
    app.add_exception_handler(TimeoutError, storage_error)

    @app.exception_handler(ValueError)
    async def invalid_value(request: Request, error: ValueError) -> JSONResponse:
        return response(422, "INVALID_REQUEST", "engine input could not be validated")


def response(status: int, code: str, message: str, retryable: bool=False) -> JSONResponse:
    return JSONResponse(
        {"error": ErrorView(code=code, message=message, retryable=retryable).model_dump(mode="json", by_alias=True)},
        status_code=status,
    )
