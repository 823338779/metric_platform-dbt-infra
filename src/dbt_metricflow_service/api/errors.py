"""统一错误转换，驱动异常和请求秘密不进入响应。"""

from __future__ import annotations

from typing import Final

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy.exc import DBAPIError, TimeoutError
from starlette.exceptions import HTTPException
from starlette.requests import Request

from dbt_metricflow_service import constants as strings

from ..application.errors import ServiceError
from ..models.builds import ErrorView
from ..models.service import ErrorResponse

REQUEST_CONTRACT_MISMATCH: Final[str] = "request does not match the engine contract"
REQUESTED_OPERATION_UNAVAILABLE: Final[str] = "requested operation is unavailable"
PERSISTENT_STORAGE_UNAVAILABLE: Final[str] = "persistent storage is temporarily unavailable"
ENGINE_INPUT_INVALID: Final[str] = "engine input could not be validated"


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(ServiceError)
    async def application_error(request: Request, error: ServiceError) -> JSONResponse:
        return JSONResponse(
            ErrorResponse(error=error.error).model_dump(mode=strings.JSON_MODE, by_alias=True),
            status_code=error.status,
        )

    @app.exception_handler(RequestValidationError)
    async def invalid_request(request: Request, error: RequestValidationError) -> JSONResponse:
        return response(422, strings.INVALID_REQUEST, REQUEST_CONTRACT_MISMATCH)

    @app.exception_handler(HTTPException)
    async def http_error(request: Request, error: HTTPException) -> JSONResponse:
        return response(
            error.status_code,
            strings.NOT_FOUND if error.status_code == 404 else strings.HTTP_ERROR,
            REQUESTED_OPERATION_UNAVAILABLE,
        )

    async def storage_error(request: Request, error: Exception) -> JSONResponse:
        return response(503, strings.STORAGE_UNAVAILABLE, PERSISTENT_STORAGE_UNAVAILABLE, True)

    app.add_exception_handler(DBAPIError, storage_error)
    app.add_exception_handler(TimeoutError, storage_error)

    @app.exception_handler(ValueError)
    async def invalid_value(request: Request, error: ValueError) -> JSONResponse:
        return response(422, strings.INVALID_REQUEST, ENGINE_INPUT_INVALID)


def response(status: int, code: str, message: str, retryable: bool=False) -> JSONResponse:
    return JSONResponse(
        ErrorResponse(error=ErrorView(code=code, message=message, retryable=retryable)).model_dump(
            mode=strings.JSON_MODE, by_alias=True,
        ),
        status_code=status,
    )
