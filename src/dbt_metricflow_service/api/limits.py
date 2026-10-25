from __future__ import annotations

import logging
import re
from typing import Final

from fastapi.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from dbt_metricflow_service import constants as strings

from ..models.service import ErrorResponse, ErrorView

REQUEST_BODY_TOO_LARGE: Final[str] = "request body exceeds limit"


logger = logging.getLogger(__name__)
DEFAULT_MAX_REQUEST_BODY_BYTES = 8 * 1024 * 1024
ENGINE_WRITE_PATH = re.compile(
    r"^/(builds|query(?:/available-dimensions)?)$"
)


class RequestBodyLimitMiddleware:
    """Bound request bodies before FastAPI materializes JSON for job routes."""

    def __init__(self, app: ASGIApp, max_bytes: int = DEFAULT_MAX_REQUEST_BODY_BYTES) -> None:
        self._app = app
        self._max_bytes = max_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if (
            scope[strings.ASGI_TYPE_FIELD] != strings.ASGI_HTTP_SCOPE
            or scope.get(strings.ASGI_METHOD_FIELD) != strings.HTTP_POST_METHOD
            or not ENGINE_WRITE_PATH.fullmatch(scope.get(strings.ASGI_PATH_FIELD, ""))
        ):
            await self._app(scope, receive, send)
            return

        content_length = self._content_length(scope)
        if content_length is not None and content_length > self._max_bytes:
            await self._reject(scope, receive, send)
            return

        body = bytearray()
        while True:
            message = await receive()
            if message[strings.ASGI_TYPE_FIELD] == strings.ASGI_HTTP_DISCONNECT:
                return
            if message[strings.ASGI_TYPE_FIELD] != strings.ASGI_HTTP_REQUEST:
                continue
            body.extend(message.get(strings.ASGI_BODY_FIELD, b""))
            if len(body) > self._max_bytes:
                await self._reject(scope, receive, send)
                return
            if not message.get(strings.ASGI_MORE_BODY_FIELD, False):
                break

        replayed = False

        async def replay_receive() -> Message:
            nonlocal replayed
            if not replayed:
                replayed = True
                return {
                    strings.ASGI_TYPE_FIELD: strings.ASGI_HTTP_REQUEST,
                    strings.ASGI_BODY_FIELD: bytes(body),
                    strings.ASGI_MORE_BODY_FIELD: False,
                }
            return await receive()

        await self._app(scope, replay_receive, send)

    @staticmethod
    def _content_length(scope: Scope) -> int | None:
        for name, value in scope.get(strings.ASGI_HEADERS_FIELD, []):
            if name.lower() == b"content-length":
                try:
                    return int(value)
                except ValueError:
                    return None
        return None

    @staticmethod
    async def _reject(scope: Scope, receive: Receive, send: Send) -> None:
        response = JSONResponse(
            status_code=413,
            content=ErrorResponse(error=ErrorView(
                code=strings.REQUEST_TOO_LARGE, message=REQUEST_BODY_TOO_LARGE,
            )).model_dump(mode=strings.JSON_MODE, by_alias=True),
        )
        await response(scope, receive, send)
