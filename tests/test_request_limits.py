from __future__ import annotations

import logging

import pytest
from starlette.types import Message, Receive, Scope, Send

from dbt_metricflow_service.request_limits import RequestBodyLimitMiddleware

logger = logging.getLogger(__name__)


async def _invoke(
    frames: list[Message],
    *,
    max_bytes: int = 8,
    method: str = "POST",
    path: str = "/v1/dbt/jobs",
    headers: list[tuple[bytes, bytes]] | None = None,
) -> tuple[bool, list[Message], list[Message]]:
    called = False
    received: list[Message] = []

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        nonlocal called
        called = True
        received.append(await receive())

    remaining = iter(frames)

    async def receive() -> Message:
        return next(remaining)

    sent: list[Message] = []

    async def send(message: Message) -> None:
        sent.append(message)

    scope: Scope = {
        "type": "http",
        "method": method,
        "path": path,
        "headers": headers or [],
    }
    await RequestBodyLimitMiddleware(app, max_bytes=max_bytes)(scope, receive, send)
    return called, sent, received


async def test_chunked_limit_precedes_application() -> None:
    called, sent, _ = await _invoke(
        [
            {"type": "http.request", "body": b"abcd", "more_body": True},
            {"type": "http.request", "body": b"efghi", "more_body": False},
        ]
    )
    assert called is False
    assert sent[0]["status"] == 413


@pytest.mark.parametrize(
    "headers",
    [[], [(b"content-length", b"1")]],
)
async def test_actual_body_size_is_enforced_and_exact_limit_replayed(
    headers: list[tuple[bytes, bytes]],
) -> None:
    called, _, received = await _invoke(
        [{"type": "http.request", "body": b"abcdefgh", "more_body": False}],
        headers=headers,
    )
    assert called is True
    assert received == [{"type": "http.request", "body": b"abcdefgh", "more_body": False}]


async def test_content_length_can_reject_before_reading_body() -> None:
    called, sent, _ = await _invoke(
        [{"type": "http.request", "body": b"", "more_body": False}],
        headers=[(b"content-length", b"9")],
    )
    assert called is False
    assert sent[0]["status"] == 413


async def test_disconnect_does_not_call_application() -> None:
    called, sent, _ = await _invoke([{"type": "http.disconnect"}])
    assert called is False
    assert sent == []


async def test_unrelated_route_is_not_buffered() -> None:
    called, _, received = await _invoke(
        [{"type": "http.request", "body": b"large body", "more_body": False}],
        method="GET",
        path="/health/live",
    )
    assert called is True
    assert received[0]["body"] == b"large body"
