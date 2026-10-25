"""固定提交写入口的部署凭据。"""

from __future__ import annotations

import hmac
from collections.abc import Awaitable, Callable

from fastapi import Header

from dbt_metricflow_service import constants as strings
from dbt_metricflow_service.settings import Settings

from ..application.errors import ServiceError


def mutation_guard(settings: Settings) -> Callable[..., Awaitable[None]]:
    async def require_service(authorization: str | None = Header(default=None)) -> None:
        token = settings.service_token
        if not token:
            raise ServiceError(strings.AUTH_MUTATIONS_DISABLED, "service credentials are not configured", 503)
        if authorization is None or not hmac.compare_digest(
                authorization.encode(), (strings.AUTH_BEARER_PREFIX + token).encode()):
            raise ServiceError(strings.AUTH_CREDENTIAL_REQUIRED, "service credentials are required", 401)
    return require_service
