"""固定提交写入口的部署凭据。"""

from __future__ import annotations

import hmac
from collections.abc import Awaitable, Callable

from fastapi import Header, HTTPException

from dbt_metricflow_service import constants as strings
from dbt_metricflow_service.settings import Settings

from ..models.service import CredentialError


def mutation_guard(settings: Settings) -> Callable[..., Awaitable[str]]:
    async def require_service(authorization: str | None = Header(default=None)) -> str:
        token = getattr(settings, strings.SERVICE_TOKEN_FIELD, None)
        if not token:
            raise HTTPException(503, detail=CredentialError(code=strings.AUTH_MUTATIONS_DISABLED).model_dump())
        if authorization is None or not hmac.compare_digest(
                authorization.encode(), (strings.AUTH_BEARER_PREFIX + token).encode()):
            raise HTTPException(401, detail=CredentialError(code=strings.AUTH_CREDENTIAL_REQUIRED).model_dump())
        # 幂等来源来自部署凭据，不信任客户端自报的业务身份。
        return strings.AUTH_SERVICE_IDENTITY
    return require_service
