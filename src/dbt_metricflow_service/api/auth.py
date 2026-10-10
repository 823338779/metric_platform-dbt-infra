"""固定提交写入口的部署凭据。"""

from __future__ import annotations

import hmac
from collections.abc import Awaitable, Callable

from fastapi import Header, HTTPException

from dbt_metricflow_service.settings import Settings


def mutation_guard(settings: Settings) -> Callable[..., Awaitable[str]]:
    async def require_service(authorization: str | None = Header(default=None)) -> str:
        token = getattr(settings, "service_token", None)
        if not token:
            raise HTTPException(503, detail={"code": "mutations_disabled"})
        if authorization is None or not hmac.compare_digest(
                authorization.encode(), ("Bearer " + token).encode()):
            raise HTTPException(401, detail={"code": "service_credential_required"})
        # 幂等来源来自部署凭据，不信任客户端自报的业务身份。
        return "service"
    return require_service
