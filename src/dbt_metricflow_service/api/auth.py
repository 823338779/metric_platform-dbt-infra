"""固定提交写入口的部署凭据。"""

import hmac

from fastapi import Header, HTTPException


def mutation_guard(settings):
    async def require_service(authorization: str | None = Header(default=None)):
        token = getattr(settings, "service_token", None)
        if not token:
            raise HTTPException(503, detail={"code": "mutations_disabled"})
        if authorization is None or not hmac.compare_digest(
                authorization.encode(), ("Bearer " + token).encode()):
            raise HTTPException(401, detail={"code": "service_credential_required"})
    return require_service
