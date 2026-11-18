"""HTTP transport shared by the stdio tools."""

import httpx
from pydantic import BaseModel, ValidationError

from .models.contracts import JsonObject


class ToolFailure(Exception):
    def __init__(self, error: JsonObject) -> None:
        self.error = error


async def request(http: httpx.AsyncClient, method: str, path: str, response_model: type[BaseModel], *,
                  params: JsonObject | None = None,
                  body: JsonObject | None = None, build_id: str | None = None) -> JsonObject:
    submitting = (method, path) == ("POST", "/builds")
    try:
        response = await http.request(method, path, params=params, json=body)
    except httpx.RequestError as exc:
        unknown = submitting and not isinstance(exc, (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout))
        raise ToolFailure({"code": "BUILD_ACCEPTANCE_UNKNOWN" if unknown else "SERVICE_UNREACHABLE",
                           "message": "Retry with the original inputs and idempotency key." if unknown
                           else "Cannot reach the HTTP service.", "retryable": True, "buildId": build_id}) from exc
    try:
        payload = response.json()
    except ValueError:
        payload = None
    if response.is_success and isinstance(payload, dict):
        try:
            response_model.model_validate(payload)
        except ValidationError:
            pass
        else:
            return payload
    if not response.is_success and isinstance(payload, dict) and isinstance(payload.get("error"), dict):
        error = {k: v for k, v in payload["error"].items() if k in ("code", "message", "phase", "retryable", "buildId")}
        if not error.get("buildId"):
            error["buildId"] = build_id
        token = http.headers.get("Authorization", "").removeprefix("Bearer ")
        if token:
            error = {k: v.replace(token, "[redacted]") if isinstance(v, str) else v for k, v in error.items()}
        raise ToolFailure(error)
    unknown = submitting and (response.is_success or response.status_code >= 500 or response.status_code == 408)
    raise ToolFailure({"code": "BUILD_ACCEPTANCE_UNKNOWN" if unknown else "HTTP_ERROR",
                       "message": f"Unexpected service response (HTTP {response.status_code}).",
                       "buildId": build_id, "retryable": unknown or response.status_code >= 500})
