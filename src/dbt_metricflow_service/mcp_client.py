"""HTTP transport shared by the stdio tools."""

import httpx

from .models.contracts import JsonObject


class ToolFailure(Exception):
    def __init__(self, error: JsonObject) -> None:
        self.error = error


async def request(http: httpx.AsyncClient, method: str, path: str, *, params: JsonObject | None = None,
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
        return payload
    if isinstance(payload, dict) and isinstance(payload.get("error"), dict):
        error = {k: v for k, v in payload["error"].items() if k in ("code", "message", "phase", "retryable", "buildId")}
        if not error.get("buildId"):
            error["buildId"] = build_id
        raise ToolFailure(error)
    raise ToolFailure({"code": "BUILD_ACCEPTANCE_UNKNOWN" if submitting else "HTTP_ERROR",
                       "message": f"Unexpected service response (HTTP {response.status_code}).",
                       "buildId": build_id, "retryable": submitting or response.status_code >= 500})
