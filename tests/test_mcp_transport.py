import httpx
import pytest

pytest.importorskip("mcp")
from dbt_metricflow_service.mcp_server import Gateway

BUILD = "00000000-0000-0000-0000-000000000001"


@pytest.mark.asyncio
async def test_deployment_reads_actual_pointer_and_preserves_errors():
    args = {"repository": "https://example.org/demo", "environment_id": BUILD, "branch_name": "main"}
    for active in (None, BUILD):
        def respond(req, active=active):
            assert req.url.params["environmentId"] == BUILD
            return httpx.Response(200, json={"activeBuildId": active, "latestAttempt": {"buildId": "other"}})

        async with httpx.AsyncClient(base_url="http://service", transport=httpx.MockTransport(respond)) as http:
            result = await Gateway(http).call("get_current_deployment", args)
            assert not result.isError
            assert result.structuredContent["status"] == ("active" if active else "undeployed")
            assert result.structuredContent["activeBuildId"] == active


@pytest.mark.asyncio
async def test_connection_failure_and_invalid_input_are_structured():
    def fail(req):
        raise httpx.ConnectError("secret")

    async with httpx.AsyncClient(base_url="http://service", transport=httpx.MockTransport(fail)) as http:
        gateway = Gateway(http)
        args = {"repository": "https://example.org/demo", "environment_id": BUILD, "branch_name": "main"}
        for params, code in [(args, "SERVICE_UNREACHABLE"), ({"secret": "secret"}, "INVALID_REQUEST")]:
            result = await gateway.call("get_current_deployment", params)
            assert result.isError and result.structuredContent["error"]["code"] == code
            assert "secret" not in result.content[0].text
