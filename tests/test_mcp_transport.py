import sys

import httpx
import pytest

pytest.importorskip("mcp")
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from dbt_metricflow_service.mcp_server import Gateway

BUILD = "00000000-0000-0000-0000-000000000001"


@pytest.mark.asyncio
async def test_stdio_discovery_explains_inputs_and_result_fields():
    launch = StdioServerParameters(command=sys.executable, args=["-m", "dbt_metricflow_service.mcp_server"],
                                   env={"DBT_METRICFLOW_SERVICE_URL": "http://127.0.0.1:1"})
    async with stdio_client(launch) as streams, ClientSession(*streams) as client:
        await client.initialize()
        discovered = {tool.name: tool for tool in (await client.list_tools()).tools}
    result_fields = {
        "get_current_deployment": ("activeBuildId", "undeployed"),
        "search_catalog": ("resourceId", "nextCursor"),
        "get_resource": ("resource", "source"),
        "get_available_dimensions": ("optionId", "granularities", "operators"),
        "query_metrics": ("columns", "rows", "normalizedTimeRange", "resultTruncated"),
        "submit_build": ("buildId", "get_build", "BUILD_ACCEPTANCE_UNKNOWN"),
        "get_build": ("buildStatus", "deploymentStatus", "queryAvailability"),
    }
    assert discovered.keys() == result_fields.keys()
    assert discovered["submit_build"].inputSchema["properties"]["idempotency_key"]["format"] == "uuid"
    for name, fields in result_fields.items():
        tool = discovered[name]
        assert all(field in tool.description for field in fields), name
        for param, schema in tool.inputSchema["properties"].items():
            assert schema.get("description"), (name, param)


@pytest.mark.asyncio
async def test_deployment_reads_actual_pointer_and_preserves_errors():
    args = {"repository": "https://example.org/demo", "environment_id": BUILD, "branch_name": "main"}
    for active in (None, BUILD):
        def respond(req, active=active):
            assert req.url.params["environmentId"] == BUILD
            key = {"repository": args["repository"], "environmentId": BUILD, "branchName": "main"}
            attempt = {**key, "buildId": "00000000-0000-0000-0000-000000000002", "deploymentStatus": "PENDING",
                       "generation": 2, "createdAt": "2026-01-01T00:00:00Z"}
            return httpx.Response(200, json={**key, "activeBuildId": active, "latestAttempt": attempt})

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


@pytest.mark.asyncio
async def test_malformed_auth_and_known_build_errors(build_request):
    responses = iter([httpx.Response(200, json={}), httpx.Response(202, json={}),
                      httpx.Response(401, text="Authorization required"),
                      httpx.Response(422, json={"error": {"code": "INVALID_REQUEST", "retryable": False,
                                                          "message": "rejected Bearer sensitive-token"}})])
    async with httpx.AsyncClient(base_url="http://test", headers={"Authorization": "Bearer sensitive-token"},
                                 transport=httpx.MockTransport(lambda req: next(responses))) as http:
        gateway = Gateway(http)
        args = build_request.model_dump(mode="json")
        deployment = {"repository": args["repository"], "environment_id": BUILD, "branch_name": "main"}
        bad = await gateway.call("get_current_deployment", {**deployment,
                                 "repository": "https://user:sensitive-token@example.org/demo"})
        assert bad.structuredContent["error"]["code"] == "INVALID_REQUEST"
        assert "sensitive-token" not in bad.content[0].text
        for tool, params, code, retry in [
            ("get_current_deployment", deployment, "HTTP_ERROR", False),
            ("submit_build", args, "BUILD_ACCEPTANCE_UNKNOWN", True),
            ("submit_build", args, "HTTP_ERROR", False),
            ("get_build", {"build_id": BUILD}, "INVALID_REQUEST", False),
            ("query_metrics", {"build_id": BUILD, "metric_resource_ids": ["revenue"], "limit": 0},
             "INVALID_REQUEST", False),
        ]:
            result = await gateway.call(tool, params)
            assert result.isError
            error = result.structuredContent["error"]
            assert (error["code"], error["retryable"]) == (code, retry)
            if "build_id" in params:
                assert error["buildId"] == BUILD
            assert "sensitive-token" not in result.content[0].text
