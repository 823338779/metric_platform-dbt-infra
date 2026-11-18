import json

import httpx
import pytest

pytest.importorskip("mcp")
from test_mcp_transport import BUILD

from dbt_metricflow_service.mcp_server import TOOLS, Gateway


@pytest.mark.asyncio
async def test_submit_recovers_same_request_and_reads_separate_status(build_request):
    sent = []
    args = build_request.model_dump(mode="json", by_alias=False, exclude_none=True)
    args["deployment_policy"] = "ON_SUCCESS"
    accepted = {"execution": build_request.execution.model_dump(mode="json", by_alias=True), "buildId": BUILD,
                "repository": args["repository"], "branchName": "main", "requestedCommitSha": args["commit_sha"],
                "commitSha": args["commit_sha"], "toolchainVersion": "test", "buildStatus": "QUEUED", "phase": "QUEUED",
                "createdAt": "2026-01-01T00:00:00Z", "updatedAt": "2026-01-01T00:00:00Z"}

    def respond(req):
        if req.method == "GET":
            return httpx.Response(200, json={**accepted, "buildStatus": "SUCCEEDED", "initialDeployment": {
                "deploymentStatus": "FAILED", "repository": args["repository"], "environmentId": BUILD,
                "branchName": "main", "generation": 1, "buildId": BUILD, "createdAt": accepted["createdAt"]}})
        sent.append(json.loads(req.content))
        if len(sent) == 1:
            raise httpx.ReadTimeout("unknown")
        return httpx.Response(202, json=accepted)

    async with httpx.AsyncClient(base_url="http://test", transport=httpx.MockTransport(respond)) as http:
        gateway = Gateway(http)
        for field in ("commit_sha", "deployment_policy", "idempotency_key"):
            result = await gateway.call("submit_build", {k: v for k, v in args.items() if k != field})
            assert result.isError
        assert not sent
        result = await gateway.call("submit_build", args)
        assert result.structuredContent["error"]["code"] == "BUILD_ACCEPTANCE_UNKNOWN"
        assert len(sent) == 1
        assert (await gateway.call("submit_build", args)).structuredContent["buildId"] == BUILD
        assert sent[0] == sent[1] and sent[0]["idempotencyKey"] == args["idempotency_key"]
        result = (await gateway.call("get_build", {"build_id": BUILD})).structuredContent
        assert result["buildStatus"] == "SUCCEEDED" and result["initialDeployment"]["deploymentStatus"] == "FAILED"
        assert result["queryAvailability"] == "unknown"
        assert len(TOOLS) == 7
