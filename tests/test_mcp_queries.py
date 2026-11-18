import httpx
import pytest

pytest.importorskip("mcp")
from test_mcp_transport import BUILD

from dbt_metricflow_service.mcp_server import Gateway


@pytest.mark.asyncio
async def test_query_uses_exact_resource_ids_and_preserves_results():
    uid = "metric.package.revenue"
    table = {"columns": [{"name": "revenue", "type": "number"}], "rows": [[1.25], [None]], "buildId": BUILD,
             "mode": "QUERY", "commitSha": "a" * 40, "businessTimezone": "Asia/Shanghai",
             "normalizedTimeRange": {"startTime": None, "endTime": None},
             "resultTruncated": True}
    requests = []

    def respond(req):
        import json
        requests.append(req)
        if req.url.path.endswith("/search"):
            assert req.url.params["resourceId"] == uid
            return httpx.Response(200, json={"items": [{"resourceId": uid, "name": "revenue", "description": "",
                                                        "resourceType": "metric", "queryNameUnique": True}]})
        body = json.loads(req.content)
        assert req.url.params["build_id"] == BUILD
        assert body["metrics"] == ["revenue"]
        assert body["groupBy"] == [{"name": "channel", "bindings": {"revenue": {"optionId": "d1"}}}]
        assert body["orderBy"] == [{"fieldId": "revenue", "direction": "DESC"}]
        return httpx.Response(200, json=table)

    async with httpx.AsyncClient(base_url="http://test", transport=httpx.MockTransport(respond)) as http:
        result = await Gateway(http).call("query_metrics", {
            "build_id": BUILD, "metric_resource_ids": [uid],
            "group_by": [{"name": "channel", "bindings": {uid: {"optionId": "d1"}}}],
            "order_by": [{"fieldId": uid, "direction": "DESC"}],
        })
    assert result.structuredContent == table
    assert len(requests) == 2
