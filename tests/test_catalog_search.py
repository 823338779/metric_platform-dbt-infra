import json
from types import SimpleNamespace
from uuid import uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient

from dbt_metricflow_service.api.catalog import router
from dbt_metricflow_service.api.errors import register_exception_handlers
from dbt_metricflow_service.application.catalog import CatalogService


def test_search_pages_and_resolves_metrics():
    build_id = str(uuid4())
    metrics = {}
    for index in range(5):
        uid = f"metric.demo.sales{index}"
        metrics[uid] = {"unique_id": uid, "name": f"sales{index}", "resource_type": "metric",
                            "label": "销售额", "description": "渠道" * 45}
    manifest = json.dumps({"metrics": metrics, "nodes": {}}).encode()
    build = SimpleNamespace(build_id=build_id, build_status="SUCCEEDED", output_set_id="sealed")
    service = CatalogService(SimpleNamespace(get=lambda _: build), SimpleNamespace(read_file=lambda *_: manifest),
                             lambda *_: list(metrics.values()), 850)
    app = FastAPI()
    register_exception_handlers(app)
    app.include_router(router(service))
    http = TestClient(app)
    catalog = f"/builds/{build_id}/catalog"
    assert http.get(catalog).json()["error"]["code"] == "RESULT_TOO_LARGE"
    url = catalog + "/search"
    params = {"keyword": "销售", "resourceType": "metric"}
    seen = []
    while True:
        response = http.get(url, params=params)
        assert response.status_code == 200
        assert len(response.content) <= 850
        page = response.json()
        seen.extend(item["resourceId"] for item in page["items"])
        if not page["nextCursor"]:
            break
        params["cursor"] = page["nextCursor"]
        assert http.get(url, params={**params, "keyword": "other"}).status_code == 422
    assert seen == sorted(metrics)
    exact = http.get(url, params={"resourceId": seen[-1], "resourceType": "metric"})
    assert exact.json()["items"][0]["queryNameUnique"] is True
    assert http.get(url, params={"resourceId": "unknown"}).status_code == 404
    assert not http.get(url, params={"keyword": "missing"}).json()["items"]
    metrics[seen[0]]["name"] = metrics[seen[-1]]["name"]
    manifest = json.dumps({"metrics": metrics}).encode()
    assert not http.get(url, params={"resourceId": seen[-1]}).json()["items"][0]["queryNameUnique"]
    service.max_result_bytes = 30
    assert http.get(url, params={"keyword": "sales"}).json()["error"]["code"] == "RESULT_TOO_LARGE"
