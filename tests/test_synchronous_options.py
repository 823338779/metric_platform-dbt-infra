"""查询选项在同一请求完成，不创建任务，也不要求预先计算才能查询。"""

from types import SimpleNamespace
from uuid import uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient

from dbt_metricflow_service.api.queries import router


def test_options_response_contains_result_without_task_protocol():
    build_id = str(uuid4())
    result = {"buildId": build_id, "metricResourceIds": ["metric.orders"], "options": []}
    service = SimpleNamespace(options=lambda *args: result)
    app = FastAPI()
    app.include_router(router(service, lambda: "platform"))
    client = TestClient(app)

    response = client.post(
        f"/v3/builds/{build_id}/query-options", json={"metricResourceIds": ["metric.orders"]}
    )

    assert response.status_code == 200
    assert response.json() == result
    assert client.get(f"/v3/query-options/{uuid4()}").status_code == 404
