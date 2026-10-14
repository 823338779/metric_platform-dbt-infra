"""查询在同一请求完成，不暴露任务轮询，也不要求预先计算才能查询。"""

from types import SimpleNamespace
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from dbt_metricflow_service.api.errors import register_exception_handlers
from dbt_metricflow_service.api.limits import RequestBodyLimitMiddleware
from dbt_metricflow_service.api.queries import router
from dbt_metricflow_service.application.errors import ServiceError
from dbt_metricflow_service.models.queries import QueryRequest
from tests.synchronous_queries import inline_executor
from tests.test_publication_queries import query_service
from tests.test_publication_storage import store as store


def test_available_dimensions_response_contains_result_without_task_protocol():
    build_id = str(uuid4())
    result = {"buildId": build_id, "metricResourceIds": ["metric.orders"], "dimensions": []}
    service = SimpleNamespace(available_dimensions=lambda *args: result)
    app = FastAPI()
    app.include_router(router(service, lambda: "platform"))
    client = TestClient(app)

    response = client.post(
        "/query/available-dimensions", params={"build_id": build_id}, json={"metricResourceIds": ["metric.orders"]}
    )

    assert response.status_code == 200
    assert response.json() == result
    assert client.post(f"/builds/{build_id}/available-dimensions", json={}).status_code == 404
    assert client.post(f"/builds/{build_id}/query-options", json={}).status_code == 404
    assert client.get(f"/query-options/{uuid4()}").status_code == 404


@pytest.mark.parametrize("path", ["/query", "/query/available-dimensions"])
def test_query_routes_keep_request_body_limit(path):
    app = FastAPI()
    app.add_middleware(RequestBodyLimitMiddleware, max_bytes=32)
    service = SimpleNamespace(available_dimensions=lambda *args: pytest.fail("oversized request reached service"))
    app.include_router(router(service, lambda: "platform"))
    response = TestClient(app).post(
        path, params={"build_id": str(uuid4())}, json={"metricResourceIds": ["metric." + "x" * 50]},
    )
    assert response.status_code == 413
    assert response.json()["error"]["code"] == "REQUEST_TOO_LARGE"


def test_query_response_contains_rows_without_persistent_identity():
    build_id, query_id = str(uuid4()), str(uuid4())
    result = {
        "buildId": build_id, "mode": "QUERY",
        "commitSha": "a" * 40, "businessTimezone": "Asia/Shanghai",
        "normalizedTimeRange": {"startTime": None, "endTime": None},
        "columns": [{"name": "orders", "type": "int"}], "rows": [[7]],
        "resultTruncated": False,
    }
    app = FastAPI()
    app.include_router(router(SimpleNamespace(submit=lambda *args: result), lambda: "platform"))
    client = TestClient(app)

    response = client.post("/query", params={"build_id": build_id}, json={
        "mode": "QUERY", "metricResourceIds": ["metric.orders"],
    })

    assert response.status_code == 200
    assert response.json()["rows"] == [[7]]
    assert client.post(f"/builds/{build_id}/queries", json={}).status_code == 404
    assert client.get(f"/queries/{query_id}/status").status_code == 404
    assert client.get(f"/queries/{query_id}/results").status_code == 404


@pytest.mark.parametrize("fields", [{"offset": 0}, {"offset": 100}, {"limit": 0}, {"limit": 10001}])
def test_query_rejects_invalid_page_before_execution(fields):
    app = FastAPI()
    service = SimpleNamespace(submit=lambda *args: pytest.fail("invalid page reached execution"))
    app.include_router(router(service, lambda: "platform"))
    response = TestClient(app).post("/query", params={"build_id": str(uuid4())}, json={
        "mode": "QUERY", "metricResourceIds": ["metric.orders"],
        **fields,
    })
    assert response.status_code == 422


@pytest.mark.parametrize("path", ["/query", "/query/available-dimensions"])
@pytest.mark.parametrize("params", [{}, {"build_id": "invalid"}])
def test_query_routes_require_valid_build_parameter(path, params):
    app = FastAPI()
    app.include_router(router(SimpleNamespace(), lambda: "platform"))
    body = {"metricResourceIds": ["metric.orders"]}
    if path == "/query":
        body.update(mode="QUERY")
    response = TestClient(app).post(path, params=params, json=body)
    assert response.status_code == 422
    assert any(error["loc"] == ["query", "build_id"] for error in response.json()["detail"])


@pytest.mark.parametrize("path", ["/query", "/query/available-dimensions"])
def test_query_routes_reject_build_id_in_body(path):
    app = FastAPI()
    app.include_router(router(SimpleNamespace(), lambda: "platform"))
    body = {"buildId": str(uuid4()), "metricResourceIds": ["metric.orders"]}
    if path == "/query":
        body.update(mode="QUERY")
    response = TestClient(app).post(path, params={"build_id": str(uuid4())}, json=body)
    assert response.status_code == 422
    assert any(error["loc"] == ["body", "buildId"] for error in response.json()["detail"])


@pytest.mark.parametrize("code,status", [("INVALID_QUERY", 422), ("COMMAND_FAILED", 503), ("COMMAND_TIMEOUT", 504)])
def test_failed_query_returns_http_error_in_original_request(store, tmp_path, code, status):
    service, parent, build = query_service(store, tmp_path)

    def fail_query(build, payload):
        raise ServiceError(code, "query execution failed", status, build_id=build.build_id)

    service.execute = fail_query
    app = FastAPI()
    register_exception_handlers(app)
    app.include_router(router(service, lambda: "platform"))
    response = TestClient(app).post("/query", params={"build_id": build.build_id}, json={
        "mode": "QUERY", "metricResourceIds": ["metric.sample.orders"],
    })
    assert response.status_code == status
    assert response.json()["error"]["code"] == code
    assert response.json()["error"]["buildId"] == build.build_id


def test_post_returns_current_rows_without_persistent_identity(store, tmp_path):
    service, parent, build = query_service(store, tmp_path)
    payload = {"columns": [{"name": "orders", "type": "int"}], "rows": [[i] for i in range(250)]}
    service.execute = inline_executor(query_result=payload)
    app = FastAPI()
    app.include_router(router(service, lambda: "platform"))
    client = TestClient(app)
    params = {"build_id": build.build_id}
    body = {"mode": "QUERY", "metricResourceIds": ["metric.sample.orders"], "limit": 1000}
    first = client.post("/query", params=params, json=body)
    assert first.status_code == 200
    assert first.json()["rows"] == payload["rows"]
    assert not {"queryId", "state", "nextOffset", "resultAvailable"} & first.json().keys()
    payload["rows"] = [[999]]
    second = client.post("/query", params=params, json=body)
    assert second.status_code == 200
    assert second.json()["rows"] == [[999]]
    assert len(service.execute.calls) == 2
    assert client.post("/query", params=params, json={**body, "limit": 500}).status_code == 200


def test_query_computes_current_dimensions_before_compiling(store, tmp_path):
    service, parent, build = query_service(store, tmp_path)
    option = service.fixture_available_dimensions.dimensions[0].option_id
    count = len(service.execute.calls)
    service.submit(build.build_id, QueryRequest(
        mode="QUERY", metricResourceIds=["metric.sample.orders"], groupBy=[{"optionId": option}],
    ), "platform")
    assert service.execute.calls[count][1] == ("orders",)
    assert service.execute.calls[count + 1][1].mode == "QUERY"
    assert service.execute.calls[-1][1].group_by == ["order__region"]
