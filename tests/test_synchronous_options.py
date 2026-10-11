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
        "/v3/query/available-dimensions", params={"build_id": build_id}, json={"metricResourceIds": ["metric.orders"]}
    )

    assert response.status_code == 200
    assert response.json() == result
    assert client.post(f"/v3/builds/{build_id}/available-dimensions", json={}).status_code == 404
    assert client.post(f"/v3/builds/{build_id}/query-options", json={}).status_code == 404
    assert client.get(f"/v3/query-options/{uuid4()}").status_code == 404


@pytest.mark.parametrize("path", ["/v3/query", "/v3/query/available-dimensions"])
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


def test_query_response_contains_first_page_without_status_polling():
    build_id, query_id = str(uuid4()), str(uuid4())
    result = {
        "queryId": query_id, "buildId": build_id, "state": "SUCCEEDED", "mode": "QUERY",
        "commitSha": "a" * 40, "businessTimezone": "Asia/Shanghai", "normalizedTimeRange": {},
        "resultAvailable": True, "columns": [{"name": "orders", "type": "int"}], "rows": [[7]],
        "offset": 0, "returnedRows": 1, "nextOffset": None, "availableRows": 1, "resultTruncated": False,
    }
    app = FastAPI()
    app.include_router(router(SimpleNamespace(submit=lambda *args: result), lambda: "platform"))
    client = TestClient(app)

    response = client.post("/v3/query", params={"build_id": build_id}, json={
        "idempotencyKey": "query", "mode": "QUERY", "metricResourceIds": ["metric.orders"],
    })

    assert response.status_code == 200
    assert response.json()["rows"] == [[7]]
    assert client.post(f"/v3/builds/{build_id}/queries", json={}).status_code == 404
    assert client.get(f"/v3/queries/{query_id}/status").status_code == 404
    assert client.get(f"/v3/queries/{query_id}/results").status_code == 404


@pytest.mark.parametrize("fields", [{"offset": 0}, {"offset": 100}, {"limit": 0}, {"limit": 10001}])
def test_query_rejects_invalid_page_before_execution(fields):
    app = FastAPI()
    service = SimpleNamespace(submit=lambda *args: pytest.fail("invalid page reached execution"))
    app.include_router(router(service, lambda: "platform"))
    response = TestClient(app).post("/v3/query", params={"build_id": str(uuid4())}, json={
        "idempotencyKey": "query", "mode": "QUERY", "metricResourceIds": ["metric.orders"],
        **fields,
    })
    assert response.status_code == 422


@pytest.mark.parametrize("path", ["/v3/query", "/v3/query/available-dimensions"])
@pytest.mark.parametrize("params", [{}, {"build_id": "invalid"}])
def test_query_routes_require_valid_build_parameter(path, params):
    app = FastAPI()
    app.include_router(router(SimpleNamespace(), lambda: "platform"))
    body = {"metricResourceIds": ["metric.orders"]}
    if path == "/v3/query":
        body.update(idempotencyKey="query", mode="QUERY")
    response = TestClient(app).post(path, params=params, json=body)
    assert response.status_code == 422
    assert any(error["loc"] == ["query", "build_id"] for error in response.json()["detail"])


@pytest.mark.parametrize("path", ["/v3/query", "/v3/query/available-dimensions"])
def test_query_routes_reject_build_id_in_body(path):
    app = FastAPI()
    app.include_router(router(SimpleNamespace(), lambda: "platform"))
    body = {"buildId": str(uuid4()), "metricResourceIds": ["metric.orders"]}
    if path == "/v3/query":
        body.update(idempotencyKey="query", mode="QUERY")
    response = TestClient(app).post(path, params={"build_id": str(uuid4())}, json=body)
    assert response.status_code == 422
    assert any(error["loc"] == ["body", "buildId"] for error in response.json()["detail"])


@pytest.mark.parametrize("code,status", [("INVALID_QUERY", 422), ("COMMAND_FAILED", 503), ("COMMAND_TIMEOUT", 504)])
def test_failed_query_returns_http_error_in_original_request(store, tmp_path, code, status):
    service, parent, build = query_service(store, tmp_path)

    def fail_query(job_id):
        job = service.jobs.claim(str(uuid4()), toolchain_version=parent["toolchain_version"], job_id=job_id)
        service.jobs.fail(job_id, job["lease_token"], code, {"private": "secret-test-value"})

    service.execute = fail_query
    app = FastAPI()
    register_exception_handlers(app)
    app.include_router(router(service, lambda: "platform"))
    response = TestClient(app).post("/v3/query", params={"build_id": build["build_id"]}, json={
        "idempotencyKey": uuid4().hex,
        "mode": "QUERY", "metricResourceIds": ["metric.sample.orders"],
    })
    assert response.status_code == status
    assert response.json()["error"]["code"] == code
    assert response.json()["error"]["buildId"] == build["build_id"]
    assert "secret-test-value" not in response.text


def test_completed_query_returns_first_page_and_replays_without_execution(store, tmp_path):
    service, parent, build = query_service(store, tmp_path)
    payload = {"columns": [{"name": "orders", "type": "int"}], "rows": [[i] for i in range(250)]}
    service.execute = inline_executor(service.jobs, parent["toolchain_version"], query_result=payload)
    request = QueryRequest(idempotencyKey=uuid4().hex, mode="QUERY", metricResourceIds=["metric.sample.orders"])

    first = service.submit(build["build_id"], request, "platform")
    assert first.state == "SUCCEEDED"
    assert first.rows == payload["rows"][:100]
    assert first.next_offset == 100
    following = service.results(first.query_id, first.next_offset, 200)
    assert following.rows == payload["rows"][100:]
    assert following.next_offset is None
    assert service.submit(build["build_id"], request, "platform") == first
    assert service.jobs.get(first.query_id)["attempt_no"] == 1


def test_post_returns_first_page_and_replays_without_reexecution(store, tmp_path):
    service, parent, build = query_service(store, tmp_path)
    payload = {"columns": [{"name": "orders", "type": "int"}], "rows": [[i] for i in range(250)]}
    service.execute = inline_executor(service.jobs, parent["toolchain_version"], query_result=payload)
    app = FastAPI()
    register_exception_handlers(app)
    app.include_router(router(service, lambda: "platform"))
    client = TestClient(app)
    url = "/v3/query"
    params = {"build_id": build["build_id"]}
    body = {"idempotencyKey": uuid4().hex, "mode": "QUERY",
            "metricResourceIds": ["metric.sample.orders"], "limit": 1000}

    dimensions = client.post("/v3/query/available-dimensions", params=params,
                             json={"metricResourceIds": ["metric.sample.orders"]})
    assert dimensions.status_code == 200
    assert dimensions.json()["buildId"] == build["build_id"]
    assert dimensions.json()["dimensions"]
    first = client.post(url, params=params, json=body)
    assert first.status_code == 200
    assert first.json()["buildId"] == build["build_id"]
    assert first.json()["rows"] == payload["rows"][:100]
    assert first.json()["offset"] == 0
    query_id = first.json()["queryId"]

    replay = client.post(url, params=params, json=body)
    assert replay.status_code == 200
    assert replay.json() == first.json()
    assert service.jobs.get(query_id)["attempt_no"] == 1
    conflict = client.post(url, params=params, json={**body, "limit": 500})
    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"


def test_query_recomputes_options_when_cached_result_is_gone(store, tmp_path):
    service, parent, build = query_service(store, tmp_path)
    option = service.fixture_options.dimensions[0]["optionId"]
    records = service.jobs.options_for(parent["job_id"])
    with store.db.transaction() as connection:
        for record in records:
            connection.exec_driver_sql("DELETE FROM runtime_job_result WHERE job_id=%s", (record["job_id"],))

    result = service.submit(build["build_id"], QueryRequest(
        idempotencyKey=uuid4().hex, mode="QUERY", metricResourceIds=["metric.sample.orders"],
        groupBy=[{"optionId": option}],
    ), "platform")

    assert result.state == "SUCCEEDED"
    saved = service.jobs.get(result.query_id)
    assert saved["request_json"]["engineRequest"]["groupBy"] == ["order__region"]


def test_unfinished_query_retry_rejects_incompatible_engine_without_waiting(store, tmp_path):
    service, parent, build = query_service(store, tmp_path)
    request = QueryRequest(idempotencyKey=uuid4().hex, mode="QUERY", metricResourceIds=["metric.sample.orders"])

    def interrupted(job_id):
        raise RuntimeError("request stopped before execution")

    service.execute = interrupted
    with pytest.raises(RuntimeError):
        service.submit(build["build_id"], request, "platform")
    service.builds.toolchain = "incompatible-toolchain"
    service.execute = lambda *_: pytest.fail("incompatible query reached execution")

    with pytest.raises(ServiceError) as failure:
        service.submit(build["build_id"], request, "platform")
    assert failure.value.status == 503
    assert failure.value.error.code == "EXECUTION_ENVIRONMENT_UNAVAILABLE"
