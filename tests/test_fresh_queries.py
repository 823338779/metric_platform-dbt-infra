"""每次请求必须执行并返回新结果；不依赖外部数据库。"""

from copy import deepcopy
from types import SimpleNamespace
from uuid import uuid4

import pytest
from pydantic import ValidationError

from dbt_metricflow_service.application.queries import QueryService
from dbt_metricflow_service.models.queries import AvailableDimensionsRequest, QueryRequest
from tests.synchronous_queries import NATIVE_AVAILABLE_DIMENSIONS


@pytest.fixture
def live_service():
    build_id, run_id = str(uuid4()), str(uuid4())
    build = {
        "build_id": build_id, "run_id": run_id, "repository": "repository",
        "config_snapshot": {"businessTimezone": "Asia/Shanghai"},
        "output_set_id": str(uuid4()), "config_version": "1", "toolchain_version": "test",
        "commit_sha": "a" * 40,
    }
    executions = []

    def execute(build, request):
        executions.append(request)
        if request["mode"] == "OPTIONS":
            payload = deepcopy(NATIVE_AVAILABLE_DIMENSIONS)
            count = sum(item["mode"] == "OPTIONS" for item in executions)
            payload["allowedFilters"] = ["="] if count == 1 else ["IN"]
            return payload
        count = sum(item["mode"] == "QUERY" for item in executions)
        return {"columns": [{"name": "orders", "type": "int"}], "rows": [[count]], "truncated": False}

    builds = SimpleNamespace(load=lambda _: build, query_unavailable_reason=lambda _: None, timeout=30)
    catalogs = SimpleNamespace(read_build=lambda _: {"resources": [
        {"resourceId": "metric.orders", "name": "orders", "kind": "METRIC", "capabilities": ["QUERY"]},
    ]})
    return QueryService(builds, catalogs, execute), build_id, executions


def test_identical_queries_execute_again_and_return_updated_rows(live_service):
    service, build_id, executions = live_service
    request = QueryRequest(mode="QUERY", metricResourceIds=["metric.orders"])
    first = service.submit(build_id, request, "platform")
    second = service.submit(build_id, request, "platform")
    assert not {"queryId", "state", "resultAvailable", "nextOffset"} & second.model_dump(by_alias=True).keys()
    assert first.rows == [[1]]
    assert second.rows == [[2]]
    assert [item["mode"] for item in executions] == ["QUERY", "QUERY"]


def test_available_dimensions_are_recomputed_with_updated_metadata(live_service):
    service, build_id, executions = live_service
    request = AvailableDimensionsRequest(metricResourceIds=["metric.orders"])
    first = service.available_dimensions(build_id, request, "platform")
    second = service.available_dimensions(build_id, request, "platform")
    assert first.dimensions[0]["operators"] == ["="]
    assert second.dimensions[0]["operators"] == ["IN"]
    assert [item["mode"] for item in executions] == ["OPTIONS", "OPTIONS"]


def test_query_rejects_removed_idempotency_key():
    with pytest.raises(ValidationError) as failure:
        QueryRequest(mode="QUERY", metricResourceIds=["metric.orders"], idempotencyKey="old-key")
    assert any(error["loc"] == ("idempotencyKey",) and error["type"] == "extra_forbidden"
               for error in failure.value.errors())
