"""查询直接交给执行入口，不按构建状态、产物引用或工具链版本预判。"""
from types import SimpleNamespace

import pytest

from dbt_metricflow_service.application.builds import BuildService
from dbt_metricflow_service.application.errors import ServiceError
from dbt_metricflow_service.application.queries import QueryService
from dbt_metricflow_service.models.queries import AvailableDimensionsRequest, QueryRequest
from dbt_metricflow_service.platform.models import DataQueryResult, DimensionQueryResult
from tests.data_helpers import stored_build


@pytest.mark.parametrize("changes", [
    {"build_status": "QUEUED"}, {"build_status": "FAILED"},
    {"output_set_id": None}, {"toolchain_version": "historical-version"},
])
def test_queries_reach_execution_without_availability_gate(changes):
    build = stored_build(**changes)
    calls = []
    builds = BuildService(SimpleNamespace(get=lambda _: build), "current-version")
    def execute(selected, request):
        assert selected is build
        calls.append("query")
        return DataQueryResult(rows=[[1]])
    def dimensions(selected, metrics):
        assert selected is build
        calls.append("dimensions")
        return DimensionQueryResult()
    queries = QueryService(builds, execute, dimensions)
    assert queries.submit(build.build_id, QueryRequest(mode="QUERY", metrics=["orders"]), "test").rows == [[1]]
    queries.available_dimensions(build.build_id, AvailableDimensionsRequest(metrics=["orders"]), "test")
    assert calls == ["query", "dimensions"]
    view = builds.view(build, None).model_dump(by_alias=True)
    assert not {"queryAvailable", "queryUnavailableReason"} & view.keys()


def test_execution_error_is_preserved_without_preflight_rejection():
    build = stored_build(build_status="FAILED", output_set_id=None)
    builds = BuildService(SimpleNamespace(get=lambda _: build), "different-version")
    error = ServiceError("ARTIFACT_UNAVAILABLE", "manifest missing", 503)
    def execute(*args):
        raise error
    queries = QueryService(builds, execute, execute)
    with pytest.raises(ServiceError) as caught:
        queries.submit(build.build_id, QueryRequest(mode="QUERY", metrics=["orders"]), "test")
    assert caught.value is error
