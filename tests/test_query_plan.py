from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from pydantic import TypeAdapter

from dbt_metricflow_service.application.errors import ServiceError
from dbt_metricflow_service.application.queries import QueryService
from dbt_metricflow_service.application.query_plan import compile_query, normalize_query, resolve_dimensions
from dbt_metricflow_service.engines.models import DataQueryResult, DimensionQueryResult, NativeDimension
from dbt_metricflow_service.models.queries import QueryRequest

BUILD_ID = "00000000-0000-0000-0000-000000000001"


@pytest.fixture
def options():
    return resolve_dimensions(BUILD_ID, ["revenue"], DimensionQueryResult(dimensions=[
        NativeDimension(token="order__ds", name="ds", type="time", grain_tokens={"month": "order__ds__month"}),
    ], allowed_filters=["=", "IN"]))


def test_time_normalization_and_order_use_resolved_grain(options):
    option = next(iter(options))
    request = TypeAdapter(QueryRequest).validate_python({
        "mode": "QUERY", "metrics": ["revenue", "revenue"],
        "groupBy": [{"optionId": option, "grain": "month"}],
        "orderBy": [{"fieldId": option, "direction": "DESC"}],
        "startTime": "2024-01-01T00:00:00Z", "endTime": "2024-01-02T00:00:00+08:00",
    })
    normalized = normalize_query(request, "Asia/Shanghai")
    compiled = compile_query(normalized, options)
    assert compiled.start_time.isoformat() == "2024-01-01T08:00:00"
    assert compiled.end_time.isoformat() == "2024-01-02T00:00:00"
    assert compiled.metrics == ["revenue"]
    assert compiled.group_by == ["order__ds__month"]
    assert compiled.order_by == ["-order__ds__month"]
    assert request.start_time.tzinfo is not None


@pytest.mark.parametrize("fields,code", [
    ({"groupBy": [{"optionId": "wrong"}]}, "INVALID_DIMENSION_OPTION"),
    ({"groupBy": [{"optionId": "valid", "grain": "year"}]}, "INVALID_DIMENSION_OPTION"),
    ({"filters": [{"optionId": "valid", "operator": ">", "value": 1}]}, "INVALID_FILTER_OPTION"),
    ({"orderBy": [{"fieldId": "wrong", "direction": "ASC"}]}, "INVALID_ORDER_FIELD"),
])
def test_query_selection_errors(options, fields, code):
    for items in fields.values():
        for item in items:
            if item.get("optionId") == "valid":
                item["optionId"] = next(iter(options))
    request = TypeAdapter(QueryRequest).validate_python({"mode": "QUERY", "metrics": ["revenue"], **fields})
    with pytest.raises(ServiceError) as caught:
        compile_query(request, options)
    assert caught.value.error.code == code


def test_time_range_rejection_happens_before_execution():
    execute = Mock()
    build = SimpleNamespace(build_id=BUILD_ID, config_snapshot=SimpleNamespace(business_timezone="UTC"))
    service = QueryService(SimpleNamespace(load=lambda _: build), execute, Mock(), 1024)
    request = TypeAdapter(QueryRequest).validate_python({
        "mode": "QUERY", "metrics": ["revenue"], "startTime": "2024-02-01", "endTime": "2024-01-01",
    })
    with pytest.raises(ServiceError) as caught:
        service.submit(BUILD_ID, request)
    assert caught.value.error.code == "INVALID_TIME_RANGE"
    execute.assert_not_called()


def test_query_limit_applies_to_final_utf8_response():
    build = SimpleNamespace(build_id=BUILD_ID, commit_sha="b" * 40,
                            config_snapshot=SimpleNamespace(business_timezone="UTC"))
    service = QueryService(SimpleNamespace(load=lambda _: build), lambda *args: DataQueryResult(rows=[["中文"]]),
                           Mock(), 1024)
    request = TypeAdapter(QueryRequest).validate_python({"mode": "PREVIEW", "selector": "orders"})
    result = service.submit(BUILD_ID, request)
    actual_size = len(result.model_dump_json(by_alias=True).encode())
    service.max_result_bytes = actual_size
    assert service.submit(BUILD_ID, request).rows == [["中文"]]
    service.max_result_bytes = actual_size - 1
    with pytest.raises(ServiceError) as caught:
        service.submit(BUILD_ID, request)
    assert caught.value.error.code == "RESULT_TOO_LARGE"


def test_option_identity_is_bound_to_build_and_metric_selection(options):
    native = DimensionQueryResult(dimensions=[NativeDimension(token="order__ds", name="ds")])
    current = next(iter(options))
    assert current not in resolve_dimensions("another-build", ["revenue"], native)
    assert current not in resolve_dimensions(BUILD_ID, ["other-metric"], native)
