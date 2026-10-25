import pytest
from pydantic import TypeAdapter, ValidationError

from dbt_metricflow_service.models.builds import BuildView
from dbt_metricflow_service.models.queries import QueryRequest, QueryResult, SelectedFilter


@pytest.mark.parametrize("body", [
    {"mode": "PREVIEW", "selector": "orders", "metrics": []},
    {"mode": "PREVIEW", "selector": "orders", "startTime": None},
    {"mode": "PREVIEW", "selector": "   "},
    {"mode": "QUERY", "metrics": ["revenue"], "selector": None},
    {"mode": "EXPLAIN", "metrics": ["revenue"], "dimensionOptionId": None},
    {"mode": "DIMENSION_VALUES", "metrics": ["revenue"], "dimensionOptionId": "d", "groupBy": []},
    {"mode": "DIMENSION_VALUES", "metrics": ["revenue"], "dimensionOptionId": "d", "orderBy": []},
])
def test_reject_mode_unrelated_fields(body):
    with pytest.raises(ValidationError):
        TypeAdapter(QueryRequest).validate_python(body)


@pytest.mark.parametrize("operator,value", [
    ("IN", 3), ("IN", []), ("IN", [None]), ("IN", [{}]), ("IN", list(range(101))),
    ("=", {}), ("=", []), ("=", None), ("=", float("inf")), ("=", float("nan")),
    ("OR 1=1", "x"),
])
def test_invalid_filter_value_is_rejected_at_request_boundary(operator, value):
    with pytest.raises(ValidationError):
        SelectedFilter(optionId="d", operator=operator, value=value)


@pytest.mark.parametrize("operator,value", [("IN", [1, "x", True]), ("=", 0), ("!=", False), ("=", "")])
def test_filter_preserves_scalar_json_types(operator, value):
    item = SelectedFilter(optionId="d", operator=operator, value=value)
    assert item.value == value
    assert type(item.value) is type(value)


@pytest.mark.parametrize("mode", ["QUERY", "EXPLAIN", "PREVIEW", "DIMENSION_VALUES"])
def test_each_mode_has_a_valid_request(mode):
    body = {"mode": mode, "limit": 10}
    if mode == "PREVIEW":
        body["selector"] = "orders"
    else:
        body["metrics"] = ["revenue"]
    if mode == "DIMENSION_VALUES":
        body["dimensionOptionId"] = "d"
    assert TypeAdapter(QueryRequest).validate_python(body).mode == mode


@pytest.mark.parametrize("mode,extra,absent", [
    ("PREVIEW", {"columns": [], "rows": [], "resultTruncated": False},
     {"sql", "businessTimezone", "normalizedTimeRange", "boundaryPolicy"}),
    ("EXPLAIN", {"sql": "select 1", "businessTimezone": "UTC",
                 "normalizedTimeRange": {"startTime": None, "endTime": None}},
     {"columns", "rows", "resultTruncated", "boundaryPolicy"}),
])
def test_mode_results_only_contain_relevant_fields(mode, extra, absent):
    result = TypeAdapter(QueryResult).validate_python({
        "buildId": "00000000-0000-0000-0000-000000000001", "commitSha": "b" * 40, "mode": mode, **extra,
    })
    assert not absent.intersection(result.model_dump(by_alias=True))


def test_build_view_has_one_config_version_location():
    assert "config_version" not in BuildView.model_fields
    assert "execution" in BuildView.model_fields
