from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from dbt_metricflow_service.platform_models import PlatformQueryRequest
from dbt_metricflow_service.platform_queries import query_options, serialize_rows, validate_query


class FakeEngine:
    def list_metrics(self, include_dimensions: bool = False) -> list[object]:
        return [SimpleNamespace(name="revenue"), SimpleNamespace(name="orders")]

    def list_dimensions(self, metric_names: list[str]) -> list[object]:
        assert metric_names == ["revenue", "orders"]
        return [SimpleNamespace(dunder_name="customer__region", name="region", type="categorical")]

    def explain(self, request: object) -> object:
        token = request.group_by_names[0]
        if token not in {"metric_time__day", "metric_time__month"}:
            raise ValueError("unsupported grain")
        return object()


def test_multi_metric_options_return_common_dimensions_and_time_tokens() -> None:
    options = query_options(FakeEngine(), ("revenue", "orders"))
    assert options["dimensions"][0]["token"] == "customer__region"
    assert {item["token"] for item in options["timeDimensions"]} == {
        "metric_time__day", "metric_time__month",
    }


def test_dimension_values_are_bounded_and_typed() -> None:
    table = SimpleNamespace(
        column_descriptions=(SimpleNamespace(column_name="region", column_type=str),),
        rows=(("A",), ("B",), ("C",)),
    )
    result = serialize_rows(table, limit=2)
    assert result["rows"] == [["A"], ["B"]]
    assert result["truncated"] is True


def test_query_rejects_unlisted_filter_operator_and_foreign_run() -> None:
    request = PlatformQueryRequest.model_validate({
        "runId": "a" * 32, "idempotencyKey": "key", "mode": "QUERY", "metrics": ["revenue"],
        "groupBy": [], "filters": [{"field": "region", "operator": "DROP", "value": "A"}],
    })
    with pytest.raises(ValueError):
        validate_query(request, {"metrics": [{"name": "revenue"}], "dimensions": [], "timeDimensions": []})
    with pytest.raises(ValidationError):
        PlatformQueryRequest.model_validate({
            "runId": "a" * 32, "idempotencyKey": "key", "mode": "QUERY", "metrics": ["revenue"],
            "remote": "untrusted",
        })


def test_decimal_result_preserves_precision() -> None:
    table = SimpleNamespace(
        column_descriptions=(SimpleNamespace(column_name="revenue", column_type=Decimal),),
        rows=((Decimal("123456789.123456789"),),),
    )
    result = serialize_rows(table, limit=10)
    assert result["rows"] == [["123456789.123456789"]]
