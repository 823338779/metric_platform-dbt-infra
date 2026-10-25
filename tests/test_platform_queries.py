from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from dbt_metricflow_service.delegates.metricflow.queries import available_dimensions, serialize_rows
from dbt_metricflow_service.platform.models import PlatformQueryRequest


class FakeEngine:
    def list_metrics(self, include_dimensions: bool = False) -> list[object]:
        return [SimpleNamespace(name="revenue"), SimpleNamespace(name="orders")]

    def list_dimensions(self, metric_names: list[str]) -> list[object]:
        assert metric_names == ["revenue", "orders"]
        return [SimpleNamespace(
            dunder_name="customer__region", name="region", type="categorical", semantic_model_reference=None,
        )]

    def explain(self, request: object) -> object:
        token = request.group_by_names[0]
        if token not in {"metric_time__day", "metric_time__month"}:
            raise ValueError("unsupported grain")
        return object()


def test_multi_metric_available_dimensions_return_common_dimensions_and_time_tokens() -> None:
    available = available_dimensions(FakeEngine(), ("revenue", "orders"))
    assert available.dimensions[0].token == "customer__region"
    assert {item.token for item in available.time_dimensions} == {
        "metric_time__day", "metric_time__month",
    }


def test_dimension_values_are_bounded_and_typed() -> None:
    table = SimpleNamespace(
        column_descriptions=(SimpleNamespace(column_name="region", column_type=str),),
        rows=(("A",), ("B",), ("C",)),
    )
    result = serialize_rows(table, limit=2)
    assert result.rows == [["A"], ["B"]]
    assert result.truncated is True


def test_query_rejects_extra_request_fields() -> None:
    with pytest.raises(ValidationError):
        PlatformQueryRequest.model_validate({
            "runId": "a" * 32, "mode": "QUERY", "metrics": ["revenue"],
            "remote": "untrusted",
        })


def test_decimal_result_preserves_precision() -> None:
    table = SimpleNamespace(
        column_descriptions=(SimpleNamespace(column_name="revenue", column_type=Decimal),),
        rows=((Decimal("123456789.123456789"),),),
    )
    result = serialize_rows(table, limit=10)
    assert result.rows == [["123456789.123456789"]]


def test_time_dimensions_advertise_only_verified_grains_on_their_own_join_path():
    class TimeEngine(FakeEngine):
        def list_dimensions(self, metric_names):
            return [
                SimpleNamespace(
                    dunder_name="metric_time__day", name="metric_time", type="time", semantic_model_reference=None,
                ),
                SimpleNamespace(dunder_name="order__day", name="day", type="time", semantic_model_reference=None),
                SimpleNamespace(dunder_name="return__day", name="day", type="time", semantic_model_reference=None),
            ]

        def explain(self, request):
            if request.group_by_names[0] not in {
                "metric_time__day", "metric_time__month", "order__day__month", "return__day__year",
            }:
                raise ValueError("unsupported grain")
            return object()

    available = available_dimensions(TimeEngine(), ("revenue", "orders"))
    assert [entry.grain_tokens for entry in available.dimensions] == [
        {"day": "metric_time__day", "month": "metric_time__month"},
        {"month": "order__day__month"},
        {"year": "return__day__year"},
    ]
