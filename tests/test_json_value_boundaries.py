"""Recursive JSON fields preserve JSON types and reject arbitrary Python values."""

from datetime import date, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from dbt_metricflow_service.models.native import NativeNode
from dbt_metricflow_service.models.queries import SelectedFilter
from dbt_metricflow_service.platform.models import EngineResult
from dbt_metricflow_service.platform.queries import _json_cell


def test_native_extensions_round_trip_recursive_json_without_coercion():
    extension = {"flags": [True, False, None], "numbers": [1, 1.5], "nested": {"value": "2"}}
    original = {"name": "orders", "adapter_extension": extension}
    native = NativeNode.model_validate(original)
    assert native.native_details() == original


def test_query_filters_and_result_cells_preserve_recursive_json():
    value = {"values": [None, True, 1, 1.5, "1", {"nested": [False]}]}
    selected = SelectedFilter(option_id="option", operator="=", value=value)
    result = EngineResult(rows=[[value]])
    assert selected.value == value
    assert result.rows == [[value]]


@pytest.mark.parametrize("invalid", [object(), b"binary", Decimal("1.5"), date(2026, 1, 1)])
def test_python_objects_are_rejected_at_json_model_boundaries(invalid):
    with pytest.raises(ValidationError):
        EngineResult(rows=[[invalid]])
    with pytest.raises(ValidationError):
        SelectedFilter(option_id="option", operator="=", value=invalid)
    with pytest.raises(ValidationError):
        NativeNode.model_validate({"name": "orders", "adapter_extension": {"nested": [invalid]}})


@pytest.mark.parametrize("value,expected", [
    (Decimal("123456789.123456789"), "123456789.123456789"),
    (date(2026, 10, 14), "2026-10-14"),
    (datetime(2026, 10, 14, 12, 30), "2026-10-14T12:30:00"),
])
def test_database_values_are_explicitly_converted_before_json_validation(value, expected):
    assert _json_cell(value) == expected


def test_binary_database_values_are_rejected_without_implicit_text_conversion():
    with pytest.raises(ValidationError):
        _json_cell(b"binary")
