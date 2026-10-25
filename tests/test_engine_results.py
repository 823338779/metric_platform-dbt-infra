from datetime import date, datetime
from decimal import Decimal
from types import SimpleNamespace

from dbt_metricflow_service.engines.results import json_cell, serialize_rows


def test_shared_json_conversion_preserves_precision_and_dates():
    assert json_cell(Decimal("12345678901234567890.123456789")) == "12345678901234567890.123456789"
    assert json_cell(date(2024, 1, 1)) == "2024-01-01"
    assert json_cell(datetime(2024, 1, 1, 12, 30)) == "2024-01-01T12:30:00"
    assert json_cell(None) is None
    assert json_cell(False) is False


def test_only_extra_row_marks_truncation():
    table = SimpleNamespace(column_descriptions=[SimpleNamespace(column_name="value", column_type=Decimal)],
                            rows=[(Decimal("1.20"),), (Decimal("2.30"),)])
    result = serialize_rows(table, limit=1)
    assert result.rows == [["1.20"]] and result.truncated
    assert serialize_rows(table, limit=2).truncated is False
