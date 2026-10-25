"""dbt 与 MetricFlow 共用的 JSON 单元格与表格转换。"""
from datetime import date, datetime
from decimal import Decimal

from metricflow.data_table.mf_table import MetricFlowDataTable
from pydantic import JsonValue, TypeAdapter

from ..models.queries import ResultColumn
from .models import DataQueryResult

JSON_VALUE_ADAPTER = TypeAdapter(JsonValue)

def json_cell(value: JsonValue | Decimal | date | datetime | bytes) -> JsonValue:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return JSON_VALUE_ADAPTER.validate_python(value)


def serialize_rows(table: MetricFlowDataTable, *, limit: int) -> DataQueryResult:
    """有界序列化 MetricFlowDataTable，Decimal 以字符串保留精度。"""

    columns = [ResultColumn(name=column.column_name, type=column.column_type.__name__)
               for column in table.column_descriptions]
    rows = [[json_cell(cell) for cell in row] for row in table.rows[:limit]]
    return DataQueryResult(columns=columns, rows=rows, truncated=len(table.rows) > limit)
