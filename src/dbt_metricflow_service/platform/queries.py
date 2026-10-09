from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from metricflow.engine.metricflow_engine import MetricFlowQueryRequest
from metricflow_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from metricflow_semantics.errors.error_classes import InvalidQueryException

from dbt_metricflow_service.platform.models import PlatformQueryRequest, QueryMode

ALLOWED_OPERATORS = frozenset({"=", "!=", ">", ">=", "<", "<=", "IN"})


def query_options(engine: Any, metrics: tuple[str, ...]) -> dict[str, Any]:
    """按 MetricFlow 原生语义返回多指标可组合维度与经 explain 核实的时间 token。"""

    available = {metric.name for metric in engine.list_metrics(include_dimensions=False)}
    if not metrics or not set(metrics) <= available:
        raise ValueError("指标不存在或为空")
    dimensions = [{
        "token": dimension.dunder_name, "name": dimension.name,
        "type": getattr(dimension.type, "value", str(dimension.type)),
    } for dimension in engine.list_dimensions(metric_names=list(metrics))]
    time_dimensions = []
    for granularity in TimeGranularity:
        token = f"metric_time__{granularity.value}"
        request = MetricFlowQueryRequest.create(metric_names=list(metrics), group_by_names=[token])
        try:
            engine.explain(request)
        except (InvalidQueryException, ValueError):
            continue
        time_dimensions.append({"token": token, "granularity": granularity.value})
    return {
        "metrics": [{"name": name} for name in metrics],
        "dimensions": dimensions, "timeDimensions": time_dimensions,
        "allowedFilters": sorted(ALLOWED_OPERATORS),
    }


def validate_query(request: PlatformQueryRequest, options: dict[str, Any]) -> None:
    """所有查询字段先与该固定 run 的原生可用项比对。"""

    metrics = {item["name"] for item in options["metrics"]}
    dimensions = {item["token"] for item in options["dimensions"]}
    time_dimensions = {item["token"] for item in options["timeDimensions"]}
    if not request.metrics or not set(request.metrics) <= metrics:
        raise ValueError("指标不可查询")
    if not set(request.group_by) <= dimensions | time_dimensions:
        raise ValueError("分组维度不可组合")
    orderable = metrics | dimensions | time_dimensions
    if any(item.lstrip("-") not in orderable for item in request.order_by):
        raise ValueError("排序字段不可组合")
    if request.mode is QueryMode.DIMENSION_VALUES and request.dimension not in dimensions | time_dimensions:
        raise ValueError("维度值字段不可查询")
    if any(item.field not in dimensions or item.operator not in ALLOWED_OPERATORS for item in request.filters):
        raise ValueError("筛选条件不在允许范围内")
    if request.start_time and request.end_time and request.start_time > request.end_time:
        raise ValueError("日期范围无效")


def _json_cell(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def serialize_rows(table: Any, *, limit: int) -> dict[str, Any]:
    """有界序列化 MetricFlowDataTable，Decimal 以字符串保留精度。"""

    columns = [{
        "name": column.column_name, "type": column.column_type.__name__,
    } for column in table.column_descriptions]
    rows = [[_json_cell(cell) for cell in row] for row in table.rows[:limit]]
    return {"columns": columns, "rows": rows, "truncated": len(table.rows) > limit}


