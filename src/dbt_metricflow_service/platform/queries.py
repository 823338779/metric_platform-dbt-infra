from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Final

from metricflow.data_table.mf_table import MetricFlowDataTable
from metricflow.engine.metricflow_engine import MetricFlowEngine, MetricFlowQueryRequest
from metricflow_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from metricflow_semantics.errors.error_classes import InvalidQueryException
from pydantic import TypeAdapter

from dbt_metricflow_service import constants as strings
from dbt_metricflow_service.platform.models import (
    EngineResult,
    NativeDimension,
    NativeMetric,
    NativeTimeDimension,
    PlatformQueryRequest,
    QueryMode,
)

from ..models.payloads import JsonValue, ResultColumn

METRIC_SELECTION_INVALID_MESSAGE: Final[str] = '指标不存在或为空'
METRICS_NOT_QUERYABLE_MESSAGE: Final[str] = '指标不可查询'
GROUP_DIMENSIONS_INCOMPATIBLE_MESSAGE: Final[str] = '分组维度不可组合'
ORDER_FIELDS_INCOMPATIBLE_MESSAGE: Final[str] = '排序字段不可组合'
DIMENSION_VALUES_UNAVAILABLE_MESSAGE: Final[str] = '维度值字段不可查询'
FILTER_NOT_ALLOWED_MESSAGE: Final[str] = '筛选条件不在允许范围内'
INVALID_DATE_RANGE_MESSAGE: Final[str] = '日期范围无效'

ALLOWED_OPERATORS = frozenset({"=", "!=", ">", ">=", "<", "<=", strings.IN})
TIME_TYPE = strings.TIME_DIMENSION_TYPE
METRIC_TIME = strings.METRIC_TIME
PATH_SEPARATOR = "__"
JSON_VALUE_ADAPTER = TypeAdapter(JsonValue)


def _verified_grains(engine: MetricFlowEngine, metrics: tuple[str, ...], base: str) -> dict[str, str]:
    # 对当前指标集合逐一 explain，不能把某个日期字段的能力套到其他 join 路径。
    tokens = {}
    for granularity in TimeGranularity:
        token = PATH_SEPARATOR.join((base, granularity.value))
        request = MetricFlowQueryRequest.create(metric_names=list(metrics), group_by_names=[token])
        try:
            engine.explain(request)
        except (InvalidQueryException, ValueError):
            continue
        tokens[granularity.value] = token
    return tokens


def available_dimensions(engine: MetricFlowEngine, metrics: tuple[str, ...]) -> EngineResult:
    """按 MetricFlow 原生语义返回多指标可组合维度与经 explain 核实的时间 token。"""

    available = {metric.name for metric in engine.list_metrics(include_dimensions=False)}
    if not metrics or not set(metrics) <= available:
        raise ValueError(METRIC_SELECTION_INVALID_MESSAGE)
    dimensions = [NativeDimension(
        token=dimension.dunder_name, name=dimension.name,
        type=getattr(dimension.type, strings.VALUE_FIELD, str(dimension.type)),
    ) for dimension in engine.list_dimensions(metric_names=list(metrics))]
    verified = {METRIC_TIME: _verified_grains(engine, metrics, METRIC_TIME)}
    for dimension in dimensions:
        if dimension.type != TIME_TYPE:
            continue
        base = dimension.token
        prefix, _, suffix = base.rpartition(PATH_SEPARATOR)
        # 原生 metric_time 带默认粒度；字段自身名为 day 时不能误删字段名。
        if suffix != dimension.name and suffix in {grain.value for grain in TimeGranularity}:
            base = prefix
        if base not in verified:
            verified[base] = _verified_grains(engine, metrics, base)
        dimension.grain_tokens = verified[base]
    time_dimensions = [NativeTimeDimension(token=token, granularity=grain)
                       for grain, token in verified[METRIC_TIME].items()]
    return EngineResult(metrics=[NativeMetric(name=name) for name in metrics],
                        dimensions=dimensions, time_dimensions=time_dimensions,
                        allowed_filters=sorted(ALLOWED_OPERATORS))



def validate_query(request: PlatformQueryRequest, available: EngineResult) -> None:
    """所有查询字段先与该固定 run 的原生可用项比对。"""

    metrics = {item.name for item in available.metrics}
    dimensions = {item.token for item in available.dimensions}
    time_dimensions = {item.token for item in available.time_dimensions}
    # 命名日期维度的粒度保留在对应选项上，验证时纳入允许的原生 token。
    time_dimensions.update(
        token for item in available.dimensions for token in item.grain_tokens.values()
    )
    if not request.metrics or not set(request.metrics) <= metrics:
        raise ValueError(METRICS_NOT_QUERYABLE_MESSAGE)
    if not set(request.group_by) <= dimensions | time_dimensions:
        raise ValueError(GROUP_DIMENSIONS_INCOMPATIBLE_MESSAGE)
    orderable = metrics | dimensions | time_dimensions
    if any(item.lstrip("-") not in orderable for item in request.order_by):
        raise ValueError(ORDER_FIELDS_INCOMPATIBLE_MESSAGE)
    if request.mode is QueryMode.DIMENSION_VALUES and request.dimension not in dimensions | time_dimensions:
        raise ValueError(DIMENSION_VALUES_UNAVAILABLE_MESSAGE)
    if any(item.field not in dimensions or item.operator not in ALLOWED_OPERATORS for item in request.filters):
        raise ValueError(FILTER_NOT_ALLOWED_MESSAGE)
    if request.start_time and request.end_time and request.start_time > request.end_time:
        raise ValueError(INVALID_DATE_RANGE_MESSAGE)


def _json_cell(value: JsonValue | Decimal | date | datetime | bytes) -> JsonValue:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return JSON_VALUE_ADAPTER.validate_python(value)


def serialize_rows(table: MetricFlowDataTable, *, limit: int) -> EngineResult:
    """有界序列化 MetricFlowDataTable，Decimal 以字符串保留精度。"""

    columns = [ResultColumn(name=column.column_name, type=column.column_type.__name__)
               for column in table.column_descriptions]
    rows = [[_json_cell(cell) for cell in row] for row in table.rows[:limit]]
    return EngineResult(columns=columns, rows=rows, truncated=len(table.rows) > limit)
