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
from dbt_metricflow_service.models.payloads import JsonValue, ResultColumn
from dbt_metricflow_service.platform.models import (
    DataQueryResult,
    DimensionQueryResult,
    NativeDimension,
    NativeMetric,
    NativeTimeDimension,
)

METRIC_SELECTION_INVALID_MESSAGE: Final[str] = '指标不存在或为空'

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


def available_dimensions(engine: MetricFlowEngine, metrics: tuple[str, ...]) -> DimensionQueryResult:
    """按 MetricFlow 原生语义返回多指标可组合维度与经 explain 核实的时间 token。"""

    available = {metric.name for metric in engine.list_metrics(include_dimensions=False)}
    if not metrics or not set(metrics) <= available:
        raise ValueError(METRIC_SELECTION_INVALID_MESSAGE)
    dimensions = [NativeDimension(
        token=dimension.dunder_name, name=dimension.name,
        semantic_model_name=(
            dimension.semantic_model_reference.semantic_model_name
            if dimension.semantic_model_reference is not None else None
        ),
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
    return DimensionQueryResult(
        metrics=[NativeMetric(name=name) for name in metrics], dimensions=dimensions,
        time_dimensions=time_dimensions, allowed_filters=sorted(ALLOWED_OPERATORS),
    )


def _json_cell(value: JsonValue | Decimal | date | datetime | bytes) -> JsonValue:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return JSON_VALUE_ADAPTER.validate_python(value)


def serialize_rows(table: MetricFlowDataTable, *, limit: int) -> DataQueryResult:
    """有界序列化 MetricFlowDataTable，Decimal 以字符串保留精度。"""

    columns = [ResultColumn(name=column.column_name, type=column.column_type.__name__)
               for column in table.column_descriptions]
    rows = [[_json_cell(cell) for cell in row] for row in table.rows[:limit]]
    return DataQueryResult(columns=columns, rows=rows, truncated=len(table.rows) > limit)
