from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from functools import cached_property
from pathlib import Path
from typing import Final

from dbt_metricflow.cli.cli_configuration import CLIConfiguration
from metricflow.engine.metricflow_engine import MetricFlowEngine, MetricFlowQueryRequest, MetricFlowQueryType
from metricflow_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from metricflow_semantics.errors.error_classes import InformativeUserError, InvalidQueryException
from metricflow_semantics.naming.linkable_spec_name import StructuredLinkableSpecName
from pydantic import JsonValue

from dbt_metricflow_service import constants as strings
from dbt_metricflow_service.adapters.starrocks import STARROCKS_ADAPTER, StarRocksSqlClient
from dbt_metricflow_service.engines import InvalidDimensionsRequest
from dbt_metricflow_service.engines.dbt import DbtClient
from dbt_metricflow_service.engines.models import (
    CompiledQuery,
    DataQueryResult,
    DimensionQueryResult,
    NativeDimension,
    NativeTimeDimension,
    QueryMode,
)

from .results import serialize_rows

FILTER_VALUE_UNSUPPORTED_MESSAGE: Final[str] = '筛选值类型不支持'
IN_FILTER_LIMIT_MESSAGE: Final[str] = 'IN 筛选值超限'
INVALID_METRICS_DIMENSIONS_MESSAGE: Final[str] = 'invalid metrics or dimensions'
METRICFLOW_RESULT_MISSING_MESSAGE: Final[str] = 'MetricFlow 未返回结果集'

PATH_SEPARATOR: Final[str] = "__"


def _literal(value: JsonValue | Decimal | date | datetime) -> str:
    """只接受可安全序列化的标量值，SQL 字符串按 SQL 标准转义。"""

    if isinstance(value, bool):
        return strings.TRUE if value else strings.FALSE
    if isinstance(value, (int, float, Decimal)):
        return str(value)
    if isinstance(value, (str, date, datetime)):
        return "'" + str(value).replace("'", "''") + "'"
    raise ValueError(FILTER_VALUE_UNSUPPORTED_MESSAGE)


def _where_constraints(request: CompiledQuery) -> list[str]:
    constraints = []
    for item in request.filters:
        if item.operator == strings.IN:
            if not isinstance(item.value, list) or not 1 <= len(item.value) <= 100:
                raise ValueError(IN_FILTER_LIMIT_MESSAGE)
            literal = "(" + ", ".join(_literal(value) for value in item.value) + ")"
        else:
            literal = _literal(item.value)
        # 分组 token 可含完整路径，筛选须将前置角色放入 entity_path；原生解析保留时间粒度后缀。
        links = StructuredLinkableSpecName.from_name(item.field, custom_granularity_names=()).entity_link_names
        dimension = f"Dimension('{item.field}')"
        if len(links) > 1:
            local_name = item.field.split(PATH_SEPARATOR, len(links) - 1)[-1]
            dimension = f"Dimension('{local_name}', entity_path={list(links[:-1])!r})"
        constraints.append(f"{{{{ {dimension} }}}} {item.operator} {literal}")
    return constraints


class MetricFlowClient:
    def __init__(self, manifest: bytes, *, project: Path, profiles: Path) -> None:
        self._manifest = manifest
        self._project = project
        self._profiles = profiles

    @cached_property
    def _engine(self) -> MetricFlowEngine:
        DbtClient(self._manifest).write_semantic_manifest(self._project / strings.SEMANTIC_MANIFEST_PATH)
        configuration = CLIConfiguration()
        configuration.setup(dbt_profiles_path=self._profiles, dbt_project_path=self._project,
                            configure_file_logging=False)
        # StarRocks 复用 dbt adapter 执行查询；其他 adapter 保持上游 MetricFlow 客户端。
        if configuration.dbt_artifacts.adapter.type() == STARROCKS_ADAPTER:
            return MetricFlowEngine(
                semantic_manifest_lookup=configuration.semantic_manifest_lookup,
                sql_client=StarRocksSqlClient(configuration.dbt_artifacts.adapter),
            )
        return configuration.mf

    def available_dimensions(self, metrics: tuple[str, ...]) -> DimensionQueryResult:
        """读取所选指标的可用维度；仅目录校验错误转换为可公开的业务错误。"""
        engine = self._engine
        try:
            return available_dimensions(engine, metrics)
        except ValueError as error:
            raise InvalidDimensionsRequest(INVALID_METRICS_DIMENSIONS_MESSAGE) from error

    def query(self, request: CompiledQuery) -> DataQueryResult:
        engine = self._engine
        mf_request = MetricFlowQueryRequest.create(
            metric_names=request.metrics,
            group_by_names=request.group_by,
            limit=request.limit + 1,
            time_constraint_start=request.start_time,
            time_constraint_end=request.end_time,
            where_constraints=_where_constraints(request),
            order_by_names=request.order_by,
            query_type=(
                MetricFlowQueryType.DIMENSION_VALUES
                if request.mode is QueryMode.DIMENSION_VALUES else MetricFlowQueryType.METRIC
            ),
        )
        try:
            if request.mode is QueryMode.EXPLAIN:
                explained = engine.explain(mf_request)
                return DataQueryResult(sql=explained.sql_statement.sql)
            result = engine.query(mf_request)
        except InformativeUserError as error:
            raise InvalidDimensionsRequest(INVALID_METRICS_DIMENSIONS_MESSAGE) from error
        if result.result_df is None:
            raise ValueError(METRICFLOW_RESULT_MISSING_MESSAGE)
        return serialize_rows(result.result_df, limit=request.limit)


METRIC_SELECTION_INVALID_MESSAGE: Final[str] = '指标不存在或为空'

ALLOWED_OPERATORS = frozenset({"=", "!=", ">", ">=", "<", "<=", strings.IN})
TIME_TYPE = strings.TIME_DIMENSION_TYPE
METRIC_TIME = strings.METRIC_TIME
PATH_SEPARATOR = "__"


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
        dimensions=dimensions,
        time_dimensions=time_dimensions, allowed_filters=sorted(ALLOWED_OPERATORS),
    )


