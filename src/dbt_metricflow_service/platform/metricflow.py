from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Final

from dbt_metricflow.cli.cli_configuration import CLIConfiguration
from metricflow.engine.metricflow_engine import MetricFlowEngine, MetricFlowQueryRequest, MetricFlowQueryType
from metricflow_semantics.naming.linkable_spec_name import StructuredLinkableSpecName

from dbt_metricflow_service import constants as strings
from dbt_metricflow_service.adapters.starrocks import STARROCKS_ADAPTER, StarRocksSqlClient
from dbt_metricflow_service.models.native import NativeManifest
from dbt_metricflow_service.platform.models import (
    DataQueryResult,
    DimensionQueryResult,
    PlatformQueryRequest,
    QueryMode,
)
from dbt_metricflow_service.platform.queries import _json_cell, available_dimensions, serialize_rows, validate_query

from ..models.payloads import JsonValue, ResultColumn

FILTER_VALUE_UNSUPPORTED_MESSAGE: Final[str] = '筛选值类型不支持'
IN_FILTER_LIMIT_MESSAGE: Final[str] = 'IN 筛选值超限'
INVALID_METRICS_DIMENSIONS_MESSAGE: Final[str] = 'invalid metrics or dimensions'
PREVIEW_RESOURCE_UNAVAILABLE_MESSAGE: Final[str] = '预览资源不可查询'
METRICFLOW_RESULT_MISSING_MESSAGE: Final[str] = 'MetricFlow 未返回结果集'

INVALID_DIMENSIONS_CODE = strings.INVALID_QUERY
PATH_SEPARATOR: Final[str] = "__"


class InvalidDimensionsRequest(ValueError):
    """已加载固定目录后确认的可用维度请求错误，不包括连接或初始化故障。"""


def _literal(value: JsonValue | Decimal | date | datetime) -> str:
    """只接受可安全序列化的标量值，SQL 字符串按 SQL 标准转义。"""

    if isinstance(value, bool):
        return strings.TRUE if value else strings.FALSE
    if isinstance(value, (int, float, Decimal)):
        return str(value)
    if isinstance(value, (str, date, datetime)):
        return "'" + str(value).replace("'", "''") + "'"
    raise ValueError(FILTER_VALUE_UNSUPPORTED_MESSAGE)


def _where_constraints(request: PlatformQueryRequest) -> list[str]:
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


def _configuration(project: Path, profiles: Path) -> CLIConfiguration:
    configuration = CLIConfiguration()
    configuration.setup(dbt_profiles_path=profiles, dbt_project_path=project, configure_file_logging=False)
    return configuration


def _engine(configuration: CLIConfiguration) -> MetricFlowEngine:
    # StarRocks 复用 dbt adapter 执行查询；其他 adapter 保持上游 MetricFlow 客户端。
    if configuration.dbt_artifacts.adapter.type() == STARROCKS_ADAPTER:
        return MetricFlowEngine(
            semantic_manifest_lookup=configuration.semantic_manifest_lookup,
            sql_client=StarRocksSqlClient(configuration.dbt_artifacts.adapter),
        )
    return configuration.mf


def execute_available_dimensions(project: Path, profiles: Path, metrics: tuple[str, ...]) -> DimensionQueryResult:
    """读取所选指标的可用维度；仅目录校验错误转换为可公开的业务错误。"""
    engine = _engine(_configuration(project, profiles))
    try:
        return available_dimensions(engine, metrics)
    except ValueError as error:
        raise InvalidDimensionsRequest(INVALID_METRICS_DIMENSIONS_MESSAGE) from error


def execute_programmatic(project: Path, profiles: Path, request: PlatformQueryRequest) -> DataQueryResult:
    """执行类型化数据请求；包含数据行查询、维度值、预览和 explain。"""
    configuration = _configuration(project, profiles)
    engine = _engine(configuration)
    if request.mode is QueryMode.PREVIEW:
        manifest = NativeManifest.model_validate_json(
            (project / strings.TARGET_DIRECTORY / strings.MANIFEST_FILE).read_bytes())
        resource = manifest.nodes.get(request.dataset_resource_id) or manifest.sources.get(request.dataset_resource_id)
        if (resource is None or not resource.relation_name or
                request.dataset_resource_id not in manifest.sources and
                (resource.resource_type not in
                 (strings.DBT_MODEL_RESOURCE, strings.DBT_SEED_RESOURCE, strings.DBT_SNAPSHOT_RESOURCE)
                 or resource.config.materialized == strings.DBT_EPHEMERAL_MATERIALIZATION)):
            raise ValueError(PREVIEW_RESOURCE_UNAVAILABLE_MESSAGE)
        adapter = configuration.dbt_artifacts.adapter
        with adapter.connection_named(strings.PREVIEW_CONNECTION_NAME):
            _response, table = adapter.execute(
                f"select * from {resource.relation_name} limit {request.limit + 1}", fetch=True
            )
        columns = [
            ResultColumn(name=name, type=type(table[0][index]).__name__ if table.rows else strings.UNKNOWN_VALUE_TYPE)
            for index, name in enumerate(table.column_names)
        ]
        rows = [[_json_cell(cell) for cell in row] for row in table.rows[:request.limit]]
        return DataQueryResult(columns=columns, rows=rows, truncated=len(table.rows) > request.limit)
    available = available_dimensions(engine, tuple(request.metrics))
    validate_query(request, available)
    group_by = [request.dimension] if request.mode is QueryMode.DIMENSION_VALUES else request.group_by
    mf_request = MetricFlowQueryRequest.create(
        metric_names=request.metrics,
        group_by_names=group_by,
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
    if request.mode is QueryMode.EXPLAIN:
        explained = engine.explain(mf_request)
        return DataQueryResult(sql=explained.sql_statement.sql)
    result = engine.query(mf_request)
    if result.result_df is None:
        raise ValueError(METRICFLOW_RESULT_MISSING_MESSAGE)
    return serialize_rows(result.result_df, limit=request.limit)
