from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import UUID

from dbt.adapters.factory import get_adapter_by_type
from dbt.config.runtime import RuntimeConfig
from dbt.context.providers import generate_runtime_macro_context
from dbt.parser.manifest import ManifestLoader
from dbt_metricflow.cli.cli_configuration import CLIConfiguration
from metricflow.engine.metricflow_engine import MetricFlowEngine, MetricFlowQueryRequest, MetricFlowQueryType

from dbt_metricflow_service.adapters.starrocks import STARROCKS_ADAPTER, StarRocksSqlClient
from dbt_metricflow_service.platform.catalog import catalog_from_artifacts
from dbt_metricflow_service.platform.models import PlatformQueryRequest, QueryMode
from dbt_metricflow_service.platform.namespace import run_prefix, validate_schema_name
from dbt_metricflow_service.platform.queries import _json_cell, query_options, serialize_rows, validate_query

INVALID_OPTIONS_CODE = "INVALID_QUERY"


class InvalidOptions(ValueError):
    """已加载固定目录后确认的选项请求错误，不包括连接或初始化故障。"""


def cleanup_versioned_relations(adapter: Any, schema: str, run_id: UUID) -> None:
    """只删除固定 schema 中本 run 的关系，允许失败构建缺少 manifest。"""

    validate_schema_name(schema)
    prefix = run_prefix(run_id)
    schema_relation = adapter.Relation.create(schema=schema)
    for relation in adapter.list_relations_without_caching(schema_relation):
        if relation.schema == schema and relation.identifier.startswith(prefix):
            adapter.drop_relation(relation)


def _literal(value: Any) -> str:
    """只接受可安全序列化的标量值，SQL 字符串按 SQL 标准转义。"""

    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float, Decimal)):
        return str(value)
    if isinstance(value, (str, date, datetime)):
        return "'" + str(value).replace("'", "''") + "'"
    raise ValueError("筛选值类型不支持")


def _where_constraints(request: PlatformQueryRequest) -> list[str]:
    constraints = []
    for item in request.filters:
        if item.operator == "IN":
            if not isinstance(item.value, list) or not 1 <= len(item.value) <= 100:
                raise ValueError("IN 筛选值超限")
            literal = "(" + ", ".join(_literal(value) for value in item.value) + ")"
        else:
            literal = _literal(item.value)
        constraints.append(f"{{{{ Dimension('{item.field}') }}}} {item.operator} {literal}")
    return constraints


def execute_programmatic(project: Path, profiles: Path, input_data: dict[str, Any]) -> dict[str, Any]:
    """在隔离或串行化的执行上下文中调用锁定版本 MetricFlow API。"""

    configuration = CLIConfiguration()
    configuration.setup(dbt_profiles_path=profiles, dbt_project_path=project, configure_file_logging=False)
    if input_data["mode"] == "CLEANUP":
        schema = input_data["schema"]
        run_id = UUID(input_data["runId"]) if "runId" in input_data else None
        if run_id is None and (not isinstance(schema, str) or not schema.startswith("run_")):
            raise ValueError("清理 schema 无效")
        if run_id is not None and input_data.get("tablePrefix") != run_prefix(run_id):
            raise ValueError("清理表前缀无效")
        # 清理只依赖项目连接配置；失败构建可能尚未产生 semantic manifest。
        metadata = configuration.dbt_project_metadata
        adapter = get_adapter_by_type(metadata.profile.credentials.type)
        if adapter.get_macro_resolver() is None:
            metadata = configuration.dbt_project_metadata
            runtime = RuntimeConfig.from_parts(
                metadata.project, metadata.profile, SimpleNamespace(vars={})
            )
            adapter.config = runtime
            macros = ManifestLoader.load_macros(
                runtime, adapter.connections.set_query_header, base_macros_only=True
            )
            adapter.set_macro_resolver(macros)
            adapter.set_macro_context_generator(generate_runtime_macro_context)
        with adapter.connection_named("platform_cleanup"):
            if run_id is None:
                adapter.drop_schema(adapter.Relation.create(schema=schema))
            else:
                cleanup_versioned_relations(adapter, schema, run_id)
        return {"cleaned": True}
    # StarRocks 复用 dbt adapter 执行查询；其他 adapter 保持上游 MetricFlow 客户端。
    if configuration.dbt_artifacts.adapter.type() == STARROCKS_ADAPTER:
        engine = MetricFlowEngine(
            semantic_manifest_lookup=configuration.semantic_manifest_lookup,
            sql_client=StarRocksSqlClient(configuration.dbt_artifacts.adapter),
        )
    else:
        engine = configuration.mf
    mode = input_data["mode"]
    if mode == "PROBE":
        metrics = engine.list_metrics(include_dimensions=False)
        if not metrics:
            raise ValueError("MetricFlow 没有可查询指标")
        result = engine.query(MetricFlowQueryRequest.create(metric_names=[metrics[0].name], limit=1))
        return {"queryCapability": result.result_df is not None}
    if mode == "OPTIONS":
        try:
            return query_options(engine, tuple(input_data["metrics"]))
        except ValueError as error:
            raise InvalidOptions("invalid metrics or dimensions") from error
    request = PlatformQueryRequest.model_validate(input_data["request"])
    if request.mode is QueryMode.PREVIEW:
        catalog = catalog_from_artifacts(project / "target")
        resource = next(
            (item for item in catalog["resources"] if item["resourceId"] == request.dataset_resource_id),
            None,
        )
        if resource is None or resource["kind"] not in {"TABLE", "VIEW"}:
            raise ValueError("预览资源不可查询")
        adapter = configuration.dbt_artifacts.adapter
        with adapter.connection_named("platform_preview"):
            _response, table = adapter.execute(
                f"select * from {resource['relation']} limit {request.limit + 1}", fetch=True
            )
        columns = [{"name": name, "type": type(table[0][index]).__name__ if table.rows else "unknown"}
                   for index, name in enumerate(table.column_names)]
        rows = [[_json_cell(cell) for cell in row] for row in table.rows[:request.limit]]
        return {"columns": columns, "rows": rows, "truncated": len(table.rows) > request.limit}
    options = query_options(engine, tuple(request.metrics))
    validate_query(request, options)
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
        return {"columns": [], "rows": [], "sql": explained.sql_statement.sql, "truncated": False}
    result = engine.query(mf_request)
    if result.result_df is None:
        raise ValueError("MetricFlow 未返回结果集")
    return serialize_rows(result.result_df, limit=request.limit)
