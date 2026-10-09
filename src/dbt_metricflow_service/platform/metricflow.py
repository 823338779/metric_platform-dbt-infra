from __future__ import annotations

import json
import os
import subprocess
import sys
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

WORKER_MODULE = "dbt_metricflow_service.platform.metricflow"
WORKER_TIMEOUT_SECONDS = 1800
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
    """在隔离 worker 内调用锁定版本 MetricFlow API，不解析 CLI 输出。"""

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


def invoke_programmatic(
    project: Path, profiles: Path, schema: str, target: str,
    input_path: Path, output_path: Path,
) -> dict[str, Any]:
    """子进程隔离 dbt 全局配置和数据库连接，产物通过受控 JSON 文件传递。"""

    environment = {
        **os.environ, "DBT_PROJECT_DIR": str(project), "DBT_PROFILES_DIR": str(profiles),
        "DBT_TARGET_PATH": str(project / "target"), "DBT_PLATFORM_SCHEMA": schema,
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false", "DBT_TARGET": target,
    }
    try:
        subprocess.run(
            [sys.executable, "-m", WORKER_MODULE, str(input_path), str(output_path)],
            cwd=project, env=environment, capture_output=True, timeout=WORKER_TIMEOUT_SECONDS, check=True,
        )
    except (OSError, subprocess.SubprocessError) as error:
        raise ValueError("MetricFlow 程序化查询失败") from error
    result = json.loads(output_path.read_text(encoding="utf-8"))
    if not isinstance(result, dict):
        raise ValueError("MetricFlow 结果格式无效")
    return result


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit(2)
    input_path = Path(sys.argv[1]).resolve()
    output_path = Path(sys.argv[2]).resolve()
    payload = json.loads(input_path.read_text(encoding="utf-8"))
    project = Path(os.environ["DBT_PROJECT_DIR"]).resolve()
    profiles = Path(os.environ["DBT_PROFILES_DIR"]).resolve()
    try:
        result = execute_programmatic(project, profiles, payload)
    except InvalidOptions:
        # 同步选项接口可区分无效参数与可重试的基础设施错误。
        output_path.write_text(json.dumps({"errorCode": INVALID_OPTIONS_CODE}), encoding="utf-8")
        raise SystemExit(2) from None
    output_path.write_text(json.dumps(result, ensure_ascii=False), encoding="utf-8")


if __name__ == "__main__":
    main()
