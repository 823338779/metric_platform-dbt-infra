"""仅供维度 explain 使用的 SQL 方言配置，不持有连接或磁盘目录。"""
from __future__ import annotations

from dbt_metricflow.cli.dbt_connectors.adapter_backed_client import SupportedAdapterTypes
from metricflow.data_table.mf_table import MetricFlowDataTable
from metricflow.protocols.sql_client import SqlEngine
from metricflow.sql.render.sql_plan_renderer import SqlPlanRenderer
from metricflow_semantics.sql.sql_bind_parameters import SqlBindParameterSet

from dbt_metricflow_service import constants as strings

PLANNING_ONLY_MESSAGE = "dimension discovery cannot execute SQL"


class PlanningSqlClient:
    """保留与实际查询相同的方言，明确禁止计划验证触发数据库执行。"""

    def __init__(self, adapter_type: str) -> None:
        # StarRocks 与现有查询客户端一致，使用 DuckDB 渲染器。
        adapter = SupportedAdapterTypes(
            strings.DUCKDB_ADAPTER if adapter_type == strings.STARROCKS_ADAPTER else adapter_type,
        )
        self.sql_engine_type: SqlEngine = adapter.sql_engine_type
        self.sql_plan_renderer: SqlPlanRenderer = adapter.sql_plan_renderer

    def query(self, stmt: str, sql_bind_parameter_set: SqlBindParameterSet | None = None) -> MetricFlowDataTable:
        raise RuntimeError(PLANNING_ONLY_MESSAGE)

    def execute(self, stmt: str, sql_bind_parameter_set: SqlBindParameterSet | None = None) -> None:
        raise RuntimeError(PLANNING_ONLY_MESSAGE)

    def dry_run(self, stmt: str, sql_bind_parameter_set: SqlBindParameterSet | None = None) -> None:
        raise RuntimeError(PLANNING_ONLY_MESSAGE)

    def close(self) -> None:
        pass

    def render_bind_parameter_key(self, bind_parameter_key: str) -> str:
        raise RuntimeError(PLANNING_ONLY_MESSAGE)
