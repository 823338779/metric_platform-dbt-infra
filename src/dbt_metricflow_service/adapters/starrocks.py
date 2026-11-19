from __future__ import annotations

from typing import Final

from dbt.adapters.base import BaseAdapter
from dbt_metricflow.cli.dbt_connectors.adapter_backed_client import AdapterBackedSqlClient
from metricflow.protocols.sql_client import SqlEngine
from metricflow.sql.render.duckdb_renderer import DuckDbSqlExpressionRenderer, DuckDbSqlPlanRenderer
from metricflow.sql.render.expr_renderer import SqlExpressionRenderResult
from metricflow_semantics.sql.sql_bind_parameters import SqlBindParameterSet
from metricflow_semantics.sql.sql_exprs import SqlGenerateUuidExpression

from dbt_metricflow_service import constants as strings

STARROCKS_ADAPTER_REQUIRED_MESSAGE: Final[str] = '此客户端只接受 StarRocks adapter'

STARROCKS_ADAPTER = strings.STARROCKS_ADAPTER
STARROCKS_UUID_SQL: Final[str] = 'UUID()'


class StarRocksExpressionRenderer(DuckDbSqlExpressionRenderer):
    """漏斗的购买事件去重需要 StarRocks 的原生 UUID 函数。"""

    def visit_generate_uuid_expr(self, node: SqlGenerateUuidExpression) -> SqlExpressionRenderResult:
        return SqlExpressionRenderResult(sql=STARROCKS_UUID_SQL, bind_parameter_set=SqlBindParameterSet())


class StarRocksPlanRenderer(DuckDbSqlPlanRenderer):
    EXPR_RENDERER = StarRocksExpressionRenderer()


class StarRocksSqlClient(AdapterBackedSqlClient):
    """通过 dbt-starrocks 执行 MetricFlow 的标准聚合 SQL。"""

    def __init__(self, adapter: BaseAdapter) -> None:
        # MetricFlow 0.213.0 没有 StarRocks 枚举；DuckDB 渲染器用于本项目验证过的聚合与时间分组。
        if adapter.type() != STARROCKS_ADAPTER:
            raise ValueError(STARROCKS_ADAPTER_REQUIRED_MESSAGE)
        self._adapter = adapter
        self._sql_engine_type = SqlEngine.DUCKDB
        self._sql_plan_renderer = StarRocksPlanRenderer()
