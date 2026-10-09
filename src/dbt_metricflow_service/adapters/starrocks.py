from __future__ import annotations

from dbt.adapters.base import BaseAdapter
from dbt_metricflow.cli.dbt_connectors.adapter_backed_client import AdapterBackedSqlClient
from metricflow.protocols.sql_client import SqlEngine
from metricflow.sql.render.duckdb_renderer import DuckDbSqlPlanRenderer

STARROCKS_ADAPTER = "starrocks"


class StarRocksSqlClient(AdapterBackedSqlClient):
    """通过 dbt-starrocks 执行 MetricFlow 的标准聚合 SQL。"""

    def __init__(self, adapter: BaseAdapter) -> None:
        # MetricFlow 0.213.0 没有 StarRocks 枚举；DuckDB 渲染器用于本项目验证过的聚合与时间分组。
        if adapter.type() != STARROCKS_ADAPTER:
            raise ValueError("此客户端只接受 StarRocks adapter")
        self._adapter = adapter
        self._sql_engine_type = SqlEngine.DUCKDB
        self._sql_plan_renderer = DuckDbSqlPlanRenderer()
