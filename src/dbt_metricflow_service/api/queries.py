"""实时维度与同步查询 HTTP 边界；结果仅在本次响应中返回。"""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter

from dbt_metricflow_service import constants as strings
from dbt_metricflow_service.application.queries import QueryService

from ..models.queries import AvailableDimensionsRequest, AvailableDimensionsView, QueryRequest, QueryResult


def router(service: QueryService) -> APIRouter:
    routes = APIRouter(prefix=strings.ROUTE_QUERY)

    @routes.post(strings.ROUTE_AVAILABLE_DIMENSIONS, response_model=AvailableDimensionsView,
                 summary="发现指标组合的可用维度")
    def available_dimensions(
        request: AvailableDimensionsRequest, build_id: UUID,
    ) -> AvailableDimensionsView:
        """在 URL 中指定 `build_id`，在请求体的 `metrics` 中传入指标名称列表。
        返回该组合可用的维度、时间粒度及过滤运算符。后续查询使用相同构建和指标组合的 `optionId` 指定维度。
        """
        return service.available_dimensions(build_id, request)

    @routes.post("", response_model=QueryResult, summary="执行指标查询、生成 SQL 或预览数据")
    def submit(
        request: QueryRequest, build_id: UUID,
    ) -> QueryResult:
        """在 URL 中指定 `build_id`，按请求体的 `mode` 同步返回本次结果。

        - `QUERY` 按指标名称计算结果，支持维度分组、过滤、时间范围和排序。
        - `EXPLAIN` 根据同样的指标查询条件生成 SQL，供检查查询逻辑。
        - `PREVIEW` 用 dbt `selector` 选中一个非 seed 节点，返回样例行。
        - `DIMENSION_VALUES` 按指标和 `dimensionOptionId` 查询该维度的取值。

        表格结果包含 `columns`、`rows` 和截断标志 `resultTruncated`。指标查询的时间范围按构建业务时区解释。
        """
        return service.submit(build_id, request)

    return routes
