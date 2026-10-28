"""四种同步查询模式的公开契约；模式无关字段由类型边界拒绝。"""

from __future__ import annotations

import math
from datetime import datetime
from enum import StrEnum
from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import Field, JsonValue, field_validator, model_validator

from dbt_metricflow_service import constants as strings

from .contracts import Contract


class QueryMode(StrEnum):
    QUERY = strings.QUERY
    EXPLAIN = strings.EXPLAIN
    PREVIEW = strings.PREVIEW
    DIMENSION_VALUES = strings.DIMENSION_VALUES


FilterOperator = Literal["=", "!=", ">", ">=", "<", "<=", "IN"]


class SelectedDimension(Contract):
    option_id: str = Field(description="固定构建及指标集合的维度选项")
    grain: str | None = Field(default=None, description="引擎支持的时间粒度")


class BoundDimension(Contract):
    name: str = Field(pattern=r"^[a-z][a-z0-9_]{0,62}$", description="对齐后的公共输出列名")
    bindings: dict[str, SelectedDimension] = Field(min_length=1, max_length=100,
                                                  description="指标名到该单指标维度选项的绑定")


class SelectedFilter(Contract):
    option_id: str
    operator: FilterOperator
    value: JsonValue

    @model_validator(mode="after")
    def validate_value(self) -> Self:
        if self.operator == "IN":
            if not isinstance(self.value, list) or not 1 <= len(self.value) <= 100:
                raise ValueError("IN requires 1 to 100 scalar values")
            values = self.value
        else:
            values = [self.value]
        for value in values:
            if not isinstance(value, (str, int, float, bool)) or (
                isinstance(value, float) and not math.isfinite(value)
            ):
                raise ValueError("filter values must be finite JSON scalars")
        return self


class SelectedOrder(Contract):
    field_id: str
    direction: Literal["ASC", "DESC"]


class AvailableDimensionsRequest(Contract):
    metrics: list[str] = Field(min_length=1, max_length=100)


class MetricSelection(AvailableDimensionsRequest):
    filters: list[SelectedFilter] = Field(default_factory=list, max_length=100)
    start_time: datetime | None = None
    end_time: datetime | None = None
    limit: int = Field(default=1000, ge=1, le=10000)


class MetricQueryRequest(MetricSelection):
    mode: Literal[QueryMode.QUERY, QueryMode.EXPLAIN]
    group_by: list[SelectedDimension | BoundDimension] = Field(default_factory=list, max_length=100)
    order_by: list[SelectedOrder] = Field(default_factory=list, max_length=100)

    @model_validator(mode="after")
    def validate_bindings(self) -> Self:
        names = set(self.metrics)
        for group in self.group_by:
            if not isinstance(group, BoundDimension):
                continue
            if set(group.bindings) != set(self.metrics):
                raise ValueError("bindings must cover exactly the selected metrics")
            if group.name in names:
                raise ValueError("bound dimension names must be unique and must not match metrics")
            names.add(group.name)
        return self


class DimensionValuesRequest(MetricSelection):
    mode: Literal[QueryMode.DIMENSION_VALUES]
    dimension_option_id: str = Field(min_length=1)


class PreviewRequest(Contract):
    mode: Literal[QueryMode.PREVIEW]
    selector: str = Field(min_length=1, description="dbt 原生 selector，匹配一个非 seed 节点")
    limit: int = Field(default=1000, ge=1, le=10000)

    @field_validator("selector")
    @classmethod
    def nonblank_selector(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("selector must not be blank")
        return value


QueryRequest = Annotated[MetricQueryRequest | DimensionValuesRequest | PreviewRequest, Field(discriminator="mode")]


class DimensionOption(Contract):
    option_id: str
    name: str | None = None
    semantic_model_name: str | None = None
    display_name: str
    dimension_type: str
    granularities: list[str]
    operators: list[str]


class AvailableDimensionsView(Contract):
    build_id: UUID
    metrics: list[str]
    dimensions: list[DimensionOption]


class ResultColumn(Contract):
    name: str
    type: str


class NormalizedTimeRange(Contract):
    start_time: str | None
    end_time: str | None


class QueryIdentity(Contract):
    build_id: UUID
    commit_sha: str | None


class TabularResult(QueryIdentity):
    columns: list[ResultColumn]
    rows: list[list[JsonValue]]
    result_truncated: bool


class PreviewResult(TabularResult):
    mode: Literal[QueryMode.PREVIEW] = QueryMode.PREVIEW


class MetricQueryResult(TabularResult):
    mode: Literal[QueryMode.QUERY, QueryMode.DIMENSION_VALUES]
    business_timezone: str
    normalized_time_range: NormalizedTimeRange


class ExplainResult(QueryIdentity):
    mode: Literal[QueryMode.EXPLAIN] = QueryMode.EXPLAIN
    business_timezone: str
    normalized_time_range: NormalizedTimeRange
    sql: str


QueryResult = Annotated[MetricQueryResult | ExplainResult | PreviewResult, Field(discriminator="mode")]
