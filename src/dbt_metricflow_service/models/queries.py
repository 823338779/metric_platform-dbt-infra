"""版本固定的同步查询与选项协议；原生语义由引擎执行。"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Final, Literal, Self
from uuid import UUID

from pydantic import Field, JsonValue, model_validator

from dbt_metricflow_service import constants as strings

from .builds import Contract
from .payloads import DimensionOption, NormalizedTimeRange, ResultColumn

INVALID_PREVIEW_FIELDS_MESSAGE: Final[str] = 'invalid PREVIEW fields'
METRICS_REQUIRED_MESSAGE: Final[str] = 'metrics are required and datasetResourceId is not allowed'
DIMENSION_OPTION_REQUIRED_MESSAGE: Final[str] = 'dimensionOptionId is required'
DIMENSION_OPTION_MODE_MESSAGE: Final[str] = 'dimensionOptionId only belongs to DIMENSION_VALUES'

class QueryMode(StrEnum):
    QUERY = strings.QUERY
    EXPLAIN = strings.EXPLAIN
    PREVIEW = strings.PREVIEW
    DIMENSION_VALUES = strings.DIMENSION_VALUES


class SelectedDimension(Contract):
    option_id: str = Field(description="版本及指标集合固定的选项")
    grain: str | None = Field(default=None, description="引擎支持的时间粒度")


class SelectedFilter(Contract):
    option_id: str = Field(description="过滤选项身份")
    operator: str = Field(description="选项允许的过滤操作")
    value: JsonValue = Field(description="保留 JSON 类型的过滤值")


class SelectedOrder(Contract):
    field_id: str = Field(description="已选择的指标或分组选项")
    direction: Literal["ASC", "DESC"] = Field(description="排序方向")


class AvailableDimensionsRequest(Contract):
    metric_resource_ids: list[str] = Field(min_length=1, max_length=100, description="指标资源集合")


class QueryRequest(Contract):
    mode: QueryMode = Field(description="受控的引擎查询模式")
    metric_resource_ids: list[str] = Field(default_factory=list, max_length=100, description="指标资源集合")
    group_by: list[SelectedDimension] = Field(default_factory=list, max_length=100, description="有序分组")
    filters: list[SelectedFilter] = Field(default_factory=list, max_length=100, description="类型化过滤条件")
    start_time: datetime | None = Field(default=None, description="起点，按构建业务时区解释")
    end_time: datetime | None = Field(default=None, description="终点，按构建业务时区解释")
    order_by: list[SelectedOrder] = Field(default_factory=list, max_length=100, description="有序排序条件")
    limit: int = Field(default=1000, ge=1, le=10000, description="最多返回的结果行数")
    dataset_resource_id: str | None = Field(default=None, description="PREVIEW 的数据集身份")
    dimension_option_id: str | None = Field(default=None, description="DIMENSION_VALUES 的维度选项")

    @model_validator(mode=strings.VALIDATOR_AFTER)
    def mode_fields(self) -> Self:
        # 先拒绝引擎会忽略的模式字段，选项归属在应用层查固定目录确认。
        if self.mode == QueryMode.PREVIEW:
            if (
                not self.dataset_resource_id
                or self.metric_resource_ids
                or self.group_by
                or self.filters
                or self.order_by
                or self.start_time
                or self.end_time
                or self.dimension_option_id
            ):
                raise ValueError(INVALID_PREVIEW_FIELDS_MESSAGE)
        elif not self.metric_resource_ids or self.dataset_resource_id:
            raise ValueError(METRICS_REQUIRED_MESSAGE)
        if self.mode == QueryMode.DIMENSION_VALUES:
            if not self.dimension_option_id:
                raise ValueError(DIMENSION_OPTION_REQUIRED_MESSAGE)
        elif self.dimension_option_id:
            raise ValueError(DIMENSION_OPTION_MODE_MESSAGE)
        return self


class AvailableDimensionsView(Contract):
    build_id: UUID = Field(description="固定构建身份")
    metric_resource_ids: list[str] = Field(description="规范化指标集合")
    dimensions: list[DimensionOption] = Field(description="当前构建及所选指标集合可用的维度")


class QueryResult(Contract):
    build_id: UUID = Field(description="固定构建身份")
    mode: QueryMode = Field(description="查询模式")
    commit_sha: str | None = Field(description="固定源码版本")
    business_timezone: str = Field(description="构建固定的业务时区")
    normalized_time_range: NormalizedTimeRange = Field(description="归一化时间输入")
    boundary_policy: str = Field(default=strings.QUERY_TIME_BOUNDARY_POLICY, description="引擎粒度边界策略")
    sql: str | None = Field(default=None, description="EXPLAIN 返回的完整编译 SQL")
    columns: list[ResultColumn] = Field(description="结果列及精度类型")
    rows: list[list[JsonValue]] = Field(description="本次实时查询的结果正文")
    result_truncated: bool = Field(description="引擎结果是否达到行数限制并截断")
