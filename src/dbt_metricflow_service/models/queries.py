"""版本固定的查询与异步选项协议；原生语义由引擎执行。"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any, Literal, Self
from uuid import UUID

from pydantic import Field, model_validator

from .builds import BuildStatus, Contract, ErrorView


class QueryMode(StrEnum):
    QUERY = "QUERY"
    EXPLAIN = "EXPLAIN"
    PREVIEW = "PREVIEW"
    DIMENSION_VALUES = "DIMENSION_VALUES"


class SelectedDimension(Contract):
    option_id: str = Field(description="版本及指标集合固定的选项")
    grain: str | None = Field(default=None, description="引擎支持的时间粒度")


class SelectedFilter(Contract):
    option_id: str = Field(description="过滤选项身份")
    operator: str = Field(description="选项允许的过滤操作")
    value: Any = Field(description="保留 JSON 类型的过滤值")


class SelectedOrder(Contract):
    field_id: str = Field(description="已选择的指标或分组选项")
    direction: Literal["ASC", "DESC"] = Field(description="排序方向")


class OptionsRequest(Contract):
    idempotency_key: str = Field(min_length=1, max_length=256, description="异步选项请求键")
    metric_resource_ids: list[str] = Field(min_length=1, max_length=100, description="指标资源集合")


class QueryRequest(Contract):
    idempotency_key: str = Field(min_length=1, max_length=256, description="本次查询的幂等键")
    mode: QueryMode = Field(description="受控的引擎查询模式")
    metric_resource_ids: list[str] = Field(default_factory=list, max_length=100, description="指标资源集合")
    group_by: list[SelectedDimension] = Field(default_factory=list, max_length=100, description="有序分组")
    filters: list[SelectedFilter] = Field(default_factory=list, max_length=100, description="类型化过滤条件")
    start_time: datetime | None = Field(default=None, description="起点，按构建业务时区解释")
    end_time: datetime | None = Field(default=None, description="终点，按构建业务时区解释")
    order_by: list[SelectedOrder] = Field(default_factory=list, max_length=100, description="有序排序条件")
    limit: int = Field(default=1000, ge=1, le=10000, description="最多保存的结果行数")
    dataset_resource_id: str | None = Field(default=None, description="PREVIEW 的数据集身份")
    dimension_option_id: str | None = Field(default=None, description="DIMENSION_VALUES 的维度选项")

    @model_validator(mode="after")
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
                raise ValueError("invalid PREVIEW fields")
        elif not self.metric_resource_ids or self.dataset_resource_id:
            raise ValueError("metrics are required and datasetResourceId is not allowed")
        if self.mode == QueryMode.DIMENSION_VALUES:
            if not self.dimension_option_id:
                raise ValueError("dimensionOptionId is required")
        elif self.dimension_option_id:
            raise ValueError("dimensionOptionId only belongs to DIMENSION_VALUES")
        return self


class OptionsTaskView(Contract):
    options_task_id: UUID = Field(description="持久选项任务身份")
    build_id: UUID = Field(description="固定构建身份")
    state: BuildStatus = Field(description="异步执行状态")
    metric_resource_ids: list[str] = Field(description="规范化指标集合")
    options: list[dict[str, Any]] | None = Field(default=None, description="成功后生成的合法选项")
    error: ErrorView | None = Field(default=None, description="稳定错误")


class QueryView(Contract):
    query_id: UUID = Field(description="持久查询身份")
    build_id: UUID = Field(description="固定构建身份")
    state: BuildStatus = Field(description="执行状态")
    mode: QueryMode = Field(description="查询模式")
    commit_sha: str | None = Field(description="固定源码版本")
    business_timezone: str = Field(description="构建固定的业务时区")
    normalized_time_range: dict[str, str | None] = Field(description="归一化时间输入")
    boundary_policy: str = Field(default="metricflow_granularity_alignment", description="引擎粒度边界策略")
    result_available: bool = Field(default=False, description="结果字节是否可读")
    error: ErrorView | None = Field(default=None, description="稳定错误")


class ResultPage(QueryView):
    sql: str | None = Field(default=None, description="EXPLAIN 返回的完整编译 SQL")
    columns: list[dict[str, Any]] = Field(description="结果列及精度类型")
    rows: list[list[Any]] = Field(description="有界结果正文")
    offset: int = Field(description="已保存结果中的偏移")
    returned_rows: int = Field(description="本页行数")
    next_offset: int | None = Field(description="下一页偏移")
    available_rows: int = Field(description="已保存行数，不代表源数据总数")
    result_truncated: bool = Field(description="引擎结果是否截断")
