from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field, JsonValue

from dbt_metricflow_service import constants as strings

from ..models.contracts import Contract
from ..models.queries import QueryMode, ResultColumn


class QueryFilter(BaseModel):
    """页面可组合的单个结构化筛选条件。"""

    model_config = ConfigDict(extra=strings.EXTRA_FORBID)
    field: str
    operator: str
    value: JsonValue


class CompiledQuery(BaseModel):
    """已解析选项与时间的引擎查询，不携带持久任务身份。"""

    model_config = ConfigDict(extra=strings.EXTRA_FORBID, populate_by_name=True)

    mode: QueryMode
    metrics: list[str] = Field(default_factory=list)
    group_by: list[str] = Field(default_factory=list, alias=strings.GROUP_BY_ALIAS)
    filters: list[QueryFilter] = Field(default_factory=list)
    start_time: datetime | None = Field(default=None, alias=strings.START_TIME_ALIAS)
    end_time: datetime | None = Field(default=None, alias=strings.END_TIME_ALIAS)
    order_by: list[str] = Field(default_factory=list, alias=strings.ORDER_BY_ALIAS)
    limit: int = Field(default=1000, ge=1, le=10_000)


class NativeDimension(Contract):
    token: str
    name: str | None = None
    semantic_model_name: str | None = None
    type: str = strings.UNKNOWN_VALUE_TYPE
    operators: list[str] | None = None
    grain_tokens: dict[str, str] = Field(default_factory=dict)


class NativeTimeDimension(Contract):
    token: str
    granularity: str


class DimensionQueryResult(Contract):
    """所选指标的可组合维度、时间粒度和过滤能力；不包含数据行。"""

    dimensions: list[NativeDimension] = Field(default_factory=list)
    time_dimensions: list[NativeTimeDimension] = Field(default_factory=list)
    allowed_filters: list[str] = Field(default_factory=list)


class DataQueryResult(Contract):
    """数据查询、维度值和预览的有界结果，或 explain 生成的 SQL。"""

    columns: list[ResultColumn] = Field(default_factory=list)
    rows: list[list[JsonValue]] = Field(default_factory=list)
    truncated: bool = False
    sql: str | None = None
