from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class QueryMode(StrEnum):
    """MetricFlow 平台查询模式。"""

    QUERY = "QUERY"
    EXPLAIN = "EXPLAIN"
    PREVIEW = "PREVIEW"
    DIMENSION_VALUES = "DIMENSION_VALUES"


class QueryFilter(BaseModel):
    """页面可组合的单个结构化筛选条件。"""

    model_config = ConfigDict(extra="forbid")
    field: str
    operator: str
    value: Any


class PlatformQueryRequest(BaseModel):
    """固定 run 的类型化查询；禁止传入 SQL、remote 或 profile。"""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    run_id: UUID = Field(alias="runId")
    idempotency_key: str = Field(alias="idempotencyKey")
    mode: QueryMode
    metrics: list[str] = Field(default_factory=list)
    group_by: list[str] = Field(default_factory=list, alias="groupBy")
    filters: list[QueryFilter] = Field(default_factory=list)
    start_time: datetime | None = Field(default=None, alias="startTime")
    end_time: datetime | None = Field(default=None, alias="endTime")
    order_by: list[str] = Field(default_factory=list, alias="orderBy")
    limit: int = Field(default=1000, ge=1, le=10_000)
    dataset_resource_id: str | None = Field(default=None, alias="datasetResourceId")
    dimension: str | None = None
