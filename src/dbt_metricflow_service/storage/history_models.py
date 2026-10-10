"""版本化发布协议；展示字段由生产者验证后封存。"""

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel

from ..models.artifacts import (
    BindingMode as BindingMode,
)
from ..models.artifacts import (
    BuildMode as BuildMode,
)
from ..models.artifacts import (
    BuildPlan as BuildPlan,
)
from ..models.artifacts import (
    CatalogRelation as CatalogRelation,
)
from ..models.artifacts import (
    DatasetAttributes as DatasetAttributes,
)
from ..models.artifacts import (
    DatasetResource as DatasetResource,
)
from ..models.artifacts import (
    DimensionAttributes as DimensionAttributes,
)
from ..models.artifacts import (
    DimensionResource as DimensionResource,
)
from ..models.artifacts import (
    MetricAttributes as MetricAttributes,
)
from ..models.artifacts import (
    MetricResource as MetricResource,
)
from ..models.artifacts import (
    PhysicalAttributes as PhysicalAttributes,
)
from ..models.artifacts import (
    PhysicalResource as PhysicalResource,
)
from ..models.artifacts import (
    PublishedCatalog as PublishedCatalog,
)
from ..models.artifacts import (
    PublishedResource as PublishedResource,
)
from ..models.artifacts import (
    RelationBinding as RelationBinding,
)
from ..models.artifacts import (
    RelationName as RelationName,
)
from ..models.artifacts import (
    ResourceBase as ResourceBase,
)
from ..models.artifacts import (
    ResourceKind as ResourceKind,
)
from ..platform.models import QueryMode


class Contract(BaseModel):
    """拒绝未约定字段，Python 内部名称映射为公开 camelCase。"""

    model_config = ConfigDict(extra="forbid", populate_by_name=True, alias_generator=to_camel)


class FixedCommitRequest(Contract):
    """只接收不可变提交身份，连接与执行参数由项目绑定决定。"""

    commit_sha: str = Field(pattern=r"^[0-9a-f]{40}(?:[0-9a-f]{24})?$")
    idempotency_key: str = Field(min_length=1, max_length=256)


class QueryOptionsRequest(Contract):
    # 前端只提供平台资源身份，不能提供引擎名称。
    release_id: UUID
    metric_resource_ids: list[str] = Field(min_length=1)


class SelectedDimension(Contract):
    # 选项身份固定到版本及指标集合，粒度必须属于该选项。
    option_id: str
    grain: str | None = None


class SelectedFilter(Contract):
    # 筛选值保留 JSON 类型，操作符由服务端引擎选项再次验证。
    option_id: str
    operator: str
    value: Any


class SelectedOrder(Contract):
    # 排序只能引用已选择的指标资源或分组选项。
    field_id: str
    direction: Literal["ASC", "DESC"]


class PublishedQueryRequest(Contract):
    # 查询受理所需的完整版本化参数；未知字段一律拒绝。
    release_id: UUID
    idempotency_key: str = Field(min_length=1, max_length=256)
    mode: QueryMode
    metric_resource_ids: list[str] = Field(default_factory=list)
    group_by: list[SelectedDimension] = Field(default_factory=list)
    filters: list[SelectedFilter] = Field(default_factory=list)
    start_time: datetime | None = None
    end_time: datetime | None = None
    order_by: list[SelectedOrder] = Field(default_factory=list)
    limit: int = Field(default=1000, ge=1, le=10000)
    dataset_resource_id: str | None = None
    dimension_option_id: str | None = None


TaskState = Literal["QUEUED", "RUNNING", "READY", "FAILED", "CANCELLED"]


class OptionsTask(Contract):
    """选项任务身份固定到发布，READY 后才提供合法选项。"""

    options_job_id: UUID
    release_id: UUID
    state: TaskState
    metric_resource_ids: list[str] | None = None
    options: list[dict[str, Any]] | None = None
    error_code: str | None = None
    error: dict[str, Any] | None = None


class QueryMetadata(Contract):
    """查询身份及受理时冻结的时间解释，历史查询不随活动发布改变。"""

    query_id: UUID
    project_id: str
    release_id: UUID
    mode: QueryMode | None
    target_commit_sha: str | None
    business_timezone: str | None = None
    normalized_time_range: dict[str, str | None] | None = None
    boundary_policy: str | None = None


class QueryStatus(QueryMetadata):
    state: TaskState
    result_available: bool
    error_code: str | None = None
    error: dict[str, Any] | None = None


class QueryResultPage(QueryMetadata):
    """游标只覆盖已保存结果，不能把 availableRows 解释为数据库总量。"""

    state: Literal["READY"]
    columns: list[dict[str, Any]]
    rows: list[list[Any]]
    offset: int
    returned_rows: int
    next_offset: int | None
    available_rows: int
    result_truncated: bool
    sql: str | None = None
