"""版本化发布协议；展示字段由生产者验证后封存。"""

from datetime import datetime
from enum import StrEnum
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel

from .platform_models import QueryMode

CATALOG_SCHEMA_VERSION = 1
PUBLISHED_CATALOG_FILE = "published_catalog.json"


class Contract(BaseModel):
    """拒绝未约定字段，Python 内部名称映射为公开 camelCase。"""

    model_config = ConfigDict(extra="forbid", populate_by_name=True, alias_generator=to_camel)


class ResourceKind(StrEnum):
    METRIC = "METRIC"
    DIMENSION = "DIMENSION"
    SEMANTIC_MODEL = "SEMANTIC_MODEL"
    TABLE = "TABLE"
    VIEW = "VIEW"


class BindingMode(StrEnum):
    BUILT = "BUILT"
    REUSED = "REUSED"
    EXTERNAL = "EXTERNAL"


class BuildMode(StrEnum):
    SEMANTIC_ONLY = "SEMANTIC_ONLY"
    SELECTIVE_BUILD = "SELECTIVE_BUILD"
    FULL_BUILD = "FULL_BUILD"


class RelationName(Contract):
    # database 在不区分 catalog 的 adapter 中可以为空；schema/identifier 必须存在。
    database: str | None = None
    schema_name: str = Field(alias="schema", min_length=1)
    identifier: str = Field(min_length=1)


class RelationBinding(Contract):
    # 原生节点和实际物理对象的一次固定映射，不沿历史发布链间接解析。
    native_id: str
    relation: RelationName
    creator_run_id: UUID | None
    mode: BindingMode
    definition_digest: str = Field(pattern="^[a-f0-9]{64}$")
    verified_at: datetime


class BuildPlan(Contract):
    # 选择集合与复用集合分区覆盖本次所有物理模型；理由用于发布审计。
    build_mode: BuildMode
    selected_native_ids: list[str]
    reuse_native_ids: list[str]
    reasons: list[str]
    relation_bindings: list[RelationBinding] = Field(default_factory=list)


class MetricAttributes(Contract):
    # 完整口径追溯与跨资源引用；表达式仅供展示，执行仍由 MetricFlow 负责。
    metric_type: str
    definition_summary: str
    input_metric_ids: list[str]
    dataset_ids: list[str]
    filters: dict[str, Any] | None = None
    time_config: dict[str, Any]


class DimensionAttributes(Contract):
    # 维度归属固定到语义模型，不能通过同名维度推断 join 路径。
    dataset_id: str
    dimension_type: str
    expression: str | None = None
    time_granularity: str | None = None


class DatasetAttributes(Contract):
    # 语义模型的物理来源、字段归属及原生实体/度量展示信息。
    model_resource_id: str
    dimension_ids: list[str]
    entities: list[dict[str, Any]]
    measures: list[dict[str, Any]]
    default_time_dimension: str | None = None


class PhysicalAttributes(Contract):
    # 列来自 docs catalog，关系来自验证后的实际绑定。
    origin: BindingMode
    relation: RelationName
    columns: dict[str, dict[str, Any]]


class ResourceBase(Contract):
    # 逻辑身份不包含构建前缀；所有资源定位都与 releaseId 联用。
    resource_id: str
    native_id: str
    name: str
    display_name: str
    description: str | None = None
    source_path: str | None = None
    capabilities: list[str]
    native_details: dict[str, Any]


class MetricResource(ResourceBase):
    kind: Literal[ResourceKind.METRIC] = ResourceKind.METRIC
    attributes: MetricAttributes


class DimensionResource(ResourceBase):
    kind: Literal[ResourceKind.DIMENSION] = ResourceKind.DIMENSION
    attributes: DimensionAttributes


class DatasetResource(ResourceBase):
    kind: Literal[ResourceKind.SEMANTIC_MODEL] = ResourceKind.SEMANTIC_MODEL
    attributes: DatasetAttributes


class PhysicalResource(ResourceBase):
    kind: Literal[ResourceKind.TABLE, ResourceKind.VIEW]
    attributes: PhysicalAttributes


PublishedResource = Annotated[
    MetricResource | DimensionResource | DatasetResource | PhysicalResource, Field(discriminator="kind")
]


class CatalogRelation(Contract):
    # 有方向的逻辑依赖，端点都必须属于同一发布快照。
    upstream_resource_id: str
    downstream_resource_id: str
    kind: str


class PublishedCatalog(Contract):
    # 完整且不可变的发布表示；读取端不再解释原生产物。
    schema_version: Literal[1] = CATALOG_SCHEMA_VERSION
    project_id: str
    release_id: UUID
    resources: list[PublishedResource]
    relations: list[CatalogRelation]
    relation_bindings: list[RelationBinding]


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
