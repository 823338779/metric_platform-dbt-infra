"""版本化发布协议；展示字段由生产者验证后封存。"""

from datetime import datetime
from enum import StrEnum
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, JsonValue, PlainSerializer
from pydantic.alias_generators import to_camel

from dbt_metricflow_service import constants as strings

from .native import NativeColumn, NativeConversion, NativeCumulative, NativeReference

CATALOG_SCHEMA_VERSION = 1
PUBLISHED_CATALOG_FILE = strings.PUBLISHED_CATALOG_FILE


class Contract(BaseModel):
    """拒绝未约定字段，Python 内部名称映射为公开 camelCase。"""

    model_config = ConfigDict(extra=strings.EXTRA_FORBID, populate_by_name=True, alias_generator=to_camel)


class ResourceKind(StrEnum):
    METRIC = strings.METRIC
    DIMENSION = strings.DIMENSION
    SEMANTIC_MODEL = strings.SEMANTIC_MODEL
    TABLE = strings.TABLE
    VIEW = strings.VIEW


class BindingMode(StrEnum):
    BUILT = strings.BUILT
    EXTERNAL = strings.EXTERNAL


class RelationName(Contract):
    # database 在不区分 catalog 的 adapter 中可以为空；schema/identifier 必须存在。
    database: str | None = None
    schema_name: str = Field(alias=strings.DBT_SCHEMA_FIELD, min_length=1)
    identifier: str = Field(min_length=1)


class RelationBinding(Contract):
    # 原生节点和实际物理对象的一次固定映射，不沿历史发布链间接解析。
    native_id: str
    relation: RelationName
    creator_run_id: UUID | None
    mode: BindingMode
    definition_digest: str = Field(pattern=strings.SHA256_PATTERN)
    verified_at: datetime


# Native fragments retain vendor keys and omit defaults that were not in the artifact.
NativeEntityView = Annotated[NativeReference, PlainSerializer(lambda value: value.native_details())]
NativeColumnView = Annotated[NativeColumn, PlainSerializer(lambda value: value.native_details())]
NativeCumulativeView = Annotated[NativeCumulative, PlainSerializer(lambda value: value.native_details())]
NativeConversionView = Annotated[NativeConversion, PlainSerializer(lambda value: value.native_details())]


class MetricTimeConfig(Contract):
    time_granularity: str | None = None
    window: JsonValue = None
    cumulative: NativeCumulativeView = Field(default_factory=NativeCumulative)
    conversion: NativeConversionView = Field(default_factory=NativeConversion)


class MetricAttributes(Contract):
    # 完整口径追溯与跨资源引用；表达式仅供展示，执行仍由 MetricFlow 负责。
    metric_type: str
    definition_summary: str
    input_metric_ids: list[str]
    dataset_ids: list[str]
    filters: dict[str, JsonValue] | None = None
    time_config: Annotated[MetricTimeConfig, PlainSerializer(
        lambda value: value.model_dump(mode=strings.JSON_MODE, by_alias=True, exclude_unset=True))]


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
    entities: list[NativeEntityView]
    measures: list[NativeEntityView]
    default_time_dimension: str | None = None


class PhysicalAttributes(Contract):
    # 列来自 docs catalog，关系来自验证后的实际绑定。
    origin: BindingMode
    relation: RelationName
    columns: dict[str, NativeColumnView]


class ResourceBase(Contract):
    # 逻辑身份不包含构建前缀；所有资源定位都与 releaseId 联用。
    resource_id: str
    native_id: str
    name: str
    display_name: str
    description: str | None = None
    source_path: str | None = None
    capabilities: list[str]
    native_details: dict[str, JsonValue]


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
    MetricResource | DimensionResource | DatasetResource | PhysicalResource,
    Field(discriminator=strings.RESOURCE_KIND_FIELD),
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
