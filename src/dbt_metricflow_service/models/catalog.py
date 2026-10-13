"""历史资源视图固定到构建；资源原生结构不重复解释。"""

from uuid import UUID

from pydantic import Field, JsonValue

from .artifacts import (
    CatalogRelation,
    DatasetAttributes,
    DimensionAttributes,
    MetricAttributes,
    PhysicalAttributes,
    PublishedResource,
    ResourceBase,
    ResourceKind,
)
from .builds import Contract, Page
from .native import NativeColumn, NativeNode


class CatalogPage(Page[PublishedResource]):
    build_id: UUID = Field(description="本页资源所属构建")


class ResourceView(ResourceBase):
    build_id: UUID = Field(description="资源所属构建")
    kind: ResourceKind = Field(description="原生资源种类")
    attributes: MetricAttributes | DimensionAttributes | DatasetAttributes | PhysicalAttributes = Field(
        description="保持封存类型的资源属性"
    )


class ResourceSourceView(Contract):
    build_id: UUID = Field(description="源码所属构建")
    resource_id: str = Field(description="资源身份")
    path: str = Field(description="工程内相对路径")
    content: str = Field(description="封存源码正文")


class ResourceLineageView(Contract):
    build_id: UUID = Field(description="血缘所属构建")
    resource_id: str = Field(description="资源身份")
    dependencies: list[CatalogRelation] = Field(description="同一构建的上下游关系")


class ResourceNativeView(Contract):
    build_id: UUID = Field(description="原生详情所属构建")
    resource_id: str = Field(description="资源身份")
    native_details: dict[str, JsonValue] = Field(description="原生引擎定义")

class ArtifactSchemas(Contract):
    manifest: str
    semantic_manifest: str
    catalog: str


class NativeResource(Contract):
    resource_id: str
    native_id: str
    kind: ResourceKind
    name: str
    description: str | None = None
    source_path: str | None = None
    definition: NativeNode
    queryable: bool = False
    relation: str | None = None
    columns: dict[str, NativeColumn] = Field(default_factory=dict)
    semantic_model_id: str | None = None


class NativeCatalog(Contract):
    resources: list[NativeResource]
    dependencies: list[CatalogRelation]
    artifact_schemas: ArtifactSchemas | None = None
    ephemeral_dependencies: dict[str, list[str]] = Field(default_factory=dict)
