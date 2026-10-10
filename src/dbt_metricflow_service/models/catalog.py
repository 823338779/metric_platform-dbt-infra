"""历史资源视图固定到构建；资源原生结构不重复解释。"""

from typing import Any
from uuid import UUID

from pydantic import Field

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
    native_details: dict[str, Any] = Field(description="原生引擎定义")
