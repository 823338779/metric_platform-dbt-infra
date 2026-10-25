"""构建原生节点对应的源码；目录数据直接使用 dbt JSON。"""

from uuid import UUID

from pydantic import Field

from .artifacts import Contract


class ResourceSourceView(Contract):
    build_id: UUID = Field(description="源码所属构建")
    resource_id: str = Field(description="资源身份")
    path: str = Field(description="工程内相对路径")
    content: str = Field(description="封存源码正文")
