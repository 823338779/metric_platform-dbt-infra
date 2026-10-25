"""固定构建的资源详情，同时返回原生节点与封存源码。"""

from uuid import UUID

from pydantic import Field

from .artifacts import Contract
from .payloads import JsonObject


class ResourceSourceView(Contract):
    path: str = Field(description="工程内相对路径")
    content: str = Field(description="封存源码正文")


class ResourceView(Contract):
    """统一资源读取响应；原生字段完整保留在 resource 内。"""

    build_id: UUID = Field(description="资源及源码所属的固定构建")
    resource_id: str = Field(description="dbt 原生资源 unique_id")
    resource: JsonObject = Field(description="完整的 dbt 原生节点定义")
    source: ResourceSourceView | None = Field(default=None, description="对应的完整源码文件；无路径或输入快照时为空")
