"""HTTP 与任务结果共享的安全业务错误；不序列化底层异常。"""

from dataclasses import dataclass
from uuid import uuid4

INVALID_SELECTION = "invalid_query_selection"


@dataclass
class PublicationError(ValueError):
    """code 保持旧客户端兼容，reason/field/recovery 供 agent 修正请求。"""

    code: str
    reason: str
    field: str | None
    safe_message: str
    retryable: bool
    recovery: str
    status_code: int = 422

    def detail(self) -> dict:
        return {"code": self.code, "reason": self.reason, "field": self.field,
                "message": self.safe_message, "retryable": self.retryable,
                "recovery": self.recovery, "requestId": uuid4().hex}
