"""可由 HTTP 或管理命令解释的稳定用例错误。"""

from __future__ import annotations

from ..models.builds import ErrorView


class ServiceError(Exception):
    """状态码是边界提示，不依赖任何 HTTP 框架。"""

    def __init__(
        self,
        code: str,
        message: str,
        status: int = 422,
        *,
        retryable: bool = False,
        phase: str | None = None,
        build_id: str | None = None,
    ) -> None:
        super().__init__(message)
        # 结构化错误仅包含可公开信息，不包裹驱动异常文本。
        self.status = status
        self.error = ErrorView.model_validate(
            {"code": code, "message": message, "retryable": retryable, "phase": phase, "build_id": build_id}
        )
