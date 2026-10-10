"""固定提交验证的公开结果。"""

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import Field

from ..storage.history_models import Contract


class ValidationReceipt(Contract):
    validation_id: UUID
    state: Literal["QUEUED", "RUNNING", "SUCCEEDED", "FAILED", "CANCELLED"]


class ValidationDiagnostic(Contract):
    """公开诊断只包含可安全展示的定义信息；不能确定的位置保持空值。"""

    code: str
    message: str
    severity: Literal["ERROR", "WARNING"] = "ERROR"
    recovery: str
    path: str | None = None
    line: int | None = None
    column: int | None = None
    resource_name: str | None = None


class ValidationResult(ValidationReceipt):
    """valid 仅在正常完成时有值；所有摘要描述此次被冻结的输入。"""

    commit_sha: str
    project_subdir: str
    config_version: str
    toolchain_version: str
    validated_project_digest: str | None = None
    valid: bool | None = None
    phase: str | None = None
    checked_levels: list[str] = Field(default_factory=list)
    diagnostics: list[ValidationDiagnostic] = Field(default_factory=list)
    completed_at: datetime | None = None
    error_code: str | None = None
