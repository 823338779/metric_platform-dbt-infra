"""固定 Git 基线上的 YAML 操作协议；不接受客户端提供执行命令或远端。"""

import hashlib
import unicodedata
from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import Field, field_validator, model_validator

from .publication_models import Contract

MAX_CHANGE_BYTES = 512 * 1024
MAX_TOTAL_BYTES = 5 * 1024 * 1024
DIGEST_HEADER = b"dbt-changes-v1\n"
HASH_PATTERN = "^[a-f0-9]{64}$"
COMMIT_PATTERN = "^[a-f0-9]{40}$"
CREATE = "CREATE"
DELETE = "DELETE"
YAML_SUFFIXES = (".yml", ".yaml")


class DraftChange(Contract):
    """路径相对 dbt 项目；旧摘要用于拒绝对错误基线应用操作。"""

    path: str = Field(min_length=1, max_length=1024)
    operation: Literal["CREATE", "UPDATE", "DELETE"]
    expected_sha256: str | None = Field(default=None, pattern=HASH_PATTERN)
    # 空字符串表示空文件，None 表示无正文（仅 DELETE）。正文不归一化换行。
    content: str | None = None

    @field_validator("path")
    @classmethod
    def safe_path(cls, value: str) -> str:
        parts = value.split("/")
        if (any(part in ("", ".", "..") for part in parts)
                or any(char in value for char in ("\\", ":"))
                or any(unicodedata.category(char).startswith("C") for char in value)
                or not value.endswith(YAML_SUFFIXES)):
            raise ValueError("path must be a project-relative YAML path without control characters")
        return value

    @model_validator(mode="after")
    def validate_operation(self):
        if (self.operation == CREATE) != (self.expected_sha256 is None):
            raise ValueError("CREATE has no old digest; UPDATE and DELETE require an old digest")
        if (self.operation == DELETE) != (self.content is None):
            raise ValueError("DELETE has no content; CREATE and UPDATE require content")
        if self.content is not None and len(self.content.encode("utf-8")) > MAX_CHANGE_BYTES:
            raise ValueError("YAML content exceeds byte limit")
        return self


class DraftValidationRequest(Contract):
    """同一幂等键绑定完整基线和操作集，不能复用于新内容。"""

    base_commit_sha: str = Field(pattern=COMMIT_PATTERN)
    idempotency_key: str = Field(min_length=1, max_length=256)
    changes: list[DraftChange] = Field(min_length=1, max_length=100)

    @model_validator(mode="after")
    def validate_changes(self):
        paths = [unicodedata.normalize("NFC", change.path).casefold() for change in self.changes]
        if len(set(paths)) != len(paths):
            raise ValueError("duplicate or case-colliding paths")
        if sum(len(change.content.encode("utf-8")) for change in self.changes
               if change.content is not None) > MAX_TOTAL_BYTES:
            raise ValueError("total YAML content exceeds byte limit")
        return self


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

    base_commit_sha: str
    project_subdir: str
    changes_digest: str
    config_version: str
    toolchain_version: str
    validated_project_digest: str | None = None
    valid: bool | None = None
    phase: str | None = None
    checked_levels: list[str] = Field(default_factory=list)
    diagnostics: list[ValidationDiagnostic] = Field(default_factory=list)
    completed_at: datetime | None = None
    error_code: str | None = None


def changes_digest(changes: list[DraftChange]) -> str:
    """跨语言摘要 v1：按路径 UTF-8 字节排序，内容摘要保留原始字节。"""
    digest = hashlib.sha256(DIGEST_HEADER)
    for change in sorted(changes, key=lambda item: item.path.encode("utf-8")):
        new_hash = hashlib.sha256(change.content.encode("utf-8")).hexdigest() if change.content is not None else "-"
        fields = (change.operation, change.path, change.expected_sha256 or "-", new_hash)
        digest.update(("\0".join(fields) + "\n").encode("utf-8"))
    return digest.hexdigest()
