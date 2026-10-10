"""固定源码与构建身份，不包含项目管理或交付状态。"""

import re
from datetime import datetime
from enum import StrEnum
from typing import Any, Generic, TypeVar
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from pydantic.alias_generators import to_camel

SHA_PATTERN = r"^[0-9a-f]{40}(?:[0-9a-f]{24})?$"
REF_PREFIX = "refs/"
MAIN_BRANCH = "main"
INVALID_REF = re.compile(r"[\x00-\x20\x7f~^:?*\[\\]")
REF_SUFFIX = ".lock"
T = TypeVar("T")


class Contract(BaseModel):
    """公开 JSON 统一 camelCase，拒绝未声明的历史别名。"""

    model_config = ConfigDict(extra="forbid", populate_by_name=True, alias_generator=to_camel)


class Environment(StrEnum):
    PREVIEW = "PREVIEW"
    PRODUCTION = "PRODUCTION"


class DeploymentPolicy(StrEnum):
    NONE = "NONE"
    ON_SUCCESS = "ON_SUCCESS"


class BuildStatus(StrEnum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    OUTCOME_UNKNOWN = "OUTCOME_UNKNOWN"


class ConfigSnapshot(Contract):
    """只保存可公开审计的执行语义，密码由 profile 环境加载。"""

    profile_binding_id: str = Field(min_length=1, description="受控 profile 的 target 引用")
    environments: list[Environment] = Field(min_length=1, description="允许使用的执行环境")
    business_timezone: str = Field(default="UTC", description="构建固定的业务时区")
    schema_name: str | None = Field(default=None, description="可选固定 schema；物理表仍按构建隔离")
    query_retry_safe: bool = Field(default=False, strict=True, description="执行端明确允许只读任务重试")

    @field_validator("schema_name")
    @classmethod
    def valid_schema(cls, value):
        if value is not None and not re.fullmatch(r"[a-z][a-z0-9_]{0,255}", value):
            raise ValueError("invalid schemaName")
        return value


def validate_branch(value: str | None) -> str | None:
    """校验 Git 短分支名，防止 ref 或选项被混入远端读取。"""
    if value is None:
        return value
    if (
        not value
        or value.startswith((REF_PREFIX, "-", "/"))
        or value.endswith(("/", "."))
        or INVALID_REF.search(value)
        or ".." in value
        or "@{" in value
        or value == "@"
        or any(not part or part.startswith(".") or part.endswith(REF_SUFFIX) for part in value.split("/"))
    ):
        raise ValueError("branchName must be a short Git branch name")
    return value


class Origin(Contract):
    """可选追溯标签；不解析对象、不查询调用方、不参与授权。"""

    session_id: str | None = Field(default=None, max_length=256, description="调用方原样提供的追溯标签")
    operation_id: str | None = Field(default=None, max_length=256, description="调用方原样提供的关联标签")


class BuildRequest(Contract):
    repository: str = Field(min_length=1, max_length=2048, description="受控配置中的规范仓库地址")
    branch_name: str | None = Field(default=None, description="可选来源分支短名")
    commit_sha: str | None = Field(default=None, pattern=SHA_PATTERN, description="固定远端提交完整 SHA")
    environment: Environment = Field(description="构建的物理执行环境")
    execution_binding: str = Field(min_length=1, max_length=128, description="可复用执行配置引用")
    config_version: str = Field(min_length=1, max_length=128, description="不可变语义配置版本")
    deployment_policy: DeploymentPolicy = Field(default=DeploymentPolicy.NONE, description="是否附带成功部署意图")
    idempotency_key: str = Field(min_length=1, max_length=256, description="调用方持久化的一次受理键")
    origin: Origin | None = Field(default=None, description="无业务语义的追溯标签")

    _branch = field_validator("branch_name")(validate_branch)

    @model_validator(mode="after")
    def fixed_source(self):
        # 这里只检查协议组合；仓库准入与配置可用性由应用用例负责。
        if not self.branch_name and not self.commit_sha:
            raise ValueError("branchName or commitSha is required")
        if self.deployment_policy == DeploymentPolicy.ON_SUCCESS and not self.branch_name:
            raise ValueError("deployment requires branchName")
        if self.environment == Environment.PRODUCTION:
            if self.branch_name != MAIN_BRANCH or not self.commit_sha:
                raise ValueError("production requires main and an explicit commitSha")
        elif self.branch_name == MAIN_BRANCH:
            raise ValueError("preview cannot target main")
        return self


class ErrorView(Contract):
    code: str = Field(description="稳定错误码")
    message: str = Field(description="不含凭据或本地路径的诊断")
    retryable: bool = Field(default=False, description="能否以同请求重试")
    phase: str | None = Field(default=None, description="失败执行阶段")
    build_id: UUID | None = Field(default=None, description="已有构建身份")


class BuildView(Contract):
    build_id: UUID = Field(description="一次构建身份，与提交 SHA 无关")
    repository: str = Field(description="规范仓库地址")
    branch_name: str | None = Field(description="请求来源分支")
    requested_commit_sha: str | None = Field(description="受理时显式指定的 SHA")
    commit_sha: str | None = Field(description="首次解析后固定的实际 SHA")
    environment: Environment = Field(description="执行环境")
    execution_binding: str = Field(description="执行配置引用")
    config_version: str = Field(description="固定配置版本")
    toolchain_version: str = Field(description="固定工具链版本")
    build_status: BuildStatus = Field(description="构建状态，不代表部署状态")
    phase: str = Field(description="当前执行阶段")
    created_at: datetime = Field(description="受理时间")
    updated_at: datetime = Field(description="最近事实更新时间")
    finished_at: datetime | None = Field(default=None, description="终态时间")
    cancel_requested: bool = Field(default=False, description="是否已请求取消")
    catalog_available: bool = Field(default=False, description="完整封存目录是否可读")
    query_available: bool = Field(default=False, description="是否可受理新的查询")
    query_unavailable_reason: str | None = Field(default=None, description="不可查询原因")
    error: ErrorView | None = Field(default=None, description="脱敏错误摘要")
    initial_deployment: dict[str, Any] | None = Field(default=None, description="受理时附带的部署意图")


class Page(Contract, Generic[T]):
    items: list[T] = Field(description="本页有界结果")
    next_cursor: str | None = Field(default=None, description="绑定筛选条件的后续游标")


class LogPage(Contract):
    items: list[str] = Field(description="有界、脱敏日志片段")
    next_cursor: str | None = Field(default=None, description="后续日志位置")
    truncated: bool = Field(default=False, description="源日志是否已截断")


class ChangeRecord(Contract):
    sequence: int = Field(description="与事实同事务提交的变化序号")
    repository: str = Field(description="规范仓库地址")
    object_type: str = Field(description="构建或部署对象类型")
    object_id: str = Field(description="对象的稳定身份")
    object_version: int = Field(description="对象递增版本")
    summary: dict[str, Any] = Field(description="可重建历史投影的有界事实摘要")


ChangePage = Page[ChangeRecord]
