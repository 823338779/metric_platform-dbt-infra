"""固定源码与构建身份，不包含项目管理或交付状态。"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Generic, TypeVar
from uuid import UUID

from pydantic import Field

from dbt_metricflow_service import constants as strings

from .contracts import SHA_PATTERN, Contract, validate_branch  # noqa: F401
from .deployments import DeploymentAttemptView

T = TypeVar("T")


class DeploymentPolicy(StrEnum):
    NONE = strings.NONE
    ON_SUCCESS = strings.ON_SUCCESS


class BuildStatus(StrEnum):
    QUEUED = strings.QUEUED
    RUNNING = strings.RUNNING
    SUCCEEDED = strings.SUCCEEDED
    FAILED = strings.FAILED
    CANCELLED = strings.CANCELLED
    OUTCOME_UNKNOWN = strings.OUTCOME_UNKNOWN


class Origin(Contract):
    """可选追溯标签；不解析对象、不查询调用方、不参与授权。"""

    session_id: str | None = Field(default=None, max_length=256, description="调用方原样提供的追溯标签")
    operation_id: str | None = Field(default=None, max_length=256, description="调用方原样提供的关联标签")


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
    initial_deployment: DeploymentAttemptView | None = Field(default=None, description="受理时附带的部署意图")


class Page(Contract, Generic[T]):
    items: list[T] = Field(description="本页有界结果")
    next_cursor: str | None = Field(default=None, description="绑定筛选条件的后续游标")


class LogPage(Contract):
    items: list[str] = Field(description="有界、脱敏日志片段")
    next_cursor: str | None = Field(default=None, description="后续日志位置")
    truncated: bool = Field(default=False, description="源日志是否已截断")
