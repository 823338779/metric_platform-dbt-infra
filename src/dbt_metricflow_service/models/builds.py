"""固定源码与构建身份，不包含项目管理或交付状态。"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Final
from urllib.parse import urlsplit
from uuid import UUID

from pydantic import Field, field_validator

from dbt_metricflow_service import constants as strings

from .contracts import SHA_PATTERN, Contract, validate_branch
from .deployments import DeploymentAttemptView
from .execution import ExecutionSnapshot
from .service import ErrorView

INVALID_REPOSITORY_URL_MESSAGE: Final[str] = 'repository must be a remote URL without embedded credentials'


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


class BuildRequest(Contract):
    repository: str = Field(min_length=1, max_length=2048, description="本次源码仓库地址")
    branch_name: str = Field(description="本次明确指定的 Git 短分支名")
    commit_sha: str | None = Field(default=None, pattern=SHA_PATTERN, description="可选固定提交")
    execution: ExecutionSnapshot = Field(description="受理时固定的完整执行配置")
    deployment_policy: DeploymentPolicy = Field(default=DeploymentPolicy.NONE, description="成功部署意图")
    idempotency_key: str = Field(min_length=1, max_length=256, description="一次提交的稳定请求键")
    origin: Origin | None = Field(default=None, description="调用方追溯标签")

    _branch = field_validator(strings.BRANCH_NAME_FIELD)(validate_branch)

    @field_validator(strings.REPOSITORY_FIELD)
    @classmethod
    def valid_repository(cls, value: str) -> str:
        parsed = urlsplit(value)
        if (parsed.scheme not in {strings.HTTP_SCHEME, strings.HTTPS_SCHEME, strings.SSH_SCHEME} or not parsed.hostname
                or parsed.password or parsed.query or parsed.fragment
                or any(char.isspace() for char in value)
                or parsed.scheme in {strings.HTTP_SCHEME, strings.HTTPS_SCHEME} and parsed.username):
            raise ValueError(INVALID_REPOSITORY_URL_MESSAGE)
        return value


class BuildView(Contract):
    execution: ExecutionSnapshot = Field(description="受理时固定的完整执行快照")
    build_id: UUID = Field(description="一次构建身份，与提交 SHA 无关")
    repository: str = Field(description="规范仓库地址")
    branch_name: str | None = Field(description="请求来源分支")
    requested_commit_sha: str | None = Field(description="受理时显式指定的 SHA")
    commit_sha: str | None = Field(description="首次解析后固定的实际 SHA")
    toolchain_version: str = Field(description="固定工具链版本")
    build_status: BuildStatus = Field(description="构建状态，不代表部署状态")
    phase: str = Field(description="当前执行阶段")
    created_at: datetime = Field(description="受理时间")
    updated_at: datetime = Field(description="最近事实更新时间")
    finished_at: datetime | None = Field(default=None, description="终态时间")
    catalog_available: bool = Field(default=False, description="完整封存目录是否可读")
    error: ErrorView | None = Field(default=None, description="脱敏错误摘要")
    initial_deployment: DeploymentAttemptView | None = Field(default=None, description="受理时附带的部署意图")


class ExecutionLogView(Contract):
    """前端展示的一条 dbt 详细日志；消息必须作为文本渲染。"""

    log_id: str = Field(description="日志唯一标识，以十进制字符串返回，避免 JavaScript 整数精度丢失")
    job_id: UUID = Field(description="所属任务标识")
    attempt_id: UUID = Field(description="执行尝试标识，可用于区分重试")
    phase: str = Field(description="产生该日志的构建阶段")
    command: str = Field(description="dbt 子命令")
    event_time: datetime = Field(description="事件产生时间，UTC")
    level: str = Field(description="原始日志级别，例如 debug、info、warn、warning、error")
    event_name: str = Field(description="dbt 事件名或 Python logger 名称")
    event_code: str = Field(description="dbt 事件代码，无此信息时为空字符串")
    invocation_id: str = Field(description="dbt 调用标识，无此信息时为空字符串")
    node_id: str = Field(description="模型等节点的 unique_id，无此信息时为空字符串")
    thread_name: str = Field(description="产生事件的线程名称")
    message: str = Field(description="脱敏后的完整消息，可包含换行和 ANSI 转义序列，不是 HTML")
    created_at: datetime = Field(description="日志写入数据库的时间，UTC")


class ExecutionLogPage(Contract):
    items: list[ExecutionLogView] = Field(description="按日志标识升序返回的本页记录")
    next_cursor: str = Field(description="绑定构建的续读游标；空页及末页也返回，可继续轮询")
    has_more: bool = Field(description="查询时是否已有下一页，不代表构建是否结束或日志是否完整")
