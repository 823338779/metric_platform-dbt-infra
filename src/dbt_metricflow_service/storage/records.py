"""数据库记录类型；UUID 由连接适配器读取为字符串，时间保持 datetime。"""

from datetime import datetime
from pathlib import Path
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from dbt_metricflow_service import constants as strings

from ..execution.models import JobStatus
from ..models.builds import BuildRequest
from ..models.contracts import Contract
from ..models.execution import ExecutionSnapshot


class StoredRecord(BaseModel):
    """数据库列快照；只接受已声明字段，不持有 ORM 会话。"""

    model_config = ConfigDict(extra=strings.EXTRA_FORBID, from_attributes=True)


class ValidationCheck(Contract):
    name: str
    status: str
    message: str | None


class ValidationSummary(Contract):
    phase: str
    checks: list[ValidationCheck]
    truncated: bool


class JobErrorDetail(StoredRecord):
    """脱敏执行诊断，保持引擎记录原有的 JSON 字段名称。"""

    model_config = ConfigDict(extra=strings.EXTRA_FORBID, populate_by_name=True)
    message: str | None = None
    type: str | None = None
    external_outcome_unknown: bool | None = Field(default=None, alias=strings.EXTERNAL_OUTCOME_UNKNOWN_ALIAS)
    validation_summary: ValidationSummary | None = Field(default=None, alias=strings.VALIDATION_SUMMARY_FIELD)
    id: UUID | None = None
    project: str | None = None
    status: JobStatus | None = None
    submitted_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    exit_code: int | None = None
    stdout: str | None = None
    stderr: str | None = None
    output_truncated: bool | None = None


class ArtifactMetadata(StoredRecord):
    source_set_id: str | None = None
    source_commit_sha: str | None = None
    project_digest: str | None = None
    toolchain_version: str | None = strings.DEFAULT_TOOLCHAIN_VERSION
    format_version: str = strings.ARTIFACT_FORMAT_VERSION


class StoredExecutionLog(StoredRecord):
    """脱离 Session 的执行日志快照；不暴露数据库实体。"""

    log_id: int
    build_id: str
    job_id: str
    attempt_id: str
    phase: str
    command: str
    event_time: datetime
    level: str
    event_name: str
    event_code: str
    invocation_id: str
    node_id: str
    thread_name: str
    message: str
    created_at: datetime


class StoredArtifactSet(ArtifactMetadata):
    set_id: str
    project_id: str
    producer_attempt_id: str | None
    kind: str
    state: str
    content_digest: str | None
    file_count: int
    raw_bytes: int
    created_at: datetime
    sealed_at: datetime | None


class ArtifactDigestEntry(StoredRecord):
    relative_path: str
    raw_sha256: str
    raw_size: int


class CaptureFile(ArtifactDigestEntry):
    path: Path
    executable: bool


class StoredArtifactFile(ArtifactDigestEntry):
    set_id: str
    content: bytes
    codec: str
    stored_size: int
    media_type: str
    executable: bool


class StoredBuild(StoredRecord):
    """供构建详情、部署和历史查询使用的构建记录。"""

    build_id: str  # 一次构建的公开身份。
    project_id: str
    input_set_id: str | None
    project_digest: str | None
    repository: str  # 规范仓库地址。
    branch_name: str | None  # 构建来源分支。
    environment: str  # 预览或正式执行环境。
    environment_id: str | None  # 外部环境身份。
    toolchain_version: str  # 固定引擎工具链版本。
    caller: str  # 可信调用来源。
    idempotency_key: str  # 构建受理幂等键。
    request_json: BuildRequest  # 已校验请求。
    commit_sha: str | None  # 实际解析并固定的源码版本。
    build_status: str  # 构建状态，与部署状态独立。
    phase: str  # 当前执行阶段。
    output_set_id: str | None  # 完整构建产物集合。
    error_code: str | None  # 稳定错误码。
    version: int  # 构建事实版本。
    created_at: datetime  # 受理时间。
    updated_at: datetime  # 最近更新时间。
    finished_at: datetime | None  # 终态时间。

    @property
    def config_snapshot(self) -> ExecutionSnapshot:
        return self.request_json.execution


class StoredDeploymentTarget(StoredRecord):
    """部署自然键、并发版本和实际生效指针。"""

    repository: str  # 目标仓库。
    environment: str  # 目标环境。
    branch_name: str  # 目标分支短名。
    version: int  # 指针乐观锁版本。
    desired_generation: int  # 最新受理部署意图的序号。
    active_build_id: str | None  # 实际生效构建。
    observed_head_sha: str | None  # 最近观察到的远端提交。
    head_observed_at: datetime | None  # 最近远端观察时间。
    source_state: str  # 远端分支相对部署的状态。


class StoredDeploymentAttempt(StoredRecord):
    """一次持久部署意图。"""

    repository: str  # 目标仓库。
    environment: str  # 目标环境。
    branch_name: str  # 目标分支。
    generation: int  # 受理顺序。
    build_id: str  # 本次意图引用的构建。
    caller: str  # 可信调用来源。
    idempotency_key: str  # 本次操作的幂等键。
    operation: str  # 历史部署来源；新意图统一由构建创建。
    deployment_status: str  # 部署状态。
    reason: str | None  # 失败或过期原因。
    version: int  # 意图事实版本。
    created_at: datetime  # 受理时间。
    deployed_at: datetime | None  # 实际切换时间。
    last_checked_at: datetime | None  # 后台最近检查时间。


class LeasedBuild(StoredRecord):
    """worker 执行一次尝试所需的固定输入和租约。"""

    build_id: str
    attempt_id: str
    lease_token: str
    project_id: str
    repository: str
    branch_name: str | None
    commit_sha: str | None
    input_set_id: str | None
    project_digest: str | None
    toolchain_version: str
    execution: ExecutionSnapshot
