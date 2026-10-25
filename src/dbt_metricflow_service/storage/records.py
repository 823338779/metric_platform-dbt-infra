"""数据库记录类型；UUID 由连接适配器读取为字符串，时间保持 datetime。"""

from datetime import datetime
from pathlib import Path
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from dbt_metricflow_service import constants as strings

from ..execution.models import JobStatus
from ..models.builds import Contract, DeploymentPolicy, Origin
from ..models.execution import ExecutionSnapshot, SnapshotBuildRequest
from ..models.payloads import ValidationSummary


class StoredRecord(BaseModel):
    """数据库列快照；只接受已声明字段，不持有 ORM 会话。"""

    model_config = ConfigDict(extra=strings.EXTRA_FORBID, from_attributes=True)


class EmptyConfig(StoredRecord):
    """尚未配置执行绑定的内部项目。"""


class JobRequest(Contract):
    """持久构建任务请求；底层队列测试允许空请求。"""

    repository: str | None = None
    branch_name: str | None = None
    commit_sha: str | None = None
    execution: ExecutionSnapshot | None = None
    deployment_policy: DeploymentPolicy | None = None
    idempotency_key: str | None = None
    origin: Origin | None = None
    build_id: str | None = None
    binding: ExecutionSnapshot | None = None
    project_digest: str | None = None


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
    config_version: str | None = strings.DEFAULT_CONFIG_VERSION
    toolchain_version: str | None = strings.DEFAULT_TOOLCHAIN_VERSION
    format_version: str = strings.ARTIFACT_FORMAT_VERSION


class StoredProject(StoredRecord):
    project_id: str
    binding_config: ExecutionSnapshot | EmptyConfig
    config_version: str
    source_set_id: str | None
    busy_job_id: str | None
    revision: int


class StoredExecutionLog(StoredRecord):
    """脱离 Session 的执行日志快照；不暴露数据库实体。"""

    log_id: int
    build_id: str | None
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
    """engine_build 完整记录，包含可选的执行生命周期关联列。"""

    build_id: str  # 一次构建的公开身份。
    run_id: str  # 受理构建时原子创建的内部执行任务。
    repository: str  # 规范仓库地址。
    branch_name: str | None  # 构建来源分支。
    environment: str  # 预览或正式执行环境。
    environment_id: str | None  # 外部环境身份。
    physical_target_key: str | None  # 完整物理写入目标摘要。
    config_version: str  # 固定配置版本。
    toolchain_version: str  # 固定引擎工具链版本。
    caller: str  # 可信调用来源。
    idempotency_key: str  # 构建受理幂等键。
    request_digest: str  # 原请求的规范化摘要。
    request_json: SnapshotBuildRequest  # 已校验请求。
    config_snapshot: ExecutionSnapshot  # 固定执行配置，不含凭据。
    requested_commit_sha: str | None  # 请求显式指定的源码版本。
    commit_sha: str | None  # 实际解析并固定的源码版本。
    build_status: str  # 构建状态，与部署状态独立。
    phase: str  # 当前执行阶段。
    cancel_requested: bool  # 是否持久化了取消意图。
    output_set_id: str | None  # 完整构建产物集合。
    error_code: str | None  # 稳定错误码。
    version: int  # 构建事实版本。
    created_at: datetime  # 受理时间。
    updated_at: datetime  # 最近更新时间。
    finished_at: datetime | None  # 终态时间。


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
    request_digest: str  # 本次请求摘要。
    operation: str  # 自动部署或独立部署操作。
    deployment_status: str  # 部署状态。
    reason: str | None  # 失败或过期原因。
    version: int  # 意图事实版本。
    created_at: datetime  # 受理时间。
    deployed_at: datetime | None  # 实际切换时间。
    last_checked_at: datetime | None  # 后台最近检查时间。


class StoredJob(StoredRecord):
    """执行任务持久字段；领取后附加的租约信息由 LeasedJob 描述。"""

    physical_target_key: str | None
    target_claimed: bool
    idempotency_scope: str | None
    idempotency_key: str | None
    attempt_no: int
    available_at: datetime
    max_attempts: int
    retry_policy: str
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    job_id: str  # 内部任务身份。
    project_id: str  # 内部执行容器身份。
    parent_run_id: str | None  # 历史父任务引用。
    request_json: JobRequest  # 固定执行请求。
    request_fingerprint: str  # 幂等请求摘要。
    status: str  # 执行状态。
    phase: str  # 执行阶段。
    input_set_id: str | None  # 固定输入产物。
    output_set_id: str | None  # 完成后生成的产物。
    config_version: str  # 固定配置版本。
    toolchain_version: str  # 固定工具链版本。
    profile_binding_id: str | None  # 数据仓库配置引用。
    schema_name: str | None  # 受控物理 schema。
    current_attempt_id: str | None  # 当前执行尝试身份。
    error_code: str | None  # 稳定错误码。
    error_detail: JobErrorDetail | None  # 脱敏错误元数据。


class LeasedJob(StoredJob):
    """已领取或通过租约校验的任务，执行器可以依赖尝试身份。"""

    attempt_id: str  # 领取或租约查询附加的尝试身份。
    lease_token: str  # 用于 fencing 的租约凭据。

    execution_stage: str = strings.PREPARING
