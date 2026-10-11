"""数据库记录类型；UUID 由连接适配器读取为字符串，时间保持 datetime。"""

from collections.abc import Callable
from datetime import datetime
from typing import Any, NotRequired, TypeAlias, TypedDict

from sqlalchemy.orm import Session

from ..models.payloads import JsonObject

# SQL 投影和历史表的开放行边界；当前构建、部署和任务使用下方固定字段类型。
DatabaseRow: TypeAlias = dict[str, Any]


class StoredBuild(TypedDict):
    """engine_build 完整记录，包含可选的执行生命周期关联列。"""

    build_id: str  # 一次构建的公开身份。
    run_id: str  # 受理构建时原子创建的内部执行任务。
    repository: str  # 规范仓库地址。
    branch_name: str | None  # 构建来源分支。
    environment: str  # 预览或正式执行环境。
    execution_binding: str  # 受控执行绑定名称。
    config_version: str  # 固定配置版本。
    toolchain_version: str  # 固定引擎工具链版本。
    caller: str  # 可信调用来源。
    idempotency_key: str  # 构建受理幂等键。
    request_digest: str  # 原请求的规范化摘要。
    request_json: JsonObject  # 已校验请求的序列化副本。
    config_snapshot: JsonObject  # 固定执行配置，不含凭据。
    requested_commit_sha: str | None  # 请求显式指定的源码版本。
    commit_sha: str | None  # 实际解析并固定的源码版本。
    build_status: str  # 构建状态，与部署状态独立。
    phase: str  # 当前执行阶段。
    cancel_requested: bool  # 是否持久化了取消意图。
    output_set_id: str | None  # 完整构建产物集合。
    catalog_digest: str | None  # 封存目录字节摘要。
    error_code: str | None  # 稳定错误码。
    version: int  # 构建事实版本。
    created_at: datetime  # 受理时间。
    updated_at: datetime  # 最近更新时间。
    finished_at: datetime | None  # 终态时间。
    run_lifecycle: NotRequired[str | None]  # 关联查询附加的物理执行生命周期。


class StoredDeploymentTarget(TypedDict):
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


class StoredDeploymentAttempt(TypedDict):
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


class StoredJob(TypedDict):
    """执行任务持久字段；领取后附加的租约信息由 LeasedJob 描述。"""

    job_id: str  # 内部任务身份。
    kind: str  # 构建、选项、查询或清理类型。
    project_id: str  # 内部执行容器身份。
    branch_id: str | None  # 历史分支关联。
    parent_run_id: str | None  # 查询或清理引用的构建任务。
    request_json: JsonObject  # 固定执行请求。
    request_fingerprint: str  # 幂等请求摘要。
    status: str  # 执行状态。
    phase: str  # 执行阶段。
    input_set_id: str | None  # 固定输入产物。
    output_set_id: str | None  # 完成后生成的产物。
    config_version: str  # 固定配置版本。
    toolchain_version: str  # 固定工具链版本。
    profile_binding_id: str | None  # 数据仓库配置引用。
    schema_name: str | None  # 受控物理 schema。
    run_lifecycle: str | None  # 构建物理对象的生命周期。
    current_attempt_id: str | None  # 当前执行尝试身份。
    error_code: str | None  # 稳定错误码。
    error_detail: JsonObject | None  # 脱敏错误元数据。
    input_mode: str  # 持久输入或进程内临时输入模式。


class LeasedJob(StoredJob):
    """已领取或通过租约校验的任务，执行器可以依赖尝试身份。"""

    attempt_id: str  # 领取或租约查询附加的尝试身份。
    lease_token: str  # 用于 fencing 的租约凭据。


class CompletionOptions(TypedDict, total=False):
    """任务完成协调器可转交给 JobStore.finish 的具名参数。"""

    output_set_id: str | None  # 待封存输出集合。
    stdout_tail: str  # 脱敏标准输出尾部。
    stderr_tail: str  # 脱敏错误输出尾部。
    exit_code: int  # 已确认的子进程退出码。
    output_truncated: bool  # 诊断输出是否被截断。
    seal: Callable[[str, Session], None] | None  # 同事务封存产物的回调。
