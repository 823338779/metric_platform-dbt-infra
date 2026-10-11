"""当前业务表的 ORM 映射；结构、约束和触发器由 schema.sql 定义。"""

from datetime import datetime
from typing import Annotated

from sqlalchemy import BigInteger, DateTime, Text, text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from dbt_metricflow_service.models.payloads import JsonObject

# 复用真正重复的列类型，字段仍显式声明名称、默认值和业务含义。
StringUUID = Annotated[str, mapped_column(UUID(as_uuid=False))]
BigInt = Annotated[int, mapped_column(BigInteger)]
JsonDocument = Annotated[JsonObject, mapped_column(JSONB(none_as_null=True))]


class Base(DeclarativeBase):
    """统一文本和带时区时间类型；可空性由 Mapped 的 Optional 注解推导。"""

    type_annotation_map = {str: Text, datetime: DateTime(timezone=True)}


class RuntimeProject(Base):
    """runtime_project 的持久字段；对外由 Store 返回记录快照。"""

    __tablename__ = "runtime_project"

    project_id: Mapped[str] = mapped_column(primary_key=True, comment="请求中的逻辑项目标识")
    binding_config: Mapped[JsonDocument] = mapped_column(
        server_default=text("'{}'::jsonb"), comment="受控绑定的非敏感配置"
    )
    config_version: Mapped[str] = mapped_column(server_default=text("'1'::text"), comment="连接和项目配置版本")
    source_set_id: Mapped[StringUUID | None] = mapped_column(comment="当前项目源码快照")
    current_output_set_id: Mapped[StringUUID | None] = mapped_column(comment="当前源码版本最近成功的默认输出")
    busy_job_id: Mapped[StringUUID | None] = mapped_column(comment="尚未确认停止的通用写任务")
    revision: Mapped[BigInt] = mapped_column(server_default=text("0"), comment="项目指针的乐观并发版本")


class RuntimeJob(Base):
    """runtime_job 的持久字段；对外由 Store 返回记录快照。"""

    __tablename__ = "runtime_job"

    job_id: Mapped[StringUUID] = mapped_column(primary_key=True, comment="公开任务标识，保留现有 run 和 query UUID")
    kind: Mapped[str] = mapped_column(comment="执行种类；DRAFT_VALIDATION 仅解析和语义验证，不改变发布或默认产物")
    project_id: Mapped[str] = mapped_column(comment="所属逻辑项目")
    parent_run_id: Mapped[StringUUID | None] = mapped_column(comment="查询或清理绑定的固定构建 run")
    idempotency_scope: Mapped[str | None] = mapped_column(comment="幂等键的接口命名空间")
    idempotency_key: Mapped[str | None] = mapped_column(comment="调用方提供的幂等键")
    request_fingerprint: Mapped[str] = mapped_column(comment="包含固定输入和配置版本的规范化请求摘要")
    request_json: Mapped[JsonDocument] = mapped_column(
        server_default=text("'{}'::jsonb"), comment="排除临时 resources 和凭据的可重放参数"
    )
    input_mode: Mapped[str] = mapped_column(
        server_default=text("'DURABLE'::text"), comment="输入为可持久恢复或仅驻接收实例内存"
    )
    pinned_instance_id: Mapped[StringUUID | None] = mapped_column(comment="持有 VOLATILE 输入的唯一实例")
    input_lease_expires_at: Mapped[datetime | None] = mapped_column(comment="覆盖排队期和执行期的内存输入租约截止时间")
    input_set_id: Mapped[StringUUID | None] = mapped_column(comment="受理时固定的输入集合")
    output_set_id: Mapped[StringUUID | None] = mapped_column(comment="成功事务发布的输出集合")
    config_version: Mapped[str] = mapped_column(comment="执行所需配置版本")
    toolchain_version: Mapped[str] = mapped_column(comment="执行所需工具链与镜像版本")
    schema_name: Mapped[str | None] = mapped_column(comment="固定构建使用且不复用的物理 schema")
    profile_binding_id: Mapped[str | None] = mapped_column(comment="不含凭据的 profile 绑定引用")
    status: Mapped[str] = mapped_column(
        server_default=text("'QUEUED'::text"), comment="持久队列及公开状态的内部生命周期"
    )
    phase: Mapped[str] = mapped_column(server_default=text("'PREPARING'::text"), comment="构建准备、执行与验证阶段")
    run_lifecycle: Mapped[str | None] = mapped_column(comment="构建 run 的活动及清理生命周期")
    attempt_no: Mapped[int] = mapped_column(server_default=text("0"), comment="已经领取执行的次数")
    current_attempt_id: Mapped[StringUUID | None] = mapped_column(comment="唯一当前执行 attempt")
    available_at: Mapped[datetime] = mapped_column(
        server_default=text("clock_timestamp()"), comment="允许领取或重试的最早时间"
    )
    deadline_at: Mapped[datetime] = mapped_column(comment="所有尝试共享的最终截止时间")
    max_attempts: Mapped[int] = mapped_column(server_default=text("3"), comment="包含首次执行的受控最大尝试数")
    retry_policy: Mapped[str] = mapped_column(
        server_default=text("'PREPARATION_ONLY'::text"), comment="由受控绑定决定的安全重试类别"
    )
    error_code: Mapped[str | None] = mapped_column(comment="稳定错误诊断码")
    error_detail: Mapped[JsonDocument | None] = mapped_column(comment="有界且已脱敏的错误诊断")
    created_at: Mapped[datetime] = mapped_column(server_default=text("clock_timestamp()"), comment="受理记录创建时间")
    started_at: Mapped[datetime | None] = mapped_column(comment="首次实际领取时间")
    finished_at: Mapped[datetime | None] = mapped_column(comment="任务最终结束时间")
    branch_id: Mapped[StringUUID | None] = mapped_column(comment="发布、查询或草稿验证的固定分支；通用任务可为空")


class RuntimeAttempt(Base):
    """runtime_attempt 的持久字段；对外由 Store 返回记录快照。"""

    __tablename__ = "runtime_attempt"

    attempt_id: Mapped[StringUUID] = mapped_column(primary_key=True, comment="一次实际执行的标识")
    job_id: Mapped[StringUUID] = mapped_column(comment="执行所属任务")
    attempt_no: Mapped[int] = mapped_column(comment="任务内的执行序号")
    worker_id: Mapped[StringUUID] = mapped_column(comment="领取该执行的进程实例")
    lease_token: Mapped[StringUUID] = mapped_column(comment="该次执行唯一的状态写入凭证")
    lease_expires_at: Mapped[datetime] = mapped_column(comment="数据库时钟确定的执行租约截止时间")
    heartbeat_at: Mapped[datetime] = mapped_column(
        server_default=text("clock_timestamp()"), comment="最近一次有效续租时间"
    )
    execution_stage: Mapped[str] = mapped_column(
        server_default=text("'PREPARING'::text"), comment="区分外部引擎尚未调用和已经启动"
    )
    state: Mapped[str] = mapped_column(
        server_default=text("'EXECUTING'::text"), comment="执行成功失败或尚未确认停止的状态"
    )
    external_execution_refs: Mapped[JsonDocument] = mapped_column(
        server_default=text("'{}'::jsonb"), comment="用于取消核对的非敏感目标库会话标识"
    )
    stop_confirmed_at: Mapped[datetime | None] = mapped_column(comment="已确认子进程及外部执行结束的时间")
    started_at: Mapped[datetime] = mapped_column(server_default=text("clock_timestamp()"), comment="执行开始时间")
    finished_at: Mapped[datetime | None] = mapped_column(comment="执行终结或被判定失联的时间")


class ArtifactSet(Base):
    """runtime_artifact_set 的持久字段；对外由 Store 返回记录快照。"""

    __tablename__ = "runtime_artifact_set"

    set_id: Mapped[StringUUID] = mapped_column(primary_key=True, comment="不可变源码或输出集合标识")
    project_id: Mapped[str] = mapped_column(comment="所属逻辑项目")
    producer_attempt_id: Mapped[StringUUID | None] = mapped_column(comment="创建该集合的 attempt，管理导入为空")
    kind: Mapped[str] = mapped_column(
        comment="SOURCE 项目源、EXECUTION 执行产物、VALIDATION_INPUT 封存的有界 YAML 操作正文"
    )
    state: Mapped[str] = mapped_column(server_default=text("'STAGING'::text"), comment="暂存、封存或删除中状态")
    source_commit_sha: Mapped[str | None] = mapped_column(comment="固定 Git 提交标识")
    project_digest: Mapped[str | None] = mapped_column(comment="原有项目内容摘要")
    source_set_id: Mapped[StringUUID | None] = mapped_column(comment="执行集合对应的不可变源码")
    config_version: Mapped[str | None] = mapped_column(comment="恢复所需配置版本")
    toolchain_version: Mapped[str | None] = mapped_column(comment="恢复所需工具链版本")
    format_version: Mapped[str] = mapped_column(server_default=text("'1'::text"), comment="持久产物格式版本")
    content_digest: Mapped[str | None] = mapped_column(comment="按路径排序的原始文件摘要清单之摘要")
    file_count: Mapped[int] = mapped_column(server_default=text("0"), comment="集合文件数量")
    raw_bytes: Mapped[BigInt] = mapped_column(server_default=text("0"), comment="集合原始文件总字节数")
    validation_json: Mapped[JsonDocument] = mapped_column(server_default=text("'{}'::jsonb"), comment="发布验证证据")
    catalog_json: Mapped[JsonDocument] = mapped_column(
        server_default=text("'{}'::jsonb"), comment="从原生产物派生的可读取目录"
    )
    metadata_json: Mapped[JsonDocument] = mapped_column(
        "metadata", server_default=text("'{}'::jsonb"), comment="不含本地路径和凭据的附加产物元数据"
    )
    created_at: Mapped[datetime] = mapped_column(server_default=text("clock_timestamp()"), comment="集合创建时间")
    sealed_at: Mapped[datetime | None] = mapped_column(comment="原子封存时间")


class ArtifactFile(Base):
    """runtime_artifact_file 的持久字段；对外由 Store 返回记录快照。"""

    __tablename__ = "runtime_artifact_file"

    set_id: Mapped[StringUUID] = mapped_column(primary_key=True, comment="文件所属集合")
    relative_path: Mapped[str] = mapped_column(primary_key=True, comment="规范化 POSIX 相对路径")
    content: Mapped[bytes] = mapped_column(comment="原始或压缩后的文件字节")
    codec: Mapped[str] = mapped_column(comment="文件字节的 raw 或 gzip 编码")
    raw_sha256: Mapped[str] = mapped_column(comment="原始文件字节摘要")
    raw_size: Mapped[BigInt] = mapped_column(comment="原始文件字节数")
    stored_size: Mapped[BigInt] = mapped_column(comment="压缩后持久化字节数")
    media_type: Mapped[str] = mapped_column(
        server_default=text("'application/octet-stream'::text"), comment="文件媒体类型"
    )
    executable: Mapped[bool] = mapped_column(server_default=text("false"), comment="还原时需要保留的执行位")


class JobResult(Base):
    """runtime_job_result 的持久字段；对外由 Store 返回记录快照。"""

    __tablename__ = "runtime_job_result"

    job_id: Mapped[StringUUID] = mapped_column(primary_key=True, comment="结果所属任务")
    attempt_id: Mapped[StringUUID] = mapped_column(comment="获准提交结果的实际执行")
    payload_json: Mapped[JsonDocument] = mapped_column(
        server_default=text("'{}'::jsonb"), comment="遵循公开契约的列、行、SQL 和执行结果"
    )
    format_version: Mapped[str] = mapped_column(server_default=text("'1'::text"), comment="结果格式版本")
    stdout_tail: Mapped[str] = mapped_column(
        server_default=text("''::text"), comment="已脱敏且有大小上限的标准输出尾部"
    )
    stderr_tail: Mapped[str] = mapped_column(
        server_default=text("''::text"), comment="已脱敏且有大小上限的标准错误尾部"
    )
    exit_code: Mapped[int | None] = mapped_column(comment="实际子进程退出码")
    output_truncated: Mapped[bool] = mapped_column(server_default=text("false"), comment="诊断输出是否发生截断")
    created_at: Mapped[datetime] = mapped_column(server_default=text("clock_timestamp()"), comment="结果原子提交时间")


class Release(Base):
    """runtime_release 的持久字段；对外由 Store 返回记录快照。"""

    __tablename__ = "runtime_release"

    release_id: Mapped[StringUUID] = mapped_column(primary_key=True, comment="服务生成的不可变公开发布身份")
    project_id: Mapped[str] = mapped_column(comment="发布所属逻辑项目")
    sequence: Mapped[BigInt] = mapped_column(comment="项目内受理序号")
    idempotency_key: Mapped[str] = mapped_column(comment="固定候选输入的管理请求幂等键")
    request_json: Mapped[JsonDocument] = mapped_column(comment="固定源码摘要和配置引用，不含连接凭据")
    baseline_release_id: Mapped[StringUUID | None] = mapped_column(comment="候选受理时的活动基线")
    run_id: Mapped[StringUUID | None] = mapped_column(comment="执行本候选的固定构建任务")
    artifact_set_id: Mapped[StringUUID | None] = mapped_column(comment="同事务封存的完整输出集合")
    state: Mapped[str] = mapped_column(
        server_default=text("'PREPARING'::text"), comment="业务发布状态，不以执行成功替代发布成功"
    )
    build_mode: Mapped[str] = mapped_column(
        server_default=text("'FULL_BUILD'::text"), comment="经过验证的全构建或复用模式"
    )
    catalog_digest: Mapped[str | None] = mapped_column(comment="完整展示文件原始字节 SHA256")
    error_code: Mapped[str | None] = mapped_column(comment="发布前失败的稳定诊断码")
    created_at: Mapped[datetime] = mapped_column(server_default=text("clock_timestamp()"), comment="候选受理时间")
    published_at: Mapped[datetime | None] = mapped_column(comment="唯一发布事务完成时刻")
    branch_id: Mapped[StringUUID] = mapped_column(comment="所属分支实例，发布序号和幂等键均在此范围内")


class ReleaseRelation(Base):
    """runtime_release_relation 的持久字段；对外由 Store 返回记录快照。"""

    __tablename__ = "runtime_release_relation"

    release_id: Mapped[StringUUID] = mapped_column(primary_key=True, comment="绑定所属发布快照")
    native_id: Mapped[str] = mapped_column(primary_key=True, comment="绑定的原生模型或 source 身份")
    creator_run_id: Mapped[StringUUID | None] = mapped_column(comment="实际创建物理对象的 run，外部 source 为空")
    binding_json: Mapped[JsonDocument] = mapped_column(comment="物理名称、复用模式、摘要与验证时间")


class Branch(Base):
    """runtime_branch 的持久字段；对外由 Store 返回记录快照。"""

    __tablename__ = "runtime_branch"

    branch_id: Mapped[StringUUID] = mapped_column(primary_key=True, comment="分支实例身份；删除后同名重建必须生成新值")
    project_id: Mapped[str] = mapped_column(comment="所属逻辑项目，参与所有跨表归属校验")
    git_ref: Mapped[str] = mapped_column(comment="受控仓库内完整分支引用")
    mode: Mapped[str] = mapped_column(comment="生产或开发预览的绑定用途")
    status: Mapped[str] = mapped_column(comment="分支创建、使用、删除及恢复状态")
    base_commit_sha: Mapped[str | None] = mapped_column(comment="登记时固定的差异比较源码基线")
    base_release_id: Mapped[StringUUID | None] = mapped_column(
        comment="与固定源码提交匹配的资源目录基线，没有对应发布时为空"
    )
    observed_head_sha: Mapped[str | None] = mapped_column(comment="最近核实的远端分支提交")
    active_release_id: Mapped[StringUUID | None] = mapped_column(comment="本分支唯一当前发布，只由封存事务推进")
    latest_release_id: Mapped[StringUUID | None] = mapped_column(comment="本分支最近受理的候选，不代表发布成功")
    publication_sequence: Mapped[BigInt] = mapped_column(
        server_default=text("0"), comment="本分支最近受理序号，阻止旧候选覆盖新输入"
    )
    version: Mapped[BigInt] = mapped_column(server_default=text("1"), comment="生命周期乐观锁版本")
    binding_config: Mapped[JsonDocument] = mapped_column(
        server_default=text("'{}'::jsonb"), comment="服务控制的执行绑定引用，不含凭据"
    )
    config_version: Mapped[str] = mapped_column(comment="执行绑定的配置版本")
    operation_key: Mapped[str | None] = mapped_column(comment="创建或登记操作的项目内幂等键")
    operation_json: Mapped[JsonDocument | None] = mapped_column(comment="固定操作输入，用于重试冲突检查和中断恢复")
    signal_version: Mapped[BigInt] = mapped_column(server_default=text("0"), comment="持久化待核对信号版本")
    processed_signal_version: Mapped[BigInt] = mapped_column(
        server_default=text("0"), comment="已核对的信号版本，避免扫描期间新信号丢失"
    )
    scan_token: Mapped[StringUUID | None] = mapped_column(comment="当前扫描租约身份")
    scan_expires_at: Mapped[datetime | None] = mapped_column(comment="扫描租约到期时间")
    created_at: Mapped[datetime] = mapped_column(server_default=text("clock_timestamp()"), comment="分支身份登记时间")
    base_input_set_id: Mapped[StringUUID | None] = mapped_column(
        comment="登记时封存的固定源码基线，删除分支后仍保留引用"
    )
    production_base_release_id: Mapped[StringUUID | None] = mapped_column(
        comment="登记时观察到的生产发布，仅用于判断生产是否推进"
    )


class BranchEvent(Base):
    """runtime_branch_event 的持久字段；对外由 Store 返回记录快照。"""

    __tablename__ = "runtime_branch_event"

    delivery_id: Mapped[str] = mapped_column(primary_key=True, comment="Forgejo webhook delivery 身份")
    payload_digest: Mapped[str] = mapped_column(comment="事件正文摘要，防止同身份不同输入")
    received_at: Mapped[datetime] = mapped_column(
        server_default=text("clock_timestamp()"), comment="首次收到事件的时间"
    )


class ExecutionBinding(Base):
    """engine_execution_binding 的持久字段；对外由 Store 返回记录快照。"""

    __tablename__ = "engine_execution_binding"

    repository: Mapped[str] = mapped_column(primary_key=True, comment="受控规范仓库地址")
    execution_binding: Mapped[str] = mapped_column(primary_key=True, comment="可复用执行配置名称")
    config_version: Mapped[str] = mapped_column(primary_key=True, comment="不可变执行配置版本")
    config_json: Mapped[JsonDocument] = mapped_column(comment="执行配置，不含凭据正文")


class Build(Base):
    """engine_build 的持久字段；对外由 Store 返回记录快照。"""

    __tablename__ = "engine_build"

    build_id: Mapped[StringUUID] = mapped_column(primary_key=True, comment="一次构建身份")
    run_id: Mapped[StringUUID] = mapped_column(comment="内部执行记录关联，不公开为构建身份")
    repository: Mapped[str] = mapped_column(comment="受控规范仓库地址")
    branch_name: Mapped[str | None] = mapped_column(comment="Git 短分支名")
    environment: Mapped[str] = mapped_column(comment="物理执行环境")
    execution_binding: Mapped[str] = mapped_column(comment="可复用执行配置名称")
    config_version: Mapped[str] = mapped_column(comment="不可变执行配置版本")
    toolchain_version: Mapped[str] = mapped_column(comment="固定工具链版本")
    caller: Mapped[str] = mapped_column(comment="可信服务调用来源")
    idempotency_key: Mapped[str] = mapped_column(comment="调用方稳定请求键")
    request_digest: Mapped[str] = mapped_column(comment="规范化输入摘要")
    request_json: Mapped[JsonDocument] = mapped_column(comment="原始规范化请求")
    config_snapshot: Mapped[JsonDocument] = mapped_column(comment="受理时固定的执行语义配置")
    requested_commit_sha: Mapped[str | None] = mapped_column(comment="请求显式指定的提交")
    commit_sha: Mapped[str | None] = mapped_column(comment="实际固定源码提交")
    build_status: Mapped[str] = mapped_column(server_default=text("'QUEUED'::text"), comment="构建终态，不代表部署成功")
    phase: Mapped[str] = mapped_column(server_default=text("'RESOLVING_SOURCE'::text"), comment="执行进度阶段")
    cancel_requested: Mapped[bool] = mapped_column(server_default=text("false"), comment="持久取消意图")
    output_set_id: Mapped[StringUUID | None] = mapped_column(comment="可靠封存产物引用")
    catalog_digest: Mapped[str | None] = mapped_column(comment="封存目录字节摘要")
    error_code: Mapped[str | None] = mapped_column(comment="脱敏稳定错误码")
    version: Mapped[BigInt] = mapped_column(server_default=text("1"), comment="对象递增版本")
    created_at: Mapped[datetime] = mapped_column(server_default=text("clock_timestamp()"), comment="受理时间")
    updated_at: Mapped[datetime] = mapped_column(server_default=text("clock_timestamp()"), comment="最近事实变更时间")
    finished_at: Mapped[datetime | None] = mapped_column(comment="执行终态时间")


class DeploymentTarget(Base):
    """engine_deployment_target 的持久字段；对外由 Store 返回记录快照。"""

    __tablename__ = "engine_deployment_target"

    repository: Mapped[str] = mapped_column(primary_key=True, comment="受控规范仓库地址")
    environment: Mapped[str] = mapped_column(primary_key=True, comment="物理执行环境")
    branch_name: Mapped[str] = mapped_column(primary_key=True, comment="Git 短分支名")
    version: Mapped[BigInt] = mapped_column(server_default=text("0"), comment="对象递增版本")
    desired_generation: Mapped[BigInt] = mapped_column(server_default=text("0"), comment="最新受理的部署意图序号")
    active_build_id: Mapped[StringUUID | None] = mapped_column(comment="当前有效构建指针")
    observed_head_sha: Mapped[str | None] = mapped_column(comment="最近观察的远端提交")
    head_observed_at: Mapped[datetime | None] = mapped_column(comment="远端观察时间")
    source_state: Mapped[str] = mapped_column(
        server_default=text("'UNDEPLOYED'::text"), comment="远端相对部署的观察状态"
    )


class DeploymentAttempt(Base):
    """engine_deployment_attempt 的持久字段；对外由 Store 返回记录快照。"""

    __tablename__ = "engine_deployment_attempt"

    repository: Mapped[str] = mapped_column(primary_key=True, comment="受控规范仓库地址")
    environment: Mapped[str] = mapped_column(primary_key=True, comment="物理执行环境")
    branch_name: Mapped[str] = mapped_column(primary_key=True, comment="Git 短分支名")
    generation: Mapped[BigInt] = mapped_column(primary_key=True, comment="按受理顺序分配的部署序号")
    build_id: Mapped[StringUUID] = mapped_column(comment="一次构建身份")
    caller: Mapped[str] = mapped_column(comment="可信服务调用来源")
    idempotency_key: Mapped[str] = mapped_column(comment="调用方稳定请求键")
    request_digest: Mapped[str] = mapped_column(comment="规范化输入摘要")
    operation: Mapped[str] = mapped_column(
        server_default=text("'DEPLOYMENT'::text"), comment="自动部署与独立部署的幂等操作域"
    )
    deployment_status: Mapped[str] = mapped_column(comment="部署意图状态")
    reason: Mapped[str | None] = mapped_column(comment="稳定状态原因")
    version: Mapped[BigInt] = mapped_column(server_default=text("1"), comment="对象递增版本")
    created_at: Mapped[datetime] = mapped_column(server_default=text("clock_timestamp()"), comment="受理时间")
    deployed_at: Mapped[datetime | None] = mapped_column(comment="实际切换时间")
    last_checked_at: Mapped[datetime | None] = mapped_column(comment="内部公平调度的最近扫描时间，不生成业务变化")


class ChangeCounter(Base):
    """engine_change_counter 的持久字段；对外由 Store 返回记录快照。"""

    __tablename__ = "engine_change_counter"

    singleton: Mapped[bool] = mapped_column(primary_key=True, server_default=text("true"), comment="变化流计数器唯一行")
    value: Mapped[BigInt] = mapped_column(comment="最后同事务分配的变化序号")


class Change(Base):
    """engine_change 的持久字段；对外由 Store 返回记录快照。"""

    __tablename__ = "engine_change"

    sequence: Mapped[BigInt] = mapped_column(primary_key=True, comment="提交顺序可重放变化序号")
    repository: Mapped[str] = mapped_column(comment="受控规范仓库地址")
    object_type: Mapped[str] = mapped_column(comment="引擎事实类型")
    object_id: Mapped[str] = mapped_column(comment="变化对象自然身份")
    object_version: Mapped[BigInt] = mapped_column(comment="变化对应对象版本")
    summary: Mapped[JsonDocument] = mapped_column(comment="完整有界事实摘要，不含执行秘密")
