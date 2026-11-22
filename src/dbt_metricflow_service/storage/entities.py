"""当前业务表的 ORM 映射；结构、约束和触发器由 schema.sql 定义。"""

from datetime import datetime
from typing import Annotated

from sqlalchemy import BigInteger, DateTime, Text, text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from dbt_metricflow_service import constants as strings
from dbt_metricflow_service.models.contracts import JsonObject

# 复用真正重复的列类型，字段仍显式声明名称、默认值和业务含义。
StringUUID = Annotated[str, mapped_column(UUID(as_uuid=False))]
BigInt = Annotated[int, mapped_column(BigInteger)]
JsonDocument = Annotated[JsonObject, mapped_column(JSONB(none_as_null=True))]
EXECUTION_LOG_TABLE = "runtime_execution_log"


class Base(DeclarativeBase):
    """统一文本和带时区时间类型；可空性由 Mapped 的 Optional 注解推导。"""

    type_annotation_map = {str: Text, datetime: DateTime(timezone=True)}


class RuntimeAttempt(Base):
    """runtime_attempt 的持久字段；对外由 Store 返回记录快照。"""

    __tablename__ = strings.RUNTIME_ATTEMPT_TABLE

    attempt_id: Mapped[StringUUID] = mapped_column(primary_key=True, comment="一次实际执行的标识")
    build_id: Mapped[StringUUID] = mapped_column(comment="所属构建")
    attempt_no: Mapped[int] = mapped_column(comment="任务内的执行序号")
    worker_id: Mapped[StringUUID] = mapped_column(comment="领取该执行的进程实例")
    lease_token: Mapped[StringUUID] = mapped_column(comment="该次执行唯一的状态写入凭证")
    lease_expires_at: Mapped[datetime] = mapped_column(comment="数据库时钟确定的执行租约截止时间")
    heartbeat_at: Mapped[datetime] = mapped_column(
        server_default=text(strings.SQL_CLOCK_TIMESTAMP), comment="最近一次有效续租时间"
    )
    execution_stage: Mapped[str] = mapped_column(
        server_default=text(strings.SQL_PREPARING), comment="区分外部引擎尚未调用和已经启动"
    )
    state: Mapped[str] = mapped_column(
        server_default=text(strings.SQL_EXECUTING), comment="执行成功失败或尚未确认停止的状态"
    )
    external_execution_refs: Mapped[JsonDocument] = mapped_column(
        server_default=text(strings.SQL_EMPTY_JSON), comment="用于取消核对的非敏感目标库会话标识"
    )
    stop_confirmed_at: Mapped[datetime | None] = mapped_column(comment="已确认子进程及外部执行结束的时间")
    started_at: Mapped[datetime] = mapped_column(
        server_default=text(strings.SQL_CLOCK_TIMESTAMP), comment="执行开始时间"
    )
    finished_at: Mapped[datetime | None] = mapped_column(comment="执行终结或被判定失联的时间")


class RuntimeExecutionLog(Base):
    """runtime_execution_log 保存脱敏后的 dbt 详细事件，与阶段记录独立。"""

    __tablename__ = EXECUTION_LOG_TABLE

    log_id: Mapped[BigInt] = mapped_column(
        primary_key=True, server_default=text("nextval('runtime_execution_log_log_id_seq'::regclass)"),
        comment="日志递增标识；同一次命令按采集顺序写入",
    )
    build_id: Mapped[StringUUID] = mapped_column(comment="日志所属构建")
    job_id: Mapped[StringUUID] = mapped_column(comment="日志 API 的任务标识；旧日志保留历史值")
    attempt_id: Mapped[StringUUID] = mapped_column(comment="所属执行尝试，区分重试")
    phase: Mapped[str] = mapped_column(comment="采集时的任务阶段")
    command: Mapped[str] = mapped_column(comment="dbt 子命令，不保存命令参数")
    event_time: Mapped[datetime] = mapped_column(comment="事件原始 UTC 时间")
    level: Mapped[str] = mapped_column(comment="事件级别，包含 debug")
    event_name: Mapped[str] = mapped_column(comment="dbt 事件名或 Python logger 名称")
    event_code: Mapped[str] = mapped_column(comment="dbt 事件代码，普通日志为空字符串")
    invocation_id: Mapped[str] = mapped_column(comment="dbt 调用标识，普通日志为空字符串")
    node_id: Mapped[str] = mapped_column(comment="节点 unique_id，命令级事件为空字符串")
    thread_name: Mapped[str] = mapped_column(comment="事件产生线程")
    message: Mapped[str] = mapped_column(comment="脱敏后的完整消息，不受 stdout 尾部上限限制")
    created_at: Mapped[datetime] = mapped_column(
        server_default=text(strings.SQL_CLOCK_TIMESTAMP), comment="数据库记录写入时间",
    )


class ArtifactSet(Base):
    """runtime_artifact_set 的持久字段；对外由 Store 返回记录快照。"""

    __tablename__ = strings.RUNTIME_ARTIFACT_SET_TABLE

    set_id: Mapped[StringUUID] = mapped_column(primary_key=True, comment="不可变源码或输出集合标识")
    project_id: Mapped[str] = mapped_column(comment="所属逻辑项目")
    producer_attempt_id: Mapped[StringUUID | None] = mapped_column(comment="创建该集合的 attempt，管理导入为空")
    kind: Mapped[str] = mapped_column(
        comment="SOURCE 项目源、EXECUTION 执行产物"
    )
    state: Mapped[str] = mapped_column(server_default=text(strings.SQL_STAGING), comment="暂存、封存或删除中状态")
    source_commit_sha: Mapped[str | None] = mapped_column(comment="固定 Git 提交标识")
    project_digest: Mapped[str | None] = mapped_column(comment="原有项目内容摘要")
    source_set_id: Mapped[StringUUID | None] = mapped_column(comment="执行集合对应的不可变源码")
    toolchain_version: Mapped[str | None] = mapped_column(comment="恢复所需工具链版本")
    format_version: Mapped[str] = mapped_column(
        server_default=text(strings.SQL_VERSION_ONE), comment="持久产物格式版本"
    )
    content_digest: Mapped[str | None] = mapped_column(comment="按路径排序的原始文件摘要清单之摘要")
    file_count: Mapped[int] = mapped_column(server_default=text(strings.SQL_ZERO), comment="集合文件数量")
    raw_bytes: Mapped[BigInt] = mapped_column(server_default=text(strings.SQL_ZERO), comment="集合原始文件总字节数")
    created_at: Mapped[datetime] = mapped_column(
        server_default=text(strings.SQL_CLOCK_TIMESTAMP), comment="集合创建时间"
    )
    sealed_at: Mapped[datetime | None] = mapped_column(comment="原子封存时间")


class ArtifactFile(Base):
    """runtime_artifact_file 的持久字段；对外由 Store 返回记录快照。"""

    __tablename__ = strings.RUNTIME_ARTIFACT_FILE_TABLE

    set_id: Mapped[StringUUID] = mapped_column(primary_key=True, comment="文件所属集合")
    relative_path: Mapped[str] = mapped_column(primary_key=True, comment="规范化 POSIX 相对路径")
    content: Mapped[bytes] = mapped_column(comment="原始或压缩后的文件字节")
    codec: Mapped[str] = mapped_column(comment="文件字节的 raw 或 gzip 编码")
    raw_sha256: Mapped[str] = mapped_column(comment="原始文件字节摘要")
    raw_size: Mapped[BigInt] = mapped_column(comment="原始文件字节数")
    stored_size: Mapped[BigInt] = mapped_column(comment="压缩后持久化字节数")
    media_type: Mapped[str] = mapped_column(
        server_default=text(strings.SQL_BINARY_MEDIA_TYPE), comment="文件媒体类型"
    )
    executable: Mapped[bool] = mapped_column(server_default=text(strings.SQL_FALSE), comment="还原时需要保留的执行位")


class Build(Base):
    """engine_build 的持久字段；对外由 Store 返回记录快照。"""

    __tablename__ = strings.ENGINE_BUILD_TABLE

    build_id: Mapped[StringUUID] = mapped_column(primary_key=True, comment="一次构建身份")
    project_id: Mapped[str] = mapped_column(comment="源码与输出集合的归属标识")
    input_set_id: Mapped[StringUUID | None] = mapped_column(comment="固定源码集合")
    project_digest: Mapped[str | None] = mapped_column(comment="固定源码摘要")
    started_at: Mapped[datetime | None] = mapped_column(comment="首次领取时间")
    error_detail: Mapped[JsonDocument | None] = mapped_column(comment="脱敏执行诊断")
    attempt_no: Mapped[int] = mapped_column(server_default=text(strings.SQL_ZERO), comment="已经领取执行的次数")
    current_attempt_id: Mapped[StringUUID | None] = mapped_column(comment="唯一当前执行 attempt")
    available_at: Mapped[datetime] = mapped_column(
        server_default=text(strings.SQL_CLOCK_TIMESTAMP), comment="允许领取或重试的最早时间"
    )
    max_attempts: Mapped[int] = mapped_column(
        server_default=text(strings.SQL_THREE), comment="包含首次执行的受控最大尝试数"
    )
    retry_policy: Mapped[str] = mapped_column(
        server_default=text(strings.SQL_PREPARATION_ONLY), comment="由受控绑定决定的安全重试类别"
    )
    repository: Mapped[str] = mapped_column(comment="受控规范仓库地址")
    branch_name: Mapped[str | None] = mapped_column(comment="Git 短分支名")
    environment: Mapped[str] = mapped_column(comment="物理执行环境")
    environment_id: Mapped[StringUUID] = mapped_column(comment="外部环境稳定身份")
    toolchain_version: Mapped[str] = mapped_column(comment="固定工具链版本")
    caller: Mapped[str] = mapped_column(comment="可信服务调用来源")
    idempotency_key: Mapped[StringUUID] = mapped_column(unique=True, comment="调用方分配的全局唯一构建 UUID")
    request_json: Mapped[JsonDocument] = mapped_column(comment="原始规范化请求")
    commit_sha: Mapped[str | None] = mapped_column(comment="实际固定源码提交")
    build_status: Mapped[str] = mapped_column(
        server_default=text(strings.SQL_QUEUED), comment="构建终态，不代表部署成功"
    )
    phase: Mapped[str] = mapped_column(server_default=text(strings.SQL_RESOLVING_SOURCE), comment="执行进度阶段")
    output_set_id: Mapped[StringUUID | None] = mapped_column(comment="可靠封存产物引用")
    error_code: Mapped[str | None] = mapped_column(comment="脱敏稳定错误码")
    version: Mapped[BigInt] = mapped_column(
        server_default=text(strings.SQL_ONE), onupdate=text("version + 1"), comment="对象递增版本"
    )
    created_at: Mapped[datetime] = mapped_column(server_default=text(strings.SQL_CLOCK_TIMESTAMP), comment="受理时间")
    updated_at: Mapped[datetime] = mapped_column(
        server_default=text(strings.SQL_CLOCK_TIMESTAMP), onupdate=text(strings.SQL_CLOCK_TIMESTAMP),
        comment="最近事实变更时间",
    )
    finished_at: Mapped[datetime | None] = mapped_column(comment="执行终态时间")


class DeploymentTarget(Base):
    """engine_deployment_target 的持久字段；对外由 Store 返回记录快照。"""

    __tablename__ = strings.ENGINE_DEPLOYMENT_TARGET_TABLE

    repository: Mapped[str] = mapped_column(primary_key=True, comment="受控规范仓库地址")
    environment: Mapped[str] = mapped_column(primary_key=True, comment="物理执行环境")
    branch_name: Mapped[str] = mapped_column(primary_key=True, comment="Git 短分支名")
    version: Mapped[BigInt] = mapped_column(server_default=text(strings.SQL_ZERO), comment="对象递增版本")
    desired_generation: Mapped[BigInt] = mapped_column(
        server_default=text(strings.SQL_ZERO), comment="最新受理的部署意图序号"
    )
    active_build_id: Mapped[StringUUID | None] = mapped_column(comment="当前有效构建指针")
    observed_head_sha: Mapped[str | None] = mapped_column(comment="最近观察的远端提交")
    head_observed_at: Mapped[datetime | None] = mapped_column(comment="远端观察时间")
    source_state: Mapped[str] = mapped_column(
        server_default=text(strings.SQL_UNDEPLOYED), comment="远端相对部署的观察状态"
    )


class DeploymentAttempt(Base):
    """engine_deployment_attempt 的持久字段；对外由 Store 返回记录快照。"""

    __tablename__ = strings.ENGINE_DEPLOYMENT_ATTEMPT_TABLE

    repository: Mapped[str] = mapped_column(primary_key=True, comment="受控规范仓库地址")
    environment: Mapped[str] = mapped_column(primary_key=True, comment="物理执行环境")
    branch_name: Mapped[str] = mapped_column(primary_key=True, comment="Git 短分支名")
    generation: Mapped[BigInt] = mapped_column(primary_key=True, comment="按受理顺序分配的部署序号")
    build_id: Mapped[StringUUID] = mapped_column(comment="一次构建身份")
    caller: Mapped[str] = mapped_column(comment="可信服务调用来源")
    idempotency_key: Mapped[str] = mapped_column(comment="调用方稳定请求键")
    operation: Mapped[str] = mapped_column(
        server_default=text(strings.SQL_BUILD), comment="历史部署来源；新意图固定为构建自动部署 BUILD"
    )
    deployment_status: Mapped[str] = mapped_column(comment="部署意图状态")
    reason: Mapped[str | None] = mapped_column(comment="稳定状态原因")
    version: Mapped[BigInt] = mapped_column(server_default=text(strings.SQL_ONE), comment="对象递增版本")
    created_at: Mapped[datetime] = mapped_column(server_default=text(strings.SQL_CLOCK_TIMESTAMP), comment="受理时间")
    deployed_at: Mapped[datetime | None] = mapped_column(comment="实际切换时间")
    last_checked_at: Mapped[datetime | None] = mapped_column(comment="内部公平调度的最近扫描时间，不生成业务变化")
