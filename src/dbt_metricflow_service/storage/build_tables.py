"""BuildStore 的 Core 表映射；仅用于语句构造，数据库结构仍由 Alembic 管理。"""

from sqlalchemy import BigInteger, Boolean, Column, DateTime, MetaData, Table, Text
from sqlalchemy.dialects.postgresql import JSONB, UUID

# 不执行 create_all 或反射；关联旧表仅声明当前用例读写的列。
METADATA = MetaData()
BUILD_TABLE_NAME = "engine_build"

# 完整构建投影与 StoredBuild 一致，UUID 延续存储层的字符串契约。
BUILD = Table(
    BUILD_TABLE_NAME,
    METADATA,
    Column("build_id", UUID(as_uuid=False), primary_key=True, comment="公开构建身份"),
    Column("run_id", UUID(as_uuid=False), comment="内部执行任务身份"),
    Column("repository", Text, nullable=False, comment="规范仓库地址"),
    Column("branch_name", Text, comment="来源分支"),
    Column("environment", Text, nullable=False, comment="构建执行环境"),
    Column("execution_binding", Text, nullable=False, comment="执行配置名称"),
    Column("config_version", Text, nullable=False, comment="固定配置版本"),
    Column("toolchain_version", Text, nullable=False, comment="固定工具链版本"),
    Column("caller", Text, nullable=False, comment="可信调用来源"),
    Column("idempotency_key", Text, nullable=False, comment="受理幂等键"),
    Column("request_digest", Text, nullable=False, comment="请求摘要"),
    Column("request_json", JSONB, nullable=False, comment="已校验请求副本"),
    Column("config_snapshot", JSONB, nullable=False, comment="固定执行配置"),
    Column("requested_commit_sha", Text, comment="请求指定源码版本"),
    Column("commit_sha", Text, comment="实际固定源码版本"),
    Column("build_status", Text, nullable=False, comment="构建状态"),
    Column("phase", Text, nullable=False, comment="执行阶段"),
    Column("cancel_requested", Boolean, nullable=False, comment="持久取消意图"),
    Column("output_set_id", UUID(as_uuid=False), comment="输出产物集合"),
    Column("catalog_digest", Text, comment="封存目录摘要"),
    Column("error_code", Text, comment="稳定错误码"),
    Column("source_incomplete", Boolean, nullable=False, comment="历史来源证据是否不足"),
    Column("version", BigInteger, nullable=False, comment="构建事实版本"),
    Column("created_at", DateTime(timezone=True), nullable=False, comment="受理时间"),
    Column("updated_at", DateTime(timezone=True), nullable=False, comment="最近更新时间"),
    Column("finished_at", DateTime(timezone=True), comment="终态时间"),
)

# 配置按仓库、执行绑定和版本定位，插入时显式列出字段。
BINDING = Table(
    "engine_execution_binding",
    METADATA,
    Column("repository", Text, primary_key=True, comment="目标仓库"),
    Column("execution_binding", Text, primary_key=True, comment="执行绑定名称"),
    Column("config_version", Text, primary_key=True, comment="不可变配置版本"),
    Column("config_json", JSONB, nullable=False, comment="不含凭据的执行配置"),
)

# 旧运行时表的局部映射，未声明列继续使用数据库默认值。
PROJECT = Table(
    "runtime_project",
    METADATA,
    Column("project_id", Text, primary_key=True, comment="内部执行容器身份"),
    Column("binding_config", JSONB, comment="固定执行配置"),
    Column("config_version", Text, comment="固定配置版本"),
)
JOB = Table(
    "runtime_job",
    METADATA,
    Column("job_id", UUID(as_uuid=False), primary_key=True, comment="内部任务身份"),
    Column("request_json", JSONB, comment="执行请求及固定源码版本"),
    Column("status", Text, comment="执行状态"),
    Column("error_code", Text, comment="稳定错误码"),
    Column("run_lifecycle", Text, comment="物理对象生命周期"),
    Column("finished_at", DateTime(timezone=True), comment="任务终态时间"),
)

# 日志仅投影持久变化序号和安全摘要。
CHANGE = Table(
    "engine_change",
    METADATA,
    Column("sequence", BigInteger, primary_key=True, comment="变化流序号"),
    Column("object_type", Text, comment="变化对象类型"),
    Column("object_id", Text, comment="变化对象身份"),
    Column("summary", JSONB, comment="脱敏事实摘要"),
)
