from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

DEFAULT_PROJECTS_ROOT = "projects"
DEFAULT_PROFILES_DIR = "profiles"
DEFAULT_JOB_ARTIFACTS_ROOT = "job-artifacts"
DEFAULT_PLATFORM_DB_PATH = "platform-jobs.sqlite"
PLATFORM_BINDINGS_FILE_ENV = "PLATFORM_BINDINGS_FILE"
DEFAULT_COMMAND_TIMEOUT_SECONDS = 1800
DEFAULT_MAX_OUTPUT_BYTES = 1_048_576
MAX_OUTPUT_BYTES_ENV = "MAX_OUTPUT_BYTES"
# 默认启动文件独立于调用者的工作目录，部署时可用环境变量替换。
CONFIG_FILE_ENV = "SERVICE_CONFIG_FILE"
DEFAULT_CONFIG_FILE = Path(__file__).resolve().parents[2] / "config" / "service.yaml"
UTF8 = "utf-8"
CONFIG_KEYS = frozenset({
    "SERVICE_HOST", "SERVICE_PORT", "PROJECTS_ROOT", "DBT_PROFILES_DIR", "COMMAND_TIMEOUT_SECONDS",
    "MAX_OUTPUT_BYTES", "JOB_ARTIFACTS_ROOT", "PLATFORM_BINDINGS_FILE", "PLATFORM_DB_PATH",
    "SERVICE_DATABASE_URL", "SERVICE_TEMP_ROOT", "WORKER_CONCURRENCY", "JOB_LEASE_SECONDS",
    "JOB_HEARTBEAT_SECONDS", "SERVICE_CONFIG_VERSION", "SERVICE_TOOLCHAIN_VERSION", "MAX_RESULT_BYTES",
    "MAX_ARTIFACT_FILE_BYTES", "MAX_ARTIFACT_BYTES", "SYNCHRONOUS_WAIT_SECONDS",
})


@dataclass(frozen=True, slots=True)
class Settings:
    """Runtime paths and resource limits for one service process."""

    # Root containing dbt project directories addressable by project ID.
    projects_root: Path
    # Directory containing the mounted dbt profiles.yml file.
    profiles_dir: Path
    # Maximum wall-clock duration allowed for one CLI subprocess.
    command_timeout_seconds: int
    # Maximum number of bytes retained for each subprocess output stream.
    max_output_bytes: int
    # Service-owned root for task-isolated derived dbt and MetricFlow artifacts.
    job_artifacts_root: Path = Path(DEFAULT_JOB_ARTIFACTS_ROOT)
    # 服务拥有的项目 Git 与 profile 绑定配置；不由 HTTP 请求覆盖。
    platform_bindings_file: Path | None = None
    # 平台构建与查询的可重启 SQLite 任务索引。
    platform_db_path: Path = Path(DEFAULT_PLATFORM_DB_PATH)
    # 启用 PostgreSQL 运行时的连接信息；repr 不包含凭据。
    database_url: str | None = field(default=None, repr=False)
    # 可丢弃的实例本地工作目录，不作为持久产物定位符。
    temp_root: Path = Path("runtime-tmp")
    # 本机同时执行的任务数，临时 resources 同样受此上限约束。
    worker_concurrency: int = 2
    # 数据库租约有效期及续租间隔，单位秒。
    lease_seconds: int = 90
    heartbeat_seconds: int = 15
    # 当前实例可执行的连接配置版本。
    config_version: str = "1"
    # 部署镜像可显式指定工具链标识，默认由安装版本及服务代码计算。
    toolchain_version: str | None = None
    # 单结果、单文件和单产物集的原始字节上限。
    max_result_bytes: int = 16 * 1024 * 1024
    max_artifact_file_bytes: int = 64 * 1024 * 1024
    max_artifact_bytes: int = 256 * 1024 * 1024
    # 同步选项/清理接口等待持久任务的最大秒数。
    synchronous_wait_seconds: int = 30
    # HTTP 服务监听地址与端口，不影响管理命令的数据库连接。
    server_host: str = "0.0.0.0"
    server_port: int = 8000

    def __post_init__(self) -> None:
        # 监听配置错误应在创建服务前暴露。
        if not self.server_host or not 1 <= self.server_port <= 65535:
            raise ValueError("server host must be nonempty and port must be between 1 and 65535")
        # 拒绝无法在租约到期前及时自停的配置。
        if self.worker_concurrency < 1 or self.heartbeat_seconds <= 0:
            raise ValueError("worker concurrency and heartbeat must be positive")
        if self.lease_seconds < self.heartbeat_seconds * 3:
            raise ValueError("lease must cover at least three heartbeat intervals")
        if min(self.max_result_bytes, self.max_artifact_file_bytes, self.max_artifact_bytes) <= 0:
            raise ValueError("runtime storage limits must be positive")

    @classmethod
    def from_file(cls, config_file: Path | None = None) -> Settings:
        """服务和管理命令共享启动文件，环境变量覆盖文件中的配置。"""
        # 显式文件优先于环境变量选择的文件，未指定时加载仓库默认文件。
        selected = (
            config_file if config_file is not None else Path(os.getenv(CONFIG_FILE_ENV, str(DEFAULT_CONFIG_FILE)))
        )
        return cls.from_environment(config_file=selected)

    @classmethod
    def from_environment(cls, *, config_file: Path | None = None) -> Settings:
        """Build settings from environment, optionally over a YAML configuration file."""
        # 安全读取单层配置；拒绝拼错的 key 和复合类型，不在错误中输出配置内容。
        config = {}
        base_dir = Path.cwd()
        if config_file is not None:
            config_file = config_file.resolve()
            base_dir = config_file.parent
            try:
                config = yaml.safe_load(config_file.read_text(encoding=UTF8))
            except yaml.YAMLError:
                raise ValueError("服务配置文件不是有效 YAML") from None
            if not isinstance(config, dict) or not config.keys() <= CONFIG_KEYS:
                raise ValueError("服务配置必须是配置项映射，且不能包含未知配置项")
            if any(value is not None and type(value) not in (str, int) for value in config.values()):
                raise ValueError("服务配置值只能是字符串、整数或 null")

        # 环境变量具有最高优先级；文件中的 null 使用该配置项的默认值。
        def value(name: str, default: str | None = None) -> str | None:
            configured = config.get(name)
            return os.getenv(name, str(configured) if configured is not None else default)

        # 文件路径以文件目录为基准，环境变量路径保留原有工作目录语义。
        def path(name: str, default: str) -> Path:
            configured = Path(value(name, default))
            return (configured if name in os.environ else base_dir / configured).resolve()

        return cls(
            projects_root=path("PROJECTS_ROOT", DEFAULT_PROJECTS_ROOT),
            profiles_dir=path("DBT_PROFILES_DIR", DEFAULT_PROFILES_DIR),
            command_timeout_seconds=int(
                value("COMMAND_TIMEOUT_SECONDS", str(DEFAULT_COMMAND_TIMEOUT_SECONDS))
            ),
            max_output_bytes=int(value(MAX_OUTPUT_BYTES_ENV, str(DEFAULT_MAX_OUTPUT_BYTES))),
            job_artifacts_root=path("JOB_ARTIFACTS_ROOT", DEFAULT_JOB_ARTIFACTS_ROOT),
            platform_bindings_file=(
                path(PLATFORM_BINDINGS_FILE_ENV, "") if value(PLATFORM_BINDINGS_FILE_ENV) else None
            ),
            platform_db_path=path("PLATFORM_DB_PATH", DEFAULT_PLATFORM_DB_PATH),
            database_url=value("SERVICE_DATABASE_URL") or None,
            temp_root=path("SERVICE_TEMP_ROOT", "runtime-tmp"),
            worker_concurrency=int(value("WORKER_CONCURRENCY", "2")),
            lease_seconds=int(value("JOB_LEASE_SECONDS", "90")),
            heartbeat_seconds=int(value("JOB_HEARTBEAT_SECONDS", "15")),
            config_version=value("SERVICE_CONFIG_VERSION", "1"),
            toolchain_version=value("SERVICE_TOOLCHAIN_VERSION") or None,
            max_result_bytes=int(value("MAX_RESULT_BYTES", str(16 * 1024 * 1024))),
            max_artifact_file_bytes=int(value("MAX_ARTIFACT_FILE_BYTES", str(64 * 1024 * 1024))),
            max_artifact_bytes=int(value("MAX_ARTIFACT_BYTES", str(256 * 1024 * 1024))),
            synchronous_wait_seconds=int(value("SYNCHRONOUS_WAIT_SECONDS", "30")),
            server_host=value("SERVICE_HOST", "0.0.0.0"),
            server_port=int(value("SERVICE_PORT", "8000")),
        )
