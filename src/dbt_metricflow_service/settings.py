from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_PROJECTS_ROOT = "projects"
DEFAULT_PROFILES_DIR = "profiles"
DEFAULT_JOB_ARTIFACTS_ROOT = "job-artifacts"
DEFAULT_PLATFORM_DB_PATH = "platform-jobs.sqlite"
PLATFORM_BINDINGS_FILE_ENV = "PLATFORM_BINDINGS_FILE"
DEFAULT_COMMAND_TIMEOUT_SECONDS = 1800
DEFAULT_MAX_OUTPUT_BYTES = 1_048_576
MAX_OUTPUT_BYTES_ENV = "MAX_OUTPUT_BYTES"


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

    def __post_init__(self) -> None:
        # 拒绝无法在租约到期前及时自停的配置。
        if self.worker_concurrency < 1 or self.heartbeat_seconds <= 0:
            raise ValueError("worker concurrency and heartbeat must be positive")
        if self.lease_seconds < self.heartbeat_seconds * 3:
            raise ValueError("lease must cover at least three heartbeat intervals")
        if min(self.max_result_bytes, self.max_artifact_file_bytes, self.max_artifact_bytes) <= 0:
            raise ValueError("runtime storage limits must be positive")

    @classmethod
    def from_environment(cls) -> Settings:
        """Build immutable settings from environment variables."""
        return cls(
            projects_root=Path(os.getenv("PROJECTS_ROOT", DEFAULT_PROJECTS_ROOT)).resolve(),
            profiles_dir=Path(os.getenv("DBT_PROFILES_DIR", DEFAULT_PROFILES_DIR)).resolve(),
            command_timeout_seconds=int(
                os.getenv("COMMAND_TIMEOUT_SECONDS", str(DEFAULT_COMMAND_TIMEOUT_SECONDS))
            ),
            max_output_bytes=int(os.getenv(MAX_OUTPUT_BYTES_ENV, str(DEFAULT_MAX_OUTPUT_BYTES))),
            job_artifacts_root=Path(
                os.getenv("JOB_ARTIFACTS_ROOT", DEFAULT_JOB_ARTIFACTS_ROOT)
            ).resolve(),
            platform_bindings_file=(
                Path(os.environ[PLATFORM_BINDINGS_FILE_ENV]).resolve()
                if PLATFORM_BINDINGS_FILE_ENV in os.environ else None
            ),
            platform_db_path=Path(os.getenv("PLATFORM_DB_PATH", DEFAULT_PLATFORM_DB_PATH)).resolve(),
            database_url=os.getenv("SERVICE_DATABASE_URL") or None,
            temp_root=Path(os.getenv("SERVICE_TEMP_ROOT", "runtime-tmp")).resolve(),
            worker_concurrency=int(os.getenv("WORKER_CONCURRENCY", "2")),
            lease_seconds=int(os.getenv("JOB_LEASE_SECONDS", "90")),
            heartbeat_seconds=int(os.getenv("JOB_HEARTBEAT_SECONDS", "15")),
            config_version=os.getenv("SERVICE_CONFIG_VERSION", "1"),
            toolchain_version=os.getenv("SERVICE_TOOLCHAIN_VERSION") or None,
            max_result_bytes=int(os.getenv("MAX_RESULT_BYTES", str(16 * 1024 * 1024))),
            max_artifact_file_bytes=int(os.getenv("MAX_ARTIFACT_FILE_BYTES", str(64 * 1024 * 1024))),
            max_artifact_bytes=int(os.getenv("MAX_ARTIFACT_BYTES", str(256 * 1024 * 1024))),
            synchronous_wait_seconds=int(os.getenv("SYNCHRONOUS_WAIT_SECONDS", "30")),
        )
