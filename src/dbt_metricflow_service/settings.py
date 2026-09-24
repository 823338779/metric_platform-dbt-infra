from __future__ import annotations

import logging
import os
from dataclasses import dataclass
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
        )
