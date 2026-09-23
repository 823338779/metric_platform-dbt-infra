from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_PROJECTS_ROOT = "/workspace/projects"
DEFAULT_PROFILES_DIR = "/workspace/profiles"
DEFAULT_JOB_ARTIFACTS_ROOT = "/workspace/job-artifacts"
DEFAULT_COMMAND_TIMEOUT_SECONDS = 1800
DEFAULT_MAX_OUTPUT_BYTES = 1_048_576


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

    @classmethod
    def from_environment(cls) -> Settings:
        """Build immutable settings from environment variables."""
        return cls(
            projects_root=Path(os.getenv("PROJECTS_ROOT", DEFAULT_PROJECTS_ROOT)).resolve(),
            profiles_dir=Path(os.getenv("DBT_PROFILES_DIR", DEFAULT_PROFILES_DIR)).resolve(),
            command_timeout_seconds=int(
                os.getenv("COMMAND_TIMEOUT_SECONDS", str(DEFAULT_COMMAND_TIMEOUT_SECONDS))
            ),
            max_output_bytes=int(os.getenv("MAX_OUTPUT_BYTES", str(DEFAULT_MAX_OUTPUT_BYTES))),
            job_artifacts_root=Path(
                os.getenv("JOB_ARTIFACTS_ROOT", DEFAULT_JOB_ARTIFACTS_ROOT)
            ).resolve(),
        )
