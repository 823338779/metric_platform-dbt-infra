from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Mapping
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

logger = logging.getLogger(__name__)


class JobStatus(StrEnum):
    """Lifecycle states exposed while a subprocess job is retained."""

    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    TIMED_OUT = "timed_out"


class JobRecord(BaseModel):
    """Immutable snapshot of one submitted CLI job."""

    model_config = ConfigDict(frozen=True)

    id: UUID = Field(description="服务进程内唯一的任务标识。")
    project: str = Field(description="任务所属的安全项目标识。")
    status: JobStatus = Field(description="任务当前生命周期状态。")
    submitted_at: datetime = Field(description="任务进入内存队列的 UTC 时间。")
    started_at: datetime | None = Field(default=None, description="子进程开始创建的 UTC 时间。")
    finished_at: datetime | None = Field(default=None, description="子进程结束处理的 UTC 时间。")
    exit_code: int | None = Field(default=None, description="子进程退出码；结束前或超时时为空。")
    stdout: str = Field(default="", description="经过截断和凭据遮盖的标准输出尾部。")
    stderr: str = Field(default="", description="经过截断和凭据遮盖的标准错误尾部。")
    output_truncated: bool = Field(default=False, description="任一输出流是否超过保留上限。")


@dataclass(frozen=True, slots=True)
class CommandSpec:
    """Literal subprocess invocation produced from one validated request."""

    # Executable and arguments passed directly to create_subprocess_exec.
    argv: tuple[str, ...]
    # Project directory used as the child process working directory.
    cwd: Path
    # Complete child environment, including the mounted profiles location.
    environment: Mapping[str, str]
    # Whether the command must hold the per-project mutation lock.
    write_operation: bool
    # Optional private subprocess input; excluded from repr to avoid accidental disclosure.
    stdin_data: bytes | None = field(default=None, repr=False)
    # Whether the runner should create an isolated derived-artifact directory.
    use_job_artifacts: bool = False
