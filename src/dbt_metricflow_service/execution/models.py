from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Final
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from dbt_metricflow_service import constants as strings

JOB_ID_DESCRIPTION: Final[str] = "服务进程内唯一的任务标识。"
JOB_PROJECT_DESCRIPTION: Final[str] = "任务所属的安全项目标识。"
JOB_STATUS_DESCRIPTION: Final[str] = "任务当前生命周期状态。"
SUBMITTED_AT_DESCRIPTION: Final[str] = "任务进入内存队列的 UTC 时间。"
STARTED_AT_DESCRIPTION: Final[str] = "开始执行的 UTC 时间。"
FINISHED_AT_DESCRIPTION: Final[str] = "执行及清理结束的 UTC 时间。"
EXIT_CODE_DESCRIPTION: Final[str] = "命令退出码或 SDK 结果映射码；尚未返回或操作不提供退出码时为空。"
STDOUT_DESCRIPTION: Final[str] = "经过截断和凭据遮盖的标准输出尾部。"
STDERR_DESCRIPTION: Final[str] = "经过截断和凭据遮盖的标准错误尾部。"
OUTPUT_TRUNCATED_DESCRIPTION: Final[str] = "任一输出流是否超过保留上限。"


logger = logging.getLogger(__name__)


class JobStatus(StrEnum):
    """SDK 或通用命令执行记录的生命周期状态。"""

    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class JobRecord(BaseModel):
    """一次 SDK 或通用命令执行的不可变快照。"""

    model_config = ConfigDict(frozen=True, extra=strings.EXTRA_FORBID)

    id: UUID = Field(description=JOB_ID_DESCRIPTION)
    project: str = Field(description=JOB_PROJECT_DESCRIPTION)
    status: JobStatus = Field(description=JOB_STATUS_DESCRIPTION)
    submitted_at: datetime = Field(description=SUBMITTED_AT_DESCRIPTION)
    started_at: datetime | None = Field(default=None, description=STARTED_AT_DESCRIPTION)
    finished_at: datetime | None = Field(default=None, description=FINISHED_AT_DESCRIPTION)
    exit_code: int | None = Field(default=None, description=EXIT_CODE_DESCRIPTION)
    stdout: str = Field(default="", description=STDOUT_DESCRIPTION)
    stderr: str = Field(default="", description=STDERR_DESCRIPTION)
    output_truncated: bool = Field(default=False, description=OUTPUT_TRUNCATED_DESCRIPTION)

    def with_outcome(self, *, status: JobStatus, exit_code: int | None, stderr: str) -> JobRecord:
        """Copy the immutable diagnostic with explicitly typed outcome fields."""
        return JobRecord(
            id=self.id, project=self.project, status=status, submitted_at=self.submitted_at,
            started_at=self.started_at, finished_at=self.finished_at, exit_code=exit_code,
            stdout=self.stdout, stderr=stderr, output_truncated=self.output_truncated,
        )


@dataclass(frozen=True, slots=True)
class ExecutionResult:
    """执行完成的产物身份；只有 worker 的带租约事务能封存产物。"""
    # 当前 attempt 产生的 STAGING 集合，交给发布事务封存。
    output_set_id: str


