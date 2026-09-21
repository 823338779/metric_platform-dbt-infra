from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Mapping
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

logger = logging.getLogger(__name__)


class DbtCommand(StrEnum):
    """dbt subcommands exposed by the service."""

    PARSE = "parse"
    COMPILE = "compile"
    SEED = "seed"
    RUN = "run"
    TEST = "test"
    BUILD = "build"
    DEBUG = "debug"


class MetricFlowCommand(StrEnum):
    """MetricFlow operations exposed by the service."""

    LIST_METRICS = "list_metrics"
    LIST_DIMENSIONS = "list_dimensions"
    EXPLAIN = "explain"
    QUERY = "query"


class JobStatus(StrEnum):
    """Lifecycle states exposed while a subprocess job is retained."""

    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    TIMED_OUT = "timed_out"


class DbtJobRequest(BaseModel):
    """Structured dbt invocation accepted by the HTTP boundary."""

    model_config = ConfigDict(extra="forbid")

    project: str = Field(description="项目根目录下的安全项目标识。")
    command: DbtCommand = Field(description="允许执行的 dbt 命令。")
    target: str | None = Field(default=None, description="profiles.yml 中的 dbt target。")
    select: list[str] = Field(default_factory=list, description="dbt selection 条件。")
    exclude: list[str] = Field(default_factory=list, description="dbt exclusion 条件。")
    variables: dict[str, object] = Field(default_factory=dict, description="传递给 dbt --vars 的值。")
    full_refresh: bool = Field(default=False, description="是否执行 dbt full refresh。")


class MetricFlowJobRequest(BaseModel):
    """Structured MetricFlow invocation accepted by the HTTP boundary."""

    model_config = ConfigDict(extra="forbid")

    project: str = Field(description="项目根目录下的安全项目标识。")
    command: MetricFlowCommand = Field(description="允许执行的 MetricFlow 命令。")
    metrics: list[str] = Field(default_factory=list, description="指标名称列表。")
    group_by: list[str] = Field(default_factory=list, description="分组维度或实体列表。")
    where: list[str] = Field(default_factory=list, description="MetricFlow where 条件列表。")
    order_by: list[str] = Field(default_factory=list, description="排序字段列表。")
    start_time: datetime | None = Field(default=None, description="查询起始时间。")
    end_time: datetime | None = Field(default=None, description="查询结束时间。")
    limit: int | None = Field(default=None, ge=1, description="最大返回行数。")


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
