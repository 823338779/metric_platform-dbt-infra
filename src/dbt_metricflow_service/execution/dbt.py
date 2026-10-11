"""dbt 命令的进程内执行与事件、退出状态转换。"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import uuid4

from .embedded import create_output, engine_environment, run_serialized
from .models import CommandSpec, JobRecord, JobStatus

if TYPE_CHECKING:
    from dbt_common.events.base_types import EventMsg

DBT = "dbt"
EVENT_MESSAGE = "msg"


async def run_dbt(
    project: str, command: CommandSpec, timeout_seconds: float, max_output_bytes: int,
    before_start: Callable[[], Awaitable[None]],
) -> JobRecord:
    """只接受 dbt 命令，完成串行与租约检查后执行并返回脱敏记录。"""
    # 在授权和引擎初始化前拒绝非 dbt 入口。
    if command.argv[:1] != (DBT,):
        raise ValueError("unsupported engine command")
    record, timed_out = await run_serialized(
        lambda: _invoke_dbt(project, command, max_output_bytes), timeout_seconds, before_start,
    )
    if timed_out:
        return record.model_copy(update={"status": JobStatus.TIMED_OUT, "exit_code": None})
    return record


def _invoke_dbt(project: str, command: CommandSpec, max_output_bytes: int) -> JobRecord:
    started = datetime.now(UTC)
    output = create_output(max_output_bytes)
    exit_code = 2
    error_type = ""
    try:
        with engine_environment(command.environment, output) as capture_text:
            from dbt.cli.main import dbtRunner

            # dbt 事件经同一个脱敏入口进入日志缓冲区。
            def capture(event: EventMsg) -> None:
                capture_text(str(getattr(event.info, EVENT_MESSAGE)))

            result = dbtRunner(callbacks=[capture]).invoke(list(command.argv[1:]))
            exit_code = 0 if result.success else (2 if result.exception is not None else 1)
            if result.exception is not None:
                error_type = type(result.exception).__name__
    except Exception as error:
        # 不公开可能包含连接凭据的原始异常文本。
        exit_code = 2
        error_type = type(error).__name__
    finally:
        output.finish()
    return JobRecord(
        id=uuid4(), project=project, status=JobStatus.SUCCEEDED if exit_code == 0 else JobStatus.FAILED,
        submitted_at=started, started_at=started, finished_at=datetime.now(UTC),
        exit_code=exit_code, stdout=output.decode(), stderr=error_type, output_truncated=output.truncated,
    )
