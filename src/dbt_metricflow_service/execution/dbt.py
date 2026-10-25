"""dbt 命令的进程内执行与事件、退出状态转换。"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Final
from uuid import uuid4

from dbt_metricflow_service import constants as strings

from .embedded import create_output, engine_environment, run_serialized
from .logs import EngineLog
from .models import CommandSpec, JobRecord, JobStatus

UNSUPPORTED_ENGINE_COMMAND: Final[str] = "unsupported engine command"
MISSING_MANIFEST: Final[str] = "successful dbt command did not return a manifest"


if TYPE_CHECKING:
    from dbt.contracts.graph.manifest import Manifest
    from dbt_common.events.base_types import EventMsg

DBT = strings.DBT_EXECUTABLE


async def run_dbt(
    project: str, command: CommandSpec, timeout_seconds: float, max_output_bytes: int,
    before_start: Callable[[], Awaitable[None]],
    *, log_sink: Callable[[EngineLog], None] | None = None,
    manifest_sink: Callable[[Manifest], None] | None = None,
) -> JobRecord:
    """只接受 dbt 命令，完成串行与租约检查后执行并返回脱敏记录。"""
    # 在授权和引擎初始化前拒绝非 dbt 入口。
    if command.argv[:1] != (DBT,):
        raise ValueError(UNSUPPORTED_ENGINE_COMMAND)
    record, timed_out = await run_serialized(
        lambda: _invoke_dbt(project, command, max_output_bytes, log_sink, manifest_sink),
        timeout_seconds, before_start,
    )
    if timed_out:
        return record.with_outcome(status=JobStatus.TIMED_OUT, exit_code=None, stderr=record.stderr)
    return record


def _invoke_dbt(
    project: str, command: CommandSpec, max_output_bytes: int,
    log_sink: Callable[[EngineLog], None] | None,
    manifest_sink: Callable[[Manifest], None] | None,
) -> JobRecord:
    started = datetime.now(UTC)
    output = create_output(max_output_bytes)
    exit_code = 2
    error_type = ""
    try:
        with engine_environment(command.environment, output, log_sink) as capture_text:
            from dbt.cli.main import dbtRunner

            # dbt 事件经同一个脱敏入口进入日志缓冲区。
            def capture(event: EventMsg) -> None:
                capture_text(EngineLog.from_dbt(event))

            result = dbtRunner(callbacks=[capture]).invoke(list(command.argv[1:]))
            exit_code = 0 if result.success else (2 if result.exception is not None else 1)
            if result.exception is not None:
                error_type = type(result.exception).__name__
            if result.success and manifest_sink is not None:
                if result.manifest is None:
                    raise ValueError(MISSING_MANIFEST)
                # 在引擎环境退出前交给调用方处理，Manifest 不进入诊断或日志。
                manifest_sink(result.manifest)
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
