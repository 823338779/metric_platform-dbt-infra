"""MetricFlow 结构化请求的进程内执行，不使用命令参数或退出码。"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from dbt_metricflow_service.models.payloads import JsonObject

from .embedded import PROFILES_ENV, PROJECT_ENV, create_output, engine_environment, run_serialized
from .models import JobRecord, JobStatus


@dataclass(frozen=True, slots=True)
class MetricFlowRequest:
    """引擎连接环境与直接在内存中传递的业务请求。"""

    # 项目路径、profile 和执行目标等引擎环境项。
    environment: Mapping[str, str]
    # 直接交给 MetricFlow 的请求，不写入诊断表示。
    payload: JsonObject = field(repr=False)


@dataclass(frozen=True, slots=True)
class MetricFlowResult:
    """MetricFlow 的业务结果、业务错误码和脱敏执行诊断。"""

    # 记录执行状态与日志；MetricFlow 不设置命令退出码。
    record: JobRecord
    # 成功时直接返回的业务对象，失败或超时时为空。
    payload: JsonObject | None = field(default=None, repr=False)
    # 可向调用方公开的业务错误码，不包含原始异常文本。
    error_code: str | None = None


async def run_metricflow(
    project: str, request: MetricFlowRequest, timeout_seconds: float, max_output_bytes: int,
    before_start: Callable[[], Awaitable[None]],
) -> MetricFlowResult:
    """在共享串行保护下执行 MetricFlow；超时结果不发布业务数据。"""
    result, timed_out = await run_serialized(
        lambda: _invoke_metricflow(project, request, max_output_bytes), timeout_seconds, before_start,
    )
    if timed_out:
        return replace(result, record=result.record.model_copy(update={"status": JobStatus.TIMED_OUT}), payload=None)
    return result


def _invoke_metricflow(project: str, request: MetricFlowRequest, max_output_bytes: int) -> MetricFlowResult:
    started = datetime.now(UTC)
    output = create_output(max_output_bytes)
    status = JobStatus.FAILED
    payload = None
    error_code = None
    error_type = ""
    try:
        with engine_environment(request.environment, output):
            from ..platform.metricflow import INVALID_OPTIONS_CODE, InvalidOptions, execute_programmatic

            # 业务错误在 MetricFlow 边界识别，成功结果保持原始内存对象。
            try:
                payload = execute_programmatic(
                    Path(request.environment[PROJECT_ENV]), Path(request.environment[PROFILES_ENV]), request.payload,
                )
            except InvalidOptions:
                error_code = INVALID_OPTIONS_CODE
            else:
                status = JobStatus.SUCCEEDED
    except Exception as error:
        # 初始化、查询与恢复失败均只公开异常类型，且不发布未完成的结果。
        status = JobStatus.FAILED
        payload = None
        error_code = None
        error_type = type(error).__name__
    finally:
        output.finish()
    record = JobRecord(
        id=uuid4(), project=project, status=status,
        submitted_at=started, started_at=started, finished_at=datetime.now(UTC),
        stdout=output.decode(), stderr=error_type, output_truncated=output.truncated,
    )
    return MetricFlowResult(record, payload, error_code)
