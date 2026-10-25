"""dbt 调用的日志、环境与原生结果上下文；不负责触发引擎。"""
from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import uuid4

from dbt_metricflow_service import constants as strings

from .embedded import create_output, engine_environment
from .logs import EngineLog
from .models import JobRecord, JobStatus

if TYPE_CHECKING:
    from dbt_common.events.base_types import EventMsg

    from dbt_metricflow_service.delegates.clients.dbt_client import DbtResult


ERROR_LEVEL = "error"
INVOCATION_ERROR_EVENT = "DbtInvocationError"


class DbtExecution:
    """保存一次调用的诊断状态，不保存命令或引擎操作。"""

    def __init__(self, project: str, max_output_bytes: int) -> None:
        self.project = project
        self.started = datetime.now(UTC)
        self.output = create_output(max_output_bytes)
        self.exit_code = 2
        self.error_type = ""

    def callback(self, event: EventMsg) -> None:
        self.capture_text(EngineLog.from_dbt(event))

    def check(self, result: DbtResult) -> None:
        from dbt_metricflow_service.runtime.dbt_executor import ExecutionError

        self.exit_code = 0 if result.success else (2 if result.exception is not None else 1)
        if result.exception is not None:
            self.log_exception(result.exception)
        if not result.success:
            raise ExecutionError(strings.COMMAND_FAILED, stopped=self.exit_code == 1)

    def log_exception(self, error: BaseException) -> None:
        self.error_type = type(error).__name__
        self.capture_text(EngineLog(datetime.now(UTC), ERROR_LEVEL,
            f"{self.error_type}: {error}", event_name=INVOCATION_ERROR_EVENT))

    @property
    def record(self) -> JobRecord:
        return JobRecord(
            id=uuid4(), project=self.project,
            status=JobStatus.SUCCEEDED if self.exit_code == 0 else JobStatus.FAILED,
            submitted_at=self.started, started_at=self.started, finished_at=datetime.now(UTC),
            exit_code=self.exit_code, stdout=self.output.decode(), stderr=self.error_type,
            output_truncated=self.output.truncated,
        )


@contextmanager
def dbt_execution(
    project: str, environment: Mapping[str, str], max_output_bytes: int,
    log_sink: Callable[[EngineLog], None] | None = None,
) -> Iterator[DbtExecution]:
    from dbt_metricflow_service.runtime.dbt_executor import ExecutionError
    from dbt_metricflow_service.storage.records import JobErrorDetail

    execution = DbtExecution(project, max_output_bytes)
    try:
        with engine_environment(environment, execution.output, log_sink) as capture_text:
            execution.capture_text = capture_text
            try:
                yield execution
            except ExecutionError:
                raise
            except Exception as error:
                execution.exit_code = 2
                execution.log_exception(error)
                raise
    except Exception as error:
        if not isinstance(error, ExecutionError):
            execution.exit_code = 2
            execution.error_type = type(error).__name__
        execution.output.finish()
        raise ExecutionError(
            strings.COMMAND_FAILED, JobErrorDetail.model_validate(execution.record, from_attributes=True),
            stopped=isinstance(error, ExecutionError) and error.stopped,
        ) from None
    finally:
        execution.output.finish()
