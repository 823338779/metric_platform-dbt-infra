"""测试中组装不同原生命令；生产构建直接调用 DbtClient.build。"""
from dataclasses import dataclass
from functools import partial
from pathlib import Path

from dbt_metricflow_service.delegates.clients.dbt_client import DbtClient
from dbt_metricflow_service.execution.dbt import dbt_execution
from dbt_metricflow_service.execution.embedded import run_serialized
from dbt_metricflow_service.execution.errors import ExecutionError
from dbt_metricflow_service.execution.log_writer import ExecutionLogWriter
from dbt_metricflow_service.execution.threads import run_in_thread
from dbt_metricflow_service.storage.execution_logs import ExecutionLogStore


@dataclass
class EngineCall:
    argv: tuple[str, ...]
    cwd: Path
    environment: dict[str, str]
    write_operation: bool


async def invoke_for_test(project, call, max_output_bytes, before_start, *, log_sink=None, manifest_sink=None):
    def invoke():
        try:
            with dbt_execution(project, call.environment, max_output_bytes, log_sink) as context:
                result = DbtClient().invoke(list(call.argv[1:]), callbacks=[context.callback])
                context.check(result)
                if manifest_sink is not None:
                    if result.manifest is None:
                        raise ValueError("successful dbt call did not return a manifest")
                    manifest_sink(result.manifest)
        except ExecutionError:
            pass
        return context.record
    return await run_serialized(invoke, before_start)


async def invoke_job_for_test(executor, job, call, phase):
    writer = ExecutionLogWriter(partial(ExecutionLogStore(executor.jobs.db).append, job, phase, call.argv[1]))
    try:
        record = await invoke_for_test(
            job.project_id, call, executor.settings.max_output_bytes,
            lambda: executor._authorize(job, phase, external=call.write_operation), log_sink=writer.emit,
        )
    finally:
        await run_in_thread(writer.close)
    if record.exit_code:
        from dbt_metricflow_service.storage.records import JobErrorDetail
        raise ExecutionError("COMMAND_FAILED", JobErrorDetail.model_validate(record, from_attributes=True),
                             stopped=not call.write_operation or record.exit_code == 1)
    return record
