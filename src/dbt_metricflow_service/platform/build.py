"""还原工程后只执行 dbt build，持久化该次调用返回的 Manifest。"""

from __future__ import annotations

from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, cast

import yaml

from dbt_metricflow_service import constants as strings
from dbt_metricflow_service.delegates.clients.dbt_client import DbtClient
from dbt_metricflow_service.models.native import NativeProject
from dbt_metricflow_service.storage.records import LeasedJob

from .manifest_capture import ManifestCapture

if TYPE_CHECKING:
    from dbt_metricflow_service.runtime.dbt_executor import ExecutionResult, RuntimeExecutor


MISSING_MANIFEST = "successful dbt build did not return a manifest"


def _normalize_artifact_target(project: Path) -> None:
    # build 已按命令参数输出到 target；只同步执行副本，供封存和历史查询恢复。
    path = project / strings.DBT_PROJECT_FILE
    configuration = NativeProject.model_validate(yaml.safe_load(path.read_text(encoding=strings.UTF8)))
    configuration.target_path = strings.TARGET_DIRECTORY
    path.write_text(yaml.safe_dump(configuration.native_details(), sort_keys=False), encoding=strings.UTF8)


async def execute_build(
    executor: RuntimeExecutor, job: LeasedJob, project: Path, *, profiles: Path | None = None,
) -> ExecutionResult:
    from ..execution.dbt import dbt_execution
    from ..execution.embedded import run_serialized
    from ..runtime.execution_logs import ExecutionLogWriter
    from ..runtime.dbt_executor import ExecutionResult, _thread, build_engine_environment
    from ..storage.execution_logs import ExecutionLogStore

    settings = executor.settings
    profiles = profiles or settings.profiles_dir
    environment = build_engine_environment(
        project, profiles, cast(str, job.schema_name), cast(str, job.profile_binding_id),
    )
    capture = ManifestCapture(settings.max_artifact_file_bytes)
    writer = ExecutionLogWriter(partial(
        ExecutionLogStore(executor.jobs.db).append, job, strings.BUILDING, strings.DBT_BUILD_COMMAND,
    ))

    def build() -> None:
        with dbt_execution(job.project_id, environment, settings.max_output_bytes, writer.emit) as context:
            result = DbtClient().build(
                project, profiles, target=cast(str, job.profile_binding_id),
                target_path=project / strings.TARGET_DIRECTORY, callbacks=[context.callback],
            )
            context.check(result)
            if result.manifest is None:
                raise ValueError(MISSING_MANIFEST)
            capture(result.manifest)

    try:
        await run_serialized(build, lambda: executor._authorize(job, strings.BUILDING, external=True))
    finally:
        # 取消时串行执行器也会等待引擎退出，之后才关闭日志写入器。
        await _thread(writer.close)
    await _thread(_normalize_artifact_target, project)
    output = await _thread(
        capture.persist, executor.artifacts, job.project_id, project,
        producer_attempt_id=job.attempt_id, metadata=executor._metadata(job),
    )
    return ExecutionResult(output_set_id=output)
