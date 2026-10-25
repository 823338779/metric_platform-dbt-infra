"""还原工程后只执行 dbt build，持久化该次调用返回的 Manifest。"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, cast

import yaml

from dbt_metricflow_service import constants as strings
from dbt_metricflow_service.models.native import NativeProject
from dbt_metricflow_service.storage.records import LeasedJob

from .manifest_capture import ManifestCapture

if TYPE_CHECKING:
    from dbt_metricflow_service.runtime.executor import ExecutionResult, RuntimeExecutor


def _normalize_artifact_target(project: Path) -> None:
    # build 已按命令参数输出到 target；只同步执行副本，供封存和历史查询恢复。
    path = project / strings.DBT_PROJECT_FILE
    configuration = NativeProject.model_validate(yaml.safe_load(path.read_text(encoding=strings.UTF8)))
    configuration.target_path = strings.TARGET_DIRECTORY
    path.write_text(yaml.safe_dump(configuration.native_details(), sort_keys=False), encoding=strings.UTF8)


async def execute_build(
    executor: RuntimeExecutor, job: LeasedJob, project: Path, *, profiles: Path | None = None,
) -> ExecutionResult:
    from ..execution.models import CommandSpec
    from ..runtime.executor import ExecutionResult, _thread, build_engine_environment

    settings = executor.settings
    profiles = profiles or settings.profiles_dir
    environment = build_engine_environment(
        project, profiles, cast(str, job.schema_name), cast(str, job.profile_binding_id),
    )
    spec = CommandSpec((
        strings.DBT_EXECUTABLE, strings.DBT_BUILD_COMMAND,
        strings.DBT_PROJECT_DIR_OPTION, str(project),
        strings.DBT_PROFILES_DIR_OPTION, str(profiles),
        strings.DBT_TARGET_OPTION, cast(str, job.profile_binding_id),
        strings.DBT_TARGET_PATH_OPTION, str(project / strings.TARGET_DIRECTORY),
    ), project, environment, True)
    capture = ManifestCapture(settings.max_artifact_file_bytes)
    await executor._dbt(job, spec, strings.BUILDING, manifest_sink=capture)
    content = capture.content
    await _thread(_normalize_artifact_target, project)
    output = await _thread(
        executor.artifacts.capture, job.project_id, project,
        producer_attempt_id=job.attempt_id, kind=strings.EXECUTION,
        metadata=executor._metadata(job), manifest_json=content,
    )
    return ExecutionResult(output_set_id=output)
