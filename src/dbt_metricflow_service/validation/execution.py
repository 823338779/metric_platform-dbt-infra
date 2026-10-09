"""在隔离进程验证请求的固定提交，不执行 build 或发布。"""

import json
import os
import sys
from pathlib import Path
from uuid import UUID

from ..execution.models import CommandSpec
from ..platform.bindings import ProjectBinding, resolve_commit
from .service import binding_digest

UTF8 = "utf-8"
WORKER_MODULE = "dbt_metricflow_service.validation.worker"
VALIDATING = "VALIDATING"
VALIDATION_OUTPUT = "validation.json"
V2_OPTION = "--branch-v2"


async def execute_commit_validation(executor, job: dict, runner, attempt: Path):
    from ..runtime.executor import ExecutionError, ExecutionResult, _thread

    request = job["request_json"]
    project_record = await _thread(executor.jobs.project, job["project_id"])
    binding = project_record["binding_config"]
    if project_record["config_version"] != job["config_version"] or binding_digest(binding) != request["bindingDigest"]:
        raise ExecutionError("VALIDATION_CONFIGURATION_CHANGED")
    configured = ProjectBinding(job["project_id"], binding["remote"], binding["projectSubdir"],
                                binding["profileBindingId"], binding.get("schemaName"))
    project, digest = await _thread(resolve_commit, configured, request["commitSha"], attempt)
    output = attempt / VALIDATION_OUTPUT
    # 与现有执行链路使用同一受控 profile 环境；覆盖宿主 target 路径，避免产物逸出 attempt。
    # schema 仅用于解析关系名，本流程从不创建该 schema 或执行模型。
    environment = {**os.environ, "DBT_PROJECT_DIR": str(project),
                   "DBT_PROFILES_DIR": str(executor.settings.profiles_dir),
                   "DBT_TARGET_PATH": str(project / "target"),
                   "DBT_PLATFORM_SCHEMA": configured.schema_name or "validation_" + UUID(job["job_id"]).hex,
                   "DBT_TARGET": job["profile_binding_id"],
                   "DBT_SEND_ANONYMOUS_USAGE_STATS": "false", "PYTHONUTF8": "1"}
    argv = (sys.executable, "-m", WORKER_MODULE, str(project), str(executor.settings.profiles_dir),
            job["profile_binding_id"], str(output))
    spec = CommandSpec(argv + (V2_OPTION,), project, environment, False)
    try:
        await executor._command(job, runner, spec, VALIDATING)
    except ExecutionError as error:
        # CLI 错误可能回显 YAML 和配置，持久层仅保存安全状态。
        error.payload = {}
        raise
    with output.open("rb") as stream:
        raw = stream.read(executor.settings.max_result_bytes + 1)
    if len(raw) > executor.settings.max_result_bytes:
        raise ExecutionError("RESULT_TOO_LARGE")
    value = json.loads(raw)
    value["validatedProjectDigest"] = digest
    return ExecutionResult(value)
