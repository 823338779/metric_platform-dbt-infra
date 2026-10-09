"""固定基线与 YAML 操作只应用到本 attempt 私有目录。"""

import hashlib
import json
import os
import sys
from pathlib import Path
from uuid import UUID

import yaml

from ..execution.models import CommandSpec
from ..platform.bindings import GIT_MAIN, ProjectBinding, resolve_draft_revision
from ..storage.artifacts import VALIDATION_INPUT_FILE, _check_paths
from ..storage.branches import BranchStore
from .models import (
    BranchDraftValidationRequest,
    DraftValidationRequest,
    changes_digest,
    changes_digest_v2,
)
from .service import binding_digest

CREATE = "CREATE"
DELETE = "DELETE"
UTF8 = "utf-8"
MODEL_PATHS = "model-paths"
PROJECT_FILE = "dbt_project.yml"
WORKER_MODULE = "dbt_metricflow_service.validation.worker"
VALIDATING = "VALIDATING"
VALIDATION_OUTPUT = "validation.json"
V2_ROOTS = ["models", "tests"]
V2_OPTION = "--branch-v2"


def apply_changes(project: Path, changes: list, *, version: int = 1) -> str:
    """先检查完整操作集，再修改副本；不按 basename 猜测目标。"""
    config = yaml.safe_load((project / PROJECT_FILE).read_text(UTF8))
    roots = V2_ROOTS if version == 2 else config.get(MODEL_PATHS, ["models"])
    existing = {path.relative_to(project).as_posix() for path in project.rglob("*") if path.is_file()}
    _check_paths(list(existing | {change.path for change in changes}))
    for change in changes:
        if not any(change.path.startswith(root + "/") for root in roots):
            raise ValueError("draft changes must stay in model paths")
        path = project / change.path
        if change.operation == CREATE:
            if path.exists():
                raise ValueError("CREATE target already exists")
        elif not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != change.expected_sha256:
            raise ValueError("draft baseline file digest mismatch")
    for change in changes:
        path = project / change.path
        if change.operation == DELETE:
            path.unlink()
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(change.content.encode(UTF8))
    digest = hashlib.sha256()
    for path in sorted((path for path in project.rglob("*") if path.is_file()),
                       key=lambda path: path.relative_to(project).as_posix().encode(UTF8)):
        digest.update(path.relative_to(project).as_posix().encode(UTF8) + b"\0")
        digest.update(hashlib.sha256(path.read_bytes()).digest())
    return digest.hexdigest()


async def execute_draft_validation(executor, job: dict, runner, attempt: Path):
    from ..runtime.executor import ExecutionError, ExecutionResult, _thread
    from .worker import diagnostic, result

    request = job["request_json"]
    project_record = await _thread(executor.jobs.project, job["project_id"])
    version2 = job["branch_id"] is not None
    if version2:
        project_record = await _thread(BranchStore(executor.jobs.db).execution_binding,
                                       job["project_id"], job["branch_id"])
    binding = project_record["binding_config"]
    if project_record["config_version"] != job["config_version"] or binding_digest(binding) != request["bindingDigest"]:
        raise ExecutionError("VALIDATION_CONFIGURATION_CHANGED")
    payload = await _thread(executor.artifacts.read_file, job["input_set_id"], VALIDATION_INPUT_FILE)
    model = BranchDraftValidationRequest if version2 else DraftValidationRequest
    draft = model.model_validate_json(payload)
    digest = changes_digest_v2(draft.changes) if version2 else changes_digest(draft.changes)
    if (draft.base_commit_sha != request["baseCommitSha"] or digest != request["changesDigest"]
            or version2 and (draft.workspace_id != request["workspaceId"]
                             or draft.draft_revision != request["draftRevision"]
                             or job["branch_id"] != request["branchId"])):
        raise ExecutionError("VALIDATION_INPUT_MISMATCH")
    configured = ProjectBinding(job["project_id"], binding["remote"], binding["projectSubdir"],
                                binding["profileBindingId"], binding.get("schemaName"))
    project, _ = await _thread(resolve_draft_revision, configured, draft.base_commit_sha, attempt,
                               git_ref=request.get("gitRef", GIT_MAIN))
    try:
        digest = await _thread(apply_changes, project, draft.changes, version=2 if version2 else 1)
    except ValueError:
        return ExecutionResult(result(["BASELINE"], [diagnostic("invalid_draft_changes",
                                      "操作路径或旧文件摘要与固定基线不一致。")]))
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
    spec = CommandSpec(argv + ((V2_OPTION,) if version2 else ()), project, environment, False)
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
