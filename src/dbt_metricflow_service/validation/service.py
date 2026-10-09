"""草稿受理与读取；短事务封存输入，网络和 dbt 执行均由 worker 承担。"""

import hashlib
import json

from dbt_metricflow_service.storage.rows import row_dict

from ..storage.artifacts import VALIDATION_INPUT_FILE
from ..storage.branches import SQL_BRANCH_LOCK, BranchStore
from ..storage.jobs import (
    SQL_SELECT_FROM_RUNTIME_JOB_2,
    SQL_SELECT_FROM_RUNTIME_PROJECT,
    SQL_SELECT_PG_ADVISORY_XACT_LOCK_HASHTEXTEXTENDED,
    StoreConflict,
)
from .models import (
    BranchDraftValidationRequest,
    BranchValidationResult,
    DraftValidationRequest,
    ValidationReceipt,
    ValidationResult,
    changes_digest,
    changes_digest_v2,
)

DRAFT_VALIDATION = "DRAFT_VALIDATION"
VALIDATION_SCOPE = "DRAFT_VALIDATION:"
SUCCEEDED = "SUCCEEDED"
JSON_MODE = "json"


def binding_digest(binding: dict) -> str:
    """记录绑定身份而不把可能含凭据的 remote 复制到 job 或公开响应。"""
    return hashlib.sha256(json.dumps(binding, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


class DraftValidationService:
    """幂等锁覆盖输入封存与任务创建，回执丢失后优先恢复原配置快照。"""

    def __init__(self, runtime):
        self.runtime = runtime

    def submit(self, project_id: str, request: DraftValidationRequest, *,
               branch_id: str | None = None) -> ValidationReceipt:
        scope = VALIDATION_SCOPE + project_id
        version2 = isinstance(request, BranchDraftValidationRequest)
        if version2 != (branch_id is not None):
            raise ValueError("branch validation requires the v2 workspace context")
        context = {}
        if version2:
            BranchStore(self.runtime.db).get(project_id, branch_id)
            scope += ":" + branch_id
            context = {"branchId": branch_id, "workspaceId": request.workspace_id,
                       "draftRevision": request.draft_revision}
        digest = changes_digest_v2(request.changes) if version2 else changes_digest(request.changes)
        with self.runtime.db.transaction() as connection:
            sql_result = connection.exec_driver_sql(SQL_SELECT_PG_ADVISORY_XACT_LOCK_HASHTEXTEXTENDED,
                           (scope + ":" + request.idempotency_key,))
            sql_result = connection.exec_driver_sql(SQL_SELECT_FROM_RUNTIME_JOB_2, (scope, request.idempotency_key))
            prior = row_dict(sql_result)
            if prior:
                saved = prior["request_json"]
                if (saved["baseCommitSha"] != request.base_commit_sha or saved["changesDigest"] != digest
                        or any(saved.get(key) != value for key, value in context.items())):
                    raise StoreConflict("validation key already binds another draft")
                return ValidationReceipt(validation_id=prior["job_id"], state=prior["status"])
            sql_result = connection.exec_driver_sql(SQL_SELECT_FROM_RUNTIME_PROJECT, (project_id,))
            project = row_dict(sql_result)
            if not project:
                raise KeyError(project_id)
            if version2:
                sql_result = connection.exec_driver_sql(SQL_BRANCH_LOCK, (project_id, branch_id, branch_id))
                project = row_dict(sql_result)
                if not project or project["status"] != "ACTIVE":
                    raise StoreConflict("branch is not active")
            if project["config_version"] != self.runtime.settings.config_version:
                from ..runtime.service import RuntimeUnavailable

                raise RuntimeUnavailable("matching project configuration is unavailable")
            binding = project["binding_config"]
            if not all(binding.get(key) for key in ("remote", "projectSubdir", "profileBindingId")):
                raise ValueError("project binding is incomplete")
            payload = request.model_dump_json(by_alias=True).encode()
            input_set = self.runtime.artifacts.capture_validation_input(
                project_id, payload, connection, version=2 if version2 else 1)
            snapshot = {"baseCommitSha": request.base_commit_sha, "changesDigest": digest,
                        "projectSubdir": binding["projectSubdir"], "bindingDigest": binding_digest(binding), **context}
            if version2:
                snapshot["gitRef"] = project["git_ref"]
            job = self.runtime.jobs.reserve(
                DRAFT_VALIDATION, project_id, snapshot, idempotency_scope=scope,
                idempotency_key=request.idempotency_key, input_set_id=input_set,
                config_version=project["config_version"], toolchain_version=self.runtime.toolchain,
                profile_binding_id=binding["profileBindingId"], retry_policy="READ_ONLY",
                timeout_seconds=self.runtime.settings.command_timeout_seconds, branch_id=branch_id, _cursor=connection,
            )
            return ValidationReceipt(validation_id=job["job_id"], state=job["status"])

    def get(self, project_id: str, validation_id: str, *, branch_id: str | None = None) -> ValidationResult:
        job = self.runtime.jobs.get(validation_id)
        if (not job or job["kind"] != DRAFT_VALIDATION or job["project_id"] != project_id
                or job["branch_id"] != branch_id):
            raise KeyError(validation_id)
        result = self.runtime.jobs.result(validation_id) if job["status"] == SUCCEEDED else None
        snapshot = job["request_json"]
        model = BranchValidationResult if branch_id else ValidationResult
        context = ({"branch_id": branch_id, "workspace_id": snapshot["workspaceId"],
                    "draft_revision": snapshot["draftRevision"]} if branch_id else {})
        return model(
            validation_id=job["job_id"], state=job["status"], base_commit_sha=snapshot["baseCommitSha"],
            changes_digest=snapshot["changesDigest"], project_subdir=snapshot["projectSubdir"],
            config_version=job["config_version"], toolchain_version=job["toolchain_version"],
            completed_at=job["finished_at"], error_code=job["error_code"],
            **(result["payload_json"] if result else {"phase": job["phase"]}),
            **context,
        )

    def input(self, job: dict) -> DraftValidationRequest:
        model = BranchDraftValidationRequest if job["branch_id"] else DraftValidationRequest
        return model.model_validate_json(
            self.runtime.artifacts.read_file(job["input_set_id"], VALIDATION_INPUT_FILE))
