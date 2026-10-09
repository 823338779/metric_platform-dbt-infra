"""固定提交验证的持久受理与结果读取。"""

import hashlib
import json

from ..publications.models import FixedCommitRequest
from ..storage.jobs import StoreConflict
from .models import ValidationReceipt, ValidationResult

DRAFT_VALIDATION = "DRAFT_VALIDATION"  # 已有数据库任务枚举，升级不改历史迁移。
VALIDATION_SCOPE = "COMMIT_VALIDATION:"


def binding_digest(binding: dict) -> str:
    return hashlib.sha256(json.dumps(binding, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


class CommitValidationService:
    def __init__(self, runtime):
        self.runtime = runtime

    def submit(self, project_id: str, request: FixedCommitRequest) -> ValidationReceipt:
        scope = VALIDATION_SCOPE + project_id
        prior = self.runtime.jobs.by_key(scope, request.idempotency_key)
        if prior:
            if prior["request_json"]["commitSha"] != request.commit_sha:
                raise StoreConflict("validation key already binds another commit")
            return ValidationReceipt(validation_id=prior["job_id"], state=prior["status"])
        project = self.runtime.jobs.project(project_id)
        if not project:
            raise KeyError(project_id)
        if project["config_version"] != self.runtime.settings.config_version:
            from ..runtime.service import RuntimeUnavailable
            raise RuntimeUnavailable("matching project configuration is unavailable")
        binding = project["binding_config"]
        if not all(binding.get(key) for key in ("remote", "projectSubdir", "profileBindingId")):
            raise ValueError("project binding is incomplete")
        snapshot = {"commitSha": request.commit_sha, "projectSubdir": binding["projectSubdir"],
                    "bindingDigest": binding_digest(binding)}
        job = self.runtime.jobs.reserve(DRAFT_VALIDATION, project_id, snapshot,
            idempotency_scope=scope, idempotency_key=request.idempotency_key,
            config_version=project["config_version"], toolchain_version=self.runtime.toolchain,
            profile_binding_id=binding["profileBindingId"], retry_policy="READ_ONLY",
            timeout_seconds=self.runtime.settings.command_timeout_seconds,
            expected_revision=project["revision"])
        return ValidationReceipt(validation_id=job["job_id"], state=job["status"])

    def get(self, project_id: str, validation_id: str) -> ValidationResult:
        job = self.runtime.jobs.get(validation_id)
        if not job or job["kind"] != DRAFT_VALIDATION or job["project_id"] != project_id:
            raise KeyError(validation_id)
        result = self.runtime.jobs.result(validation_id) if job["status"] == "SUCCEEDED" else None
        snapshot = job["request_json"]
        return ValidationResult(validation_id=job["job_id"], state=job["status"],
            commit_sha=snapshot.get("commitSha", snapshot.get("baseCommitSha")),
            project_subdir=snapshot["projectSubdir"], config_version=job["config_version"],
            toolchain_version=job["toolchain_version"], completed_at=job["finished_at"], error_code=job["error_code"],
            **(result["payload_json"] if result else {"phase": job["phase"]}))
