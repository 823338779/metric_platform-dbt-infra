"""服务拥有发布输入与查询映射。"""
import shutil

from ..platform.bindings import ProjectBinding, resolve_commit
from ..storage.branches import BranchStore
from ..storage.jobs import StoreConflict
from ..storage.publications import (
    PublicationStore,
)
from .errors import INVALID_SELECTION, PublicationError
from .models import CATALOG_SCHEMA_VERSION, FixedCommitRequest
from .query_time import DEFAULT_TIMEZONE

PUBLISHED = "PUBLISHED"
ACTIVE_BRANCH = "ACTIVE"
PROTOCOL_VERSION = "fixed-commit-v1"
AGENT_CAPABILITIES = ["fixed-commit-validation-v1", "fixed-commit-build-v1", "query-options-async-v1",
                      "query-results-page-v1", "query-time-v1"]
VALIDATION_CHECKS = ("allTestsPassed", "representativeQueryPassed", "relationsVerified")
CHECK_PASSED = "PASSED"
CHECK_FAILED = "FAILED"


def invalid_selection(reason, field, message, recovery="fix_query_selection"):
    return PublicationError(INVALID_SELECTION, reason, field, message, False, recovery)


class InvalidPublishedArtifact(RuntimeError):
    """封存目录发生存储或协议异常，不能归因于用户查询参数。"""


class ReleaseGone(ValueError):
    """历史记录仍然存在，但不能用于新的目录操作或查询。"""


def release_descriptor(row: dict) -> dict:
    return {"projectId": row["project_id"], "releaseId": row["release_id"], "runId": row["run_id"],
            "artifactSetId": row["artifact_set_id"], "publicationSequence": row["sequence"],
            "publishedAt": row["published_at"], "sourceSha": row["request_json"].get("commitSha"),
            "buildMode": row["build_mode"], "catalogSchemaVersion": CATALOG_SCHEMA_VERSION,
            "catalogDigest": row["catalog_digest"], "state": row["state"], "errorCode": row["error_code"],
            "createdAt": row["created_at"],
            "businessTimezone": row["request_json"].get("businessTimezone", DEFAULT_TIMEZONE),
            "projectSubdir": row["request_json"].get("projectSubdir", ".")}


def query_receipt(row: dict, project_id: str, release_id) -> dict:
    # 重试恢复身份后仍由查询读取端获取结果，受理响应保持可轮询状态。
    return {"queryId": row["job_id"], "projectId": project_id,
            "releaseId": str(release_id), "state": "QUEUED"}


class PublicationService:
    def __init__(self, runtime):
        # 复用运行时连接池及工具链，管理入口不启动另一个 worker。
        self.runtime = runtime
        self.store = PublicationStore(runtime.db)
        # 每个请求固定分支，未指定时始终选择 main；不能修改此值切换在途请求。


    def _branch(self, project_id):
        # UUID 还必须属于当前项目，显式 main 与旧无分支入口使用相同身份。
        branches = BranchStore(self.runtime.db)
        return branches.production(project_id)


    def _release(self, project_id, release_id):
        # 封存目录原始字节不改变，归属由外层发布记录验证。
        release = self.store.get_release(project_id, release_id)
        return release


    def submit(self, project_id: str, request: FixedCommitRequest) -> dict:
        prior = self.store.by_key(project_id, request.idempotency_key)
        if prior:
            return self._recover(prior, request)
        project = self.runtime.jobs.project(project_id)
        if not project:
            raise KeyError(project_id)
        if project["config_version"] != self.runtime.settings.config_version:
            from ..runtime.service import RuntimeUnavailable
            raise RuntimeUnavailable("matching project configuration is unavailable")
        binding = project["binding_config"]
        configured = ProjectBinding(project_id, binding["remote"], binding["projectSubdir"],
                                    binding["profileBindingId"], binding.get("schemaName"))
        directory, digest = resolve_commit(configured, request.commit_sha, self.runtime.settings.temp_root)
        try:
            source_id = self.runtime.artifacts.capture(project_id, directory, metadata={
                "source_commit_sha": request.commit_sha, "project_digest": digest,
                "config_version": project["config_version"], "toolchain_version": self.runtime.toolchain})
        finally:
            shutil.rmtree(directory)
        snapshot = {"projectId": project_id, "commitSha": request.commit_sha, "projectDigest": digest,
                    "profileBindingId": configured.profile_binding_id, "configVersion": project["config_version"],
                    "toolchainVersion": self.runtime.toolchain, "projectSubdir": configured.project_subdir,
                    "businessTimezone": binding.get("businessTimezone", DEFAULT_TIMEZONE)}
        return self.store.reserve_build(self.runtime.jobs, project, request.idempotency_key, snapshot,
                                        source_id, self.runtime.settings.command_timeout_seconds)

    @staticmethod
    def _recover(prior, request):
        if prior["request_json"]["commitSha"] != request.commit_sha:
            raise StoreConflict("release key already binds another commit")
        return prior

    def projects(self) -> list[dict]:
        return [self.publication(project_id) for project_id in self.store.project_ids()]

    def _descriptor(self, row):
        return release_descriptor(row)

    def publication(self, project_id: str) -> dict:
        branch = self._branch(project_id)
        result = self.store.get_publication(project_id, branch_id=branch["branch_id"])
        binding = branch["binding_config"]
        result.update(protocolVersion=PROTOCOL_VERSION, capabilities=AGENT_CAPABILITIES,
                      projectSubdir=binding.get("projectSubdir", "."),
                      businessTimezone=binding.get("businessTimezone", DEFAULT_TIMEZONE))
        result.update(configVersion=branch["config_version"],
                      toolchainVersion=getattr(self.runtime, "toolchain", None))
        if result["activePublication"]:
            result["activePublication"] = self._descriptor(result["activePublication"])
        return result


    def releases(self, project_id: str) -> list[dict]:
        self._branch(project_id)
        return [self._descriptor(row) for row in self.store.releases(project_id)]

    def release(self, project_id: str, release_id: str) -> dict:
        row = self._release(project_id, release_id)
        # 只展示封存的布尔证明和固定文案，不回显数据库、SQL 或 CLI 异常正文。
        checks = []
        if row["artifact_set_id"]:
            evidence = self.runtime.artifacts.metadata(row["artifact_set_id"])["validation_json"]
            checks = [{"name": name, "status": CHECK_PASSED if evidence.get(name) is True else CHECK_FAILED,
                       "message": None} for name in VALIDATION_CHECKS]
        elif row["run_id"]:
            job = self.runtime.jobs.get(row["run_id"])
            summary = (job.get("error_detail") or {}).get("validationSummary") if job else None
            if summary:
                return {**self._descriptor(row), "validationSummary": summary}
        return {**self._descriptor(row), "validationSummary": {
            "phase": row["state"], "checks": checks, "truncated": False}}


    def _active_release(self, project_id: str, release_id: str) -> dict:
        release = self._release(project_id, release_id)
        branch = self._branch(project_id)
        publication = self.store.get_publication(project_id, branch_id=branch["branch_id"])["activePublication"]
        if release["state"] != PUBLISHED:
            raise KeyError(release_id)
        if (branch["status"] != ACTIVE_BRANCH or not publication
                or publication["release_id"] != release["release_id"]):
            raise ReleaseGone("发布版本已替代")
        return release


