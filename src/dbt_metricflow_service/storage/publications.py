"""项目的唯一发布存储。"""

from __future__ import annotations

from uuid import UUID, uuid4

from sqlalchemy import Select, func, select, update
from sqlalchemy.orm import Session

from dbt_metricflow_service.models.payloads import JsonObject
from dbt_metricflow_service.storage.entities import (
    ArtifactFile,
    ArtifactSet,
    Branch,
    Release,
    ReleaseRelation,
    RuntimeJob,
    RuntimeProject,
)
from dbt_metricflow_service.storage.postgres import Database
from dbt_metricflow_service.storage.records import DatabaseRow
from dbt_metricflow_service.storage.rows import entity_dict

from ..models.artifacts import BindingMode, PublishedCatalog
from .artifacts import MAX_FILE_BYTES, _decode
from .branches import PRODUCTION
from .jobs import JobStore, StoreConflict

PUBLISHED = "PUBLISHED"
SEALED = "SEALED"
CATALOG_PATH = "target/published_catalog.json"
INPUT_FIELDS = {"commitSha": "source_commit_sha", "projectDigest": "project_digest",
                "configVersion": "config_version", "toolchainVersion": "toolchain_version"}
BUILD_RUN = "BUILD_RUN"
ACTIVE = "ACTIVE"
SUCCEEDED = "SUCCEEDED"

SUPERSEDED = "SUPERSEDED"


def _release(project_id: str, release_id: str) -> Select:
    return select(Release).where(Release.project_id == project_id,
                                 Release.release_id == release_id)


def _by_key(project_id: str, key: str) -> Select:
    return select(Release).join(Branch, (Branch.project_id == Release.project_id)
                                & (Branch.branch_id == Release.branch_id)).where(
        Release.project_id == project_id, Release.idempotency_key == key, Branch.mode == PRODUCTION)


class PublicationStore:
    """与任务存储共用数据库，活动版本仅在封存事务内推进。"""

    def __init__(self, db: Database) -> None:
        # 连接池由 Runtime 拥有；store 不单独创建连接或提交外部事务。
        self.db = db

    def by_key(self, project_id: str, key: str) -> DatabaseRow | None:
        with self.db.session() as session:
            return entity_dict(session.scalar(_by_key(project_id, key)))

    def project_ids(self) -> list[str]:
        with self.db.session() as session:
            return list(session.scalars(select(RuntimeProject.project_id).order_by(RuntimeProject.project_id)))

    def releases(self, project_id: str) -> list[DatabaseRow]:
        with self.db.session() as session:
            return [entity_dict(row) for row in session.scalars(
                select(Release).where(Release.project_id == project_id).order_by(Release.created_at.desc()))]

    def reserve_build(
        self, jobs: JobStore, project: DatabaseRow, key: str, snapshot: JsonObject, source_id: str, timeout_seconds: int
    ) -> DatabaseRow:
        from ..platform.namespace import validate_schema_name

        project_id = project["project_id"]
        scope = "PUBLICATION:" + project_id
        with self.db.session() as session:
            session.execute(select(func.pg_advisory_xact_lock(func.hashtextextended(scope + ":" + key, 0))))
            session.execute(select(RuntimeProject.project_id).where(
                RuntimeProject.project_id == project_id).with_for_update())
            prior = session.scalar(_by_key(project_id, key))
            if prior:
                if prior.request_json["commitSha"] != snapshot["commitSha"]:
                    raise StoreConflict("release key already binds another commit")
                return entity_dict(prior)
            current = session.get(RuntimeProject, project_id)
            if (current.binding_config != project["binding_config"]
                    or current.config_version != snapshot["configVersion"]):
                raise StoreConflict("project changed while reading commit")
            release = self.create_candidate_in_transaction(session, project_id, snapshot, key)
            run_id = uuid4()
            binding = project["binding_config"]
            schema = validate_schema_name(binding["schemaName"]) if binding.get("schemaName") else "run_" + run_id.hex
            job = jobs.reserve_in_transaction(
                session, BUILD_RUN, project_id, {**snapshot, "binding": binding, "releaseId": release["release_id"]},
                job_id=str(run_id), input_set_id=source_id, idempotency_scope=scope, idempotency_key=key,
                config_version=snapshot["configVersion"], toolchain_version=snapshot["toolchainVersion"],
                profile_binding_id=snapshot["profileBindingId"], schema_name=schema,
                timeout_seconds=timeout_seconds, expected_revision=project["revision"],
            )
            session.execute(update(Release).where(Release.release_id == release["release_id"])
                               .values(run_id=job["job_id"]))
            return {**release, "run_id": job["job_id"]}

    def create_candidate(self, project_id: str, request: JsonObject, idempotency_key: str, *,
                         branch_id: str | None = None) -> JsonObject:
        # 分支行串行化候选序号与请求幂等；不同分支互不淘汰。
        with self.db.session() as session:
            return self.create_candidate_in_transaction(
                session, project_id, request, idempotency_key,
                branch_id=branch_id,
            )

    def create_candidate_in_transaction(
        self,
        session: Session,
        project_id: str,
        request: JsonObject,
        idempotency_key: str,
        *,
        branch_id: str | None = None,
    ) -> JsonObject:
        # 分支行串行化候选序号与请求幂等；不同分支互不淘汰。
        if not idempotency_key:
            raise ValueError("候选幂等键不能为空")
        session.execute(select(RuntimeProject.project_id).where(
            RuntimeProject.project_id == project_id).with_for_update())
        branch = session.scalar(select(Branch).where(
            Branch.project_id == project_id,
            Branch.branch_id == branch_id if branch_id is not None else Branch.mode == PRODUCTION,
        ).with_for_update().execution_options(populate_existing=True))
        if branch is None:
            raise KeyError(project_id)
        prior = session.scalar(select(Release).where(
            Release.project_id == project_id, Release.branch_id == branch.branch_id,
            Release.idempotency_key == idempotency_key))
        if prior is not None:
            if prior.request_json != request:
                raise StoreConflict("候选幂等键已用于不同输入")
            return entity_dict(prior)
        if branch.status != ACTIVE:
            raise StoreConflict("分支当前不接受新候选")
        release = Release(
            release_id=str(uuid4()), project_id=project_id, sequence=branch.publication_sequence + 1,
            idempotency_key=idempotency_key, request_json=request, baseline_release_id=branch.active_release_id,
            branch_id=branch.branch_id,
        )
        session.add(release)
        # 候选先落库，再推进引用它的分支指针；flush 同时取得数据库默认值。
        session.flush()
        branch.publication_sequence += 1
        branch.latest_release_id = release.release_id
        commit_sha = request.get("commitSha")
        if commit_sha is not None:
            branch.observed_head_sha = commit_sha
            if branch.mode == PRODUCTION and branch.base_commit_sha is None:
                branch.base_commit_sha = commit_sha
        session.flush()
        return entity_dict(release)

    def get_release(self, project_id: str, release_id: UUID | str) -> JsonObject:
        with self.db.session() as session:
            row = entity_dict(session.scalar(_release(project_id, str(release_id))))
            if not row:
                raise KeyError(str(release_id))
            return row

    def get_publication(self, project_id: str, *, branch_id: str | None = None) -> JsonObject:
        # 在单一事务读取指针和记录，发布身份一旦取定就不替换成后续版本。
        with self.db.session() as session:
            branch = session.scalar(select(Branch).where(
                Branch.project_id == project_id,
                Branch.branch_id == branch_id if branch_id is not None else Branch.mode == PRODUCTION))
            if branch is None:
                raise KeyError(project_id)
            active = (
                entity_dict(session.scalar(_release(project_id, branch.active_release_id)))
                if branch.active_release_id else None
            )
            return {"projectId": project_id, "activePublication": active}

    def publish_in_transaction(self, session: Session, *, job_id: UUID | str, attempt_token: UUID | str,
                               release_id: UUID | str, output_set_id: UUID | str) -> None:
        # 调用者持有 job/attempt 锁；与受理及清理保持 job → project → release 顺序。
        job = JobStore(self.db)._authorized(session, job_id, attempt_token)
        if not job:
            raise ValueError("发布租约已失效")
        session.execute(select(RuntimeProject.project_id).where(
            RuntimeProject.project_id == job["project_id"]).with_for_update())
        # 从不可变候选读取分支；先锁分支再锁候选，与受理顺序一致。
        identity = session.scalar(_release(job["project_id"], str(release_id)))
        if identity is None:
            raise ValueError("发布与执行身份不匹配")
        branch = session.scalar(select(Branch).where(
            Branch.project_id == job["project_id"], Branch.branch_id == identity.branch_id,
        ).with_for_update().execution_options(populate_existing=True))
        release = session.scalar(select(Release).where(Release.run_id == str(job_id))
                                 .with_for_update().execution_options(populate_existing=True))
        if (release is None or release.release_id != str(release_id)
                or job["branch_id"] != release.branch_id):
            raise ValueError("发布与执行身份不匹配")
        if release.state == PUBLISHED:
            return
        if (branch.status != ACTIVE or branch.publication_sequence != release.sequence
                or branch.active_release_id != release.baseline_release_id
                or branch.config_version != job["config_version"]):
            release.state = SUPERSEDED
            session.flush()
            return
        output = session.get(ArtifactSet, str(output_set_id), populate_existing=True)
        if (output is None or output.state != SEALED or output.project_id != job["project_id"]
                or output.producer_attempt_id != job["current_attempt_id"]
                or output.validation_json.get("publicationValidated") is not True):
            raise ValueError("缺少已封存的完整发布证明")
        # 源码、配置、工具链以及源产物均必须来自这个候选的固定输入。
        if (any(not release.request_json.get(field)
                or release.request_json[field] != job["request_json"].get(field)
                or release.request_json[field] != getattr(output, column)
                for field, column in INPUT_FIELDS.items())
                or output.source_set_id != job["input_set_id"]):
            raise ValueError("发布产物与候选输入不匹配")
        file = entity_dict(session.get(ArtifactFile, (str(output_set_id), CATALOG_PATH)))
        if not file:
            raise ValueError("发布展示文件未入库")
        catalog = PublishedCatalog.model_validate_json(_decode(file, MAX_FILE_BYTES))
        if catalog.project_id != job["project_id"] or str(catalog.release_id) != str(release_id):
            raise ValueError("展示产物身份不匹配")
        for binding in catalog.relation_bindings:
            if ((binding.mode == BindingMode.EXTERNAL) != (binding.creator_run_id is None)
                    or binding.mode == BindingMode.BUILT and str(binding.creator_run_id) != str(job_id)
                    or binding.mode == BindingMode.REUSED and str(binding.creator_run_id) == str(job_id)):
                raise ValueError("物理绑定创建者身份不匹配")
            if binding.creator_run_id:
                creator = session.get(RuntimeJob, str(binding.creator_run_id))
                if (not creator or creator.project_id != job["project_id"]
                        or creator.branch_id != release.branch_id
                        or creator.kind != BUILD_RUN or creator.run_lifecycle != ACTIVE
                        or binding.mode == BindingMode.REUSED and creator.status != SUCCEEDED):
                    raise ValueError("物理绑定所属项目不匹配")
            session.add(ReleaseRelation(
                release_id=str(release_id), native_id=binding.native_id,
                creator_run_id=str(binding.creator_run_id) if binding.creator_run_id else None,
                binding_json=binding.model_dump(mode="json", by_alias=True),
            ))
        # 物理绑定必须在发布仍可编辑时写入，再推进发布状态和分支指针。
        session.flush()
        release.state = PUBLISHED
        release.artifact_set_id = str(output_set_id)
        release.catalog_digest = file["raw_sha256"]
        release.build_mode = output.validation_json["buildMode"]
        release.published_at = func.clock_timestamp()
        session.flush()
        branch.active_release_id = str(release_id)
        session.flush()
