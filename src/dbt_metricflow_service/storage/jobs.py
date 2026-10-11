"""数据库队列、执行租约与发布事务；外部 SQL 的停止必须单独确认。"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from datetime import timedelta
from typing import cast, overload
from uuid import UUID, uuid4

from sqlalchemy import and_, func, literal, or_, select, true, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from dbt_metricflow_service.models.payloads import JsonObject, JsonValue
from dbt_metricflow_service.storage.records import DatabaseRow, LeasedJob, StoredJob
from dbt_metricflow_service.storage.rows import entity_dict

from .branches import BranchStore
from .entities import (
    ArtifactFile,
    ArtifactSet,
    Branch,
    Build,
    DeploymentTarget,
    JobResult,
    Release,
    ReleaseRelation,
    RuntimeAttempt,
    RuntimeJob,
    RuntimeProject,
)
from .postgres import Database

# ORM 表达式显式保留条件更新和行锁，时间边界由数据库判断。
EXECUTING = "EXECUTING"
PRODUCTION = "PRODUCTION"
PREPARATION_ONLY = "PREPARATION_ONLY"
QUERY_OPTIONS = "QUERY_OPTIONS"
INVALID_QUERY = "INVALID_QUERY"
PREPARING = "PREPARING"
BUILDING = "BUILDING"
VALIDATING = "VALIDATING"
PUBLISHED = "PUBLISHED"
SUPERSEDED = "SUPERSEDED"
QUEUED = "QUEUED"
RUNNING = "RUNNING"
SUCCEEDED = "SUCCEEDED"
FAILED = "FAILED"
BUILD_RUN = "BUILD_RUN"
RUN_CLEANUP = "RUN_CLEANUP"
DBT_COMMAND = "DBT_COMMAND"
MF_COMMAND = "MF_COMMAND"
VOLATILE = "VOLATILE"
DURABLE = "DURABLE"
READ_ONLY = "READ_ONLY"
NEVER = "NEVER"
EXTERNAL = "EXTERNAL"
INPUT_LOST = "INPUT_LOST"
OUTCOME_UNKNOWN = "EXECUTION_OUTCOME_UNKNOWN"
TIMEOUT = "TASK_TIMEOUT"
LEASE_EXPIRED = "LEASE_EXPIRED"
ACTIVE = "ACTIVE"
CLEANING = "CLEANING"
CLEANED = "CLEANED"
SEALED = "SEALED"
EXECUTION = "EXECUTION"
SOURCE = "SOURCE"
EXPIRED_UNCONFIRMED = "EXPIRED_UNCONFIRMED"
STOPPED = "STOPPED"
CLEANUP_SCOPE = "RUN_CLEANUP"
REQUIRED_BUILD_FILES = frozenset(
    {"target/manifest.json", "target/semantic_manifest.json", "target/run_results.json", "target/catalog.json"}
)
REQUIRED_VALIDATION_FLAGS = ("allTestsPassed", "representativeQueryPassed", "relationsVerified")
MAX_DIAGNOSTIC = 1024 * 1024
MAX_RESULT = 16 * 1024 * 1024


FORBIDDEN_REQUEST_KEYS = frozenset({"resources", "credentials", "password", "token", "secret", "environment", "env"})
RELEASE_FIELD = "releaseId"


class _FinishLeaseLost(Exception):
    """Abort all writes when the final lease check fails."""


class StoreConflict(ValueError):
    """幂等键已用于不同请求。"""


class ProjectBusy(ValueError):
    """项目存在尚未确认结束的写操作。"""


class CleanupBlocked(ValueError):
    """run 仍被活动任务或未知外部执行引用。"""


@overload
def _safe_request(value: JsonObject) -> JsonObject: ...


@overload
def _safe_request(value: JsonValue) -> JsonValue: ...


def _safe_request(value: JsonValue) -> JsonValue:
    # resources 和凭据不进入持久请求；调用者必须另外在内存保留 VOLATILE 输入。
    if isinstance(value, dict):
        return {key: _safe_request(item) for key, item in value.items() if key.lower() not in FORBIDDEN_REQUEST_KEYS}
    if isinstance(value, list):
        return [_safe_request(item) for item in value]
    return value


class JobStore:
    def __init__(
        self,
        db: Database,
        lease_seconds: int = 90,
        *,
        max_result_bytes: int = MAX_RESULT,
        max_diagnostic_bytes: int = MAX_DIAGNOSTIC,
    ) -> None:
        # 数据库共享控制状态；租约长度只决定后续新租约的有效期。
        self.db = db
        self.lease_seconds = lease_seconds
        # 序列化后的结果和诊断分别设限，避免单条记录撑满内容存储。
        self.max_result_bytes = max_result_bytes
        self.max_diagnostic_bytes = max_diagnostic_bytes

    def register_project(
        self,
        project_id: str,
        binding_config: JsonObject | None = None,
        config_version: str = "1",
        source_set_id: str | None = None,
        preview_profile: str | None = None,
    ) -> DatabaseRow:
        # 项目导入与普通任务受理锁同一项目行，换源时立即移除旧输出指针。
        with self.db.session() as session:
            session.execute(
                pg_insert(RuntimeProject)
                .values(project_id=project_id, binding_config=binding_config or {}, config_version=config_version)
                .on_conflict_do_nothing(index_elements=[RuntimeProject.project_id])
            )
            project = session.get(RuntimeProject, project_id, with_for_update=True, populate_existing=True)
            if source_set_id is not None:
                source = session.get(ArtifactSet, source_set_id)
                if not source or source.state != SEALED or source.kind != SOURCE or source.project_id != project_id:
                    raise ValueError("Project source must be a sealed source of the same project")
            changed = source_set_id is not None and project.source_set_id != str(source_set_id)
            if changed or project.config_version != config_version:
                project.current_output_set_id = None
            if binding_config is not None:
                project.binding_config = binding_config
            if source_set_id is not None:
                project.source_set_id = source_set_id
            project.config_version = config_version
            project.revision += 1
            session.flush()
            BranchStore.ensure_production(session, project_id, preview_profile)
            return entity_dict(project)

    def project(self, project_id: str) -> DatabaseRow | None:
        with self.db.session() as session:
            row = session.execute(
                select(
                    RuntimeProject,
                    func.coalesce(Branch.publication_sequence, 0),
                    Branch.active_release_id,
                )
                .outerjoin(Branch, and_(Branch.project_id == RuntimeProject.project_id, Branch.mode == PRODUCTION))
                .where(RuntimeProject.project_id == project_id)
            ).one_or_none()
            if row is None:
                return None
            # 发布序号和活动指针只从生产分支派生，不在项目表重复保存。
            result = entity_dict(row[0])
            result.update(publication_sequence=row[1], active_published_release_id=row[2])
            return result

    def get(self, job_id: UUID | str) -> StoredJob | None:
        with self.db.session() as session:
            return cast(StoredJob | None, entity_dict(session.get(RuntimeJob, str(job_id))))

    def by_key(self, scope: str, key: str) -> StoredJob | None:
        with self.db.session() as session:
            job = session.scalar(
                select(RuntimeJob).where(RuntimeJob.idempotency_scope == scope, RuntimeJob.idempotency_key == key)
            )
            return cast(StoredJob | None, entity_dict(job))

    def result_exists(self, job_id: UUID | str) -> bool:
        # 状态读取仅检查结果引用，不加载结果正文。
        with self.db.session() as session:
            return session.scalar(select(select(JobResult.job_id).where(JobResult.job_id == str(job_id)).exists()))

    def options_for(self, run_id: UUID | str) -> list[StoredJob]:
        # 查询只消费同一固定构建的已完成选项任务。
        with self.db.session() as session:
            jobs = session.scalars(
                select(RuntimeJob)
                .where(
                    RuntimeJob.parent_run_id == str(run_id),
                    RuntimeJob.kind == QUERY_OPTIONS,
                    RuntimeJob.status == SUCCEEDED,
                )
                .order_by(RuntimeJob.created_at.desc())
            )
            return [cast(StoredJob, entity_dict(job)) for job in jobs]

    def external_started(self, attempt_id: UUID | str) -> bool:
        # 取消时以持久 attempt 为准；无记录时不能证明外部执行已经停止。
        with self.db.session() as session:
            return (
                session.scalar(
                    select(RuntimeAttempt.execution_stage).where(RuntimeAttempt.attempt_id == str(attempt_id))
                )
                != PREPARING
            )

    def result(self, job_id: UUID | str) -> DatabaseRow | None:
        with self.db.session() as session:
            return entity_dict(session.get(JobResult, str(job_id)))

    def reserve(
        self,
        kind: str,
        project_id: str,
        request_json: JsonObject,
        *,
        job_id: UUID | str | None = None,
        fingerprint: str | None = None,
        idempotency_scope: str | None = None,
        idempotency_key: str | None = None,
        parent_run_id: UUID | str | None = None,
        input_set_id: str | None = None,
        input_mode: str = DURABLE,
        pinned_instance_id: str | None = None,
        config_version: str = "1",
        toolchain_version: str = "default",
        schema_name: str | None = None,
        profile_binding_id: str | None = None,
        retry_policy: str = "PREPARATION_ONLY",
        max_attempts: int = 3,
        timeout_seconds: int = 600,
        write: bool = False,
        expected_revision: int | None = None,
        branch_id: str | None = None,
    ) -> StoredJob:
        # 幂等作用域先串行化；parent 锁统一先于项目行和子任务，避免清理受理穿透。
        with self.db.fact_session() as session:
            return cast(
                StoredJob,
                self.reserve_in_transaction(
                    session,
                    kind,
                    project_id,
                    request_json,
                    job_id=job_id,
                    fingerprint=fingerprint,
                    idempotency_scope=idempotency_scope,
                    idempotency_key=idempotency_key,
                    parent_run_id=parent_run_id,
                    input_set_id=input_set_id,
                    input_mode=input_mode,
                    pinned_instance_id=pinned_instance_id,
                    config_version=config_version,
                    toolchain_version=toolchain_version,
                    schema_name=schema_name,
                    profile_binding_id=profile_binding_id,
                    retry_policy=retry_policy,
                    max_attempts=max_attempts,
                    timeout_seconds=timeout_seconds,
                    write=write,
                    expected_revision=expected_revision,
                    branch_id=branch_id,
                ),
            )

    def reserve_in_transaction(
        self,
        connection: Session,
        kind: str,
        project_id: str,
        request_json: JsonObject,
        *,
        job_id: UUID | str | None = None,
        fingerprint: str | None = None,
        idempotency_scope: str | None = None,
        idempotency_key: str | None = None,
        parent_run_id: UUID | str | None = None,
        input_set_id: str | None = None,
        input_mode: str = DURABLE,
        pinned_instance_id: str | None = None,
        config_version: str = "1",
        toolchain_version: str = "default",
        schema_name: str | None = None,
        profile_binding_id: str | None = None,
        retry_policy: str = "PREPARATION_ONLY",
        max_attempts: int = 3,
        timeout_seconds: int = 600,
        write: bool = False,
        expected_revision: int | None = None,
        branch_id: str | None = None,
    ) -> StoredJob:
        # 幂等作用域先串行化；parent 锁统一先于项目行和子任务，避免清理受理穿透。
        session = connection
        if idempotency_key is not None:
            idempotency_scope = idempotency_scope or kind
            session.execute(
                select(func.pg_advisory_xact_lock(func.hashtextextended(idempotency_scope + ":" + idempotency_key, 0)))
            )
        parent = None
        if parent_run_id is not None:
            parent = session.get(RuntimeJob, str(parent_run_id), with_for_update=True, populate_existing=True)
            if not parent or parent.kind != BUILD_RUN or parent.project_id != project_id:
                raise ValueError("Parent must be a BUILD_RUN of this project")
        project = session.get(RuntimeProject, project_id, with_for_update=True, populate_existing=True)
        if not project:
            raise ValueError("Project is not registered")
        # 前置 manifest 校验之后若项目发生导入，拒绝混用旧输入与新配置。
        if expected_revision is not None and (
            project.revision != expected_revision or project.config_version != config_version
        ):
            raise StoreConflict("Project changed during request validation; retry")
        if input_set_id is None:
            if parent:
                input_set_id = parent.output_set_id
            elif kind in (DBT_COMMAND, MF_COMMAND):
                input_set_id = project.current_output_set_id or project.source_set_id
        safe_request = _safe_request(request_json)
        encoded = json.dumps(
            [
                {"input": fingerprint or safe_request, "branchId": branch_id}
                if branch_id
                else fingerprint or safe_request,
                kind,
                project_id,
                str(parent_run_id) if parent_run_id else None,
                input_set_id,
                input_mode,
                profile_binding_id,
                config_version,
                toolchain_version,
            ],
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        fingerprint = hashlib.sha256(encoded.encode()).hexdigest()
        if idempotency_key is not None:
            prior = session.scalar(
                select(RuntimeJob).where(
                    RuntimeJob.idempotency_scope == idempotency_scope, RuntimeJob.idempotency_key == idempotency_key
                )
            )
            if prior:
                if prior.request_fingerprint != fingerprint:
                    raise StoreConflict("Idempotency key belongs to another request")
                return cast(StoredJob, entity_dict(prior))
        if parent and (parent.status != SUCCEEDED or parent.run_lifecycle != ACTIVE):
            raise CleanupBlocked("Run is not ready and active")
        if input_set_id is not None:
            artifact = session.get(ArtifactSet, input_set_id)
            if not artifact or artifact.state != SEALED or artifact.project_id != project_id:
                raise ValueError("Input artifact set is not available")
        elif parent:
            raise ValueError("Parent has no published artifact set")
        if write and project.busy_job_id is not None:
            raise ProjectBusy("project_busy")
        # 发布类任务继承固定候选或父 run 的分支，普通任务保持无分支。
        if parent and branch_id is not None and parent.branch_id != branch_id:
            raise ValueError("父任务与分支归属不匹配")
        branch_id = parent.branch_id if parent else branch_id
        if safe_request.get(RELEASE_FIELD):
            release = session.scalar(
                select(Release).where(
                    Release.project_id == project_id, Release.release_id == safe_request[RELEASE_FIELD]
                )
            )
            if not release or branch_id is not None and branch_id != release.branch_id:
                raise ValueError("任务与发布分支归属不匹配")
            branch_id = release.branch_id
        job = RuntimeJob(
            job_id=str(job_id or uuid4()),
            kind=kind,
            project_id=project_id,
            branch_id=branch_id,
            parent_run_id=str(parent_run_id) if parent_run_id is not None else None,
            idempotency_scope=idempotency_scope,
            idempotency_key=idempotency_key,
            request_fingerprint=fingerprint,
            request_json=safe_request,
            input_mode=input_mode,
            pinned_instance_id=pinned_instance_id,
            input_lease_expires_at=func.clock_timestamp() + timedelta(seconds=self.lease_seconds)
            if input_mode == VOLATILE
            else None,
            input_set_id=input_set_id,
            config_version=config_version,
            toolchain_version=toolchain_version,
            schema_name=schema_name,
            profile_binding_id=profile_binding_id,
            run_lifecycle=ACTIVE if kind == BUILD_RUN else None,
            deadline_at=func.clock_timestamp() + timedelta(seconds=timeout_seconds),
            retry_policy=retry_policy,
            max_attempts=max_attempts,
        )
        session.add(job)
        # 先写任务并取得数据库默认值，再设置引用该任务的项目忙碌指针。
        session.flush()
        if write:
            project.busy_job_id = job.job_id
            session.flush()
        return cast(StoredJob, entity_dict(job))

    def requeue_options(self, job_id: UUID | str) -> bool:
        # 同步选项的明确终止失败可重试，沿用原截止时间和总尝试上限。
        with self.db.fact_session() as session:
            job = session.get(RuntimeJob, str(job_id))
            if job and job.parent_run_id:
                parent = session.get(RuntimeJob, job.parent_run_id, with_for_update=True, populate_existing=True)
                if parent.run_lifecycle != ACTIVE:
                    return False
            result = session.execute(
                update(RuntimeJob)
                .where(
                    RuntimeJob.job_id == str(job_id),
                    RuntimeJob.kind == QUERY_OPTIONS,
                    RuntimeJob.status == FAILED,
                    RuntimeJob.attempt_no < RuntimeJob.max_attempts,
                    RuntimeJob.deadline_at > func.clock_timestamp(),
                    RuntimeJob.error_code != INVALID_QUERY,
                    ~select(RuntimeAttempt.attempt_id)
                    .where(
                        RuntimeAttempt.job_id == RuntimeJob.job_id,
                        RuntimeAttempt.execution_stage == EXTERNAL,
                        RuntimeAttempt.stop_confirmed_at.is_(None),
                    )
                    .exists(),
                )
                .values(
                    status=QUEUED,
                    current_attempt_id=None,
                    available_at=func.clock_timestamp(),
                    finished_at=None,
                    error_code=None,
                    error_detail=None,
                )
            )
            return result.rowcount == 1

    def claim(
        self,
        worker_id: UUID | str,
        *,
        config_version: str = "1",
        config_versions: list[str] | None = None,
        toolchain_version: str = "default",
        kinds: list[str] | None = None,
    ) -> LeasedJob | None:
        # 短事务领取一个任务，跳过其他 worker 已锁住的任务；VOLATILE 输入必须仍有效。
        with self.db.fact_session() as session:
            versions = config_versions if config_versions is not None else [config_version]
            job = session.scalar(
                select(RuntimeJob)
                .where(
                    RuntimeJob.status == QUEUED,
                    RuntimeJob.available_at <= func.clock_timestamp(),
                    RuntimeJob.deadline_at > func.clock_timestamp(),
                    RuntimeJob.config_version.in_(versions),
                    RuntimeJob.toolchain_version == toolchain_version,
                    true() if kinds is None else RuntimeJob.kind.in_(kinds),
                    or_(
                        RuntimeJob.input_mode == DURABLE,
                        and_(
                            RuntimeJob.pinned_instance_id == str(worker_id),
                            RuntimeJob.input_lease_expires_at > func.clock_timestamp(),
                        ),
                    ),
                )
                .order_by(RuntimeJob.available_at, RuntimeJob.created_at)
                .limit(1)
                .with_for_update(skip_locked=True)
            )
            if not job:
                return None
            attempt_id, token = str(uuid4()), str(uuid4())
            session.add(
                RuntimeAttempt(
                    attempt_id=attempt_id,
                    job_id=job.job_id,
                    attempt_no=job.attempt_no + 1,
                    worker_id=str(worker_id),
                    lease_token=token,
                    lease_expires_at=func.clock_timestamp() + timedelta(seconds=self.lease_seconds),
                )
            )
            # attempt 必须先落库，再变更 current_attempt_id 和触发构建状态投影。
            session.flush()
            job.status = RUNNING
            job.attempt_no += 1
            job.current_attempt_id = attempt_id
            if job.started_at is None:
                job.started_at = func.clock_timestamp()
            session.flush()
            result = entity_dict(job)
            result.update(attempt_id=attempt_id, lease_token=token)
            return cast(LeasedJob, result)

    def _authorized(self, session: Session, job_id: UUID | str, token: UUID | str) -> LeasedJob | None:
        # 清理和子任务更新都先锁 parent；每次修改同时核对 current attempt、token 和数据库时间。
        parent_run_id = session.scalar(select(RuntimeJob.parent_run_id).where(RuntimeJob.job_id == str(job_id)))
        if parent_run_id:
            session.execute(select(RuntimeJob.job_id).where(RuntimeJob.job_id == parent_run_id).with_for_update())
        row = session.execute(
            select(RuntimeJob, RuntimeAttempt)
            .join(RuntimeAttempt, RuntimeAttempt.attempt_id == RuntimeJob.current_attempt_id)
            .where(
                RuntimeJob.job_id == str(job_id),
                RuntimeJob.status == RUNNING,
                RuntimeAttempt.state == EXECUTING,
                RuntimeAttempt.lease_token == str(token),
                RuntimeAttempt.lease_expires_at > func.clock_timestamp(),
                RuntimeJob.deadline_at > func.clock_timestamp(),
                or_(RuntimeJob.input_mode == DURABLE, RuntimeJob.input_lease_expires_at > func.clock_timestamp()),
            )
            .with_for_update(of=(RuntimeJob, RuntimeAttempt))
            .execution_options(populate_existing=True)
        ).one_or_none()
        if row is None:
            return None
        job, attempt = row
        result = entity_dict(job)
        result.update(
            attempt_id=attempt.attempt_id, execution_stage=attempt.execution_stage, lease_token=attempt.lease_token
        )
        return cast(LeasedJob, result)

    def heartbeat(self, job_id: UUID | str, token: UUID | str) -> bool:
        with self.db.session() as session:
            job = self._authorized(session, job_id, token)
            if not job:
                return False
            session.execute(
                update(RuntimeAttempt)
                .where(RuntimeAttempt.attempt_id == job["attempt_id"])
                .values(
                    heartbeat_at=func.clock_timestamp(),
                    lease_expires_at=func.clock_timestamp() + timedelta(seconds=self.lease_seconds),
                )
            )
            return True

    def heartbeat_inputs(self, worker_id: UUID | str, job_ids: list[str] | None = None) -> int:
        # 排队等待期也需要输入续租；已经过期的输入租约不能通过迟到心跳复活。
        with self.db.session() as session:
            result = session.execute(
                update(RuntimeJob)
                .where(
                    RuntimeJob.input_mode == VOLATILE,
                    RuntimeJob.pinned_instance_id == str(worker_id),
                    RuntimeJob.status.in_((QUEUED, RUNNING)),
                    RuntimeJob.input_lease_expires_at > func.clock_timestamp(),
                    RuntimeJob.deadline_at > func.clock_timestamp(),
                    true() if job_ids is None else RuntimeJob.job_id.in_(job_ids),
                )
                .values(input_lease_expires_at=func.clock_timestamp() + timedelta(seconds=self.lease_seconds))
            )
            return result.rowcount

    def phase(
        self,
        job_id: UUID | str,
        token: UUID | str,
        phase: str,
        *,
        external: bool = False,
        external_execution_refs: JsonObject | None = None,
    ) -> bool:
        with self.db.fact_session() as session:
            job = self._authorized(session, job_id, token)
            if not job:
                return False
            session.execute(update(RuntimeJob).where(RuntimeJob.job_id == str(job_id)).values(phase=phase))
            if job["request_json"].get("releaseId"):
                session.execute(
                    update(Release)
                    .where(Release.run_id == str(job_id), Release.state.in_((PREPARING, BUILDING, VALIDATING)))
                    .values(state=phase)
                )
            if external:
                session.execute(
                    update(RuntimeAttempt)
                    .where(RuntimeAttempt.attempt_id == job["attempt_id"])
                    .values(execution_stage=EXTERNAL, external_execution_refs=external_execution_refs or {})
                )
            return True

    def attach_input(
        self, job_id: UUID | str, token: UUID | str, set_id: str, *, project_digest: str | None = None
    ) -> bool:
        # 首次构建的源码在外部写入前固定，后续准备阶段重试可复用该集合。
        with self.db.fact_session() as session:
            job = self._authorized(session, job_id, token)
            if not job:
                return False
            source = session.get(ArtifactSet, set_id)
            if not source or source.state != SEALED or source.project_id != job["project_id"]:
                raise ValueError("Invalid source artifact set")
            if job["input_set_id"] is not None and job["input_set_id"] != str(set_id):
                raise ValueError("Job input is immutable after admission")
            session.execute(update(RuntimeJob).where(RuntimeJob.job_id == str(job_id)).values(input_set_id=set_id))
            if project_digest is not None:
                session.execute(
                    update(RuntimeJob)
                    .where(RuntimeJob.job_id == str(job_id))
                    .values(request_json=RuntimeJob.request_json.concat({"projectDigest": project_digest}))
                )
            return True

    def _release_project(self, session: Session, job_id: UUID | str) -> None:
        # 只有所有外部执行都确认结束，才释放通用写锁。
        session.execute(
            update(RuntimeProject)
            .where(
                RuntimeProject.busy_job_id == str(job_id),
                ~select(RuntimeAttempt.attempt_id)
                .where(
                    RuntimeAttempt.job_id == str(job_id),
                    RuntimeAttempt.execution_stage == EXTERNAL,
                    RuntimeAttempt.stop_confirmed_at.is_(None),
                )
                .exists(),
            )
            .values(busy_job_id=None)
        )

    def finish(
        self,
        job_id: UUID | str,
        token: UUID | str,
        payload: JsonObject | None = None,
        *,
        output_set_id: str | None = None,
        stdout_tail: str = "",
        stderr_tail: str = "",
        exit_code: int = 0,
        output_truncated: bool = False,
        seal: Callable[[str, Session], None] | None = None,
        publish: Callable[[Session, StoredJob, str], None] | None = None,
    ) -> bool:
        # 发布产物、结果和终态共用一个事务；过期 worker 无权发布任何内容。
        encoded = json.dumps(payload or {}, ensure_ascii=False, separators=(",", ":"))
        if len(encoded.encode()) > self.max_result_bytes:
            raise ValueError("Job result exceeds configured size limit")
        try:
            with self.db.fact_session() as session:
                job = self._authorized(session, job_id, token)
                if not job:
                    return False
                if job["kind"] == BUILD_RUN and output_set_id is None:
                    raise ValueError("BUILD_RUN requires a complete validated output artifact set")
                # 命令已明确结束，若取消意图先提交，则只确认取消、不发布成功或部署。
                cancelled = session.scalar(select(Build.cancel_requested).where(Build.run_id == str(job_id)))
                if cancelled:
                    session.execute(
                        update(RuntimeAttempt)
                        .where(RuntimeAttempt.attempt_id == job["attempt_id"])
                        .values(
                            state=FAILED,
                            stop_confirmed_at=func.clock_timestamp(),
                            finished_at=func.clock_timestamp(),
                        )
                    )
                    session.execute(
                        update(RuntimeJob)
                        .where(RuntimeJob.job_id == str(job_id))
                        .values(
                            status=FAILED, error_code="CANCELLED", error_detail={}, finished_at=func.clock_timestamp()
                        )
                    )
                    self._release_project(session, job_id)
                    return True
                if output_set_id is not None:
                    if job["input_mode"] == VOLATILE:
                        raise ValueError("Volatile resources cannot publish durable project artifacts")
                    output = session.get(ArtifactSet, output_set_id, with_for_update=True, populate_existing=True)
                    if not output or output.producer_attempt_id != job["attempt_id"] or output.kind != EXECUTION:
                        raise ValueError("Output must belong to the authorized attempt")
                    if output.project_id != job["project_id"]:
                        raise ValueError("Output belongs to a different project")
                    if job["kind"] == BUILD_RUN:
                        # 新构建的输出必须引用同一固定源码、摘要、配置及工具链。
                        if job["request_json"].get("buildId"):
                            expected = {
                                "source_set_id": job["input_set_id"],
                                "source_commit_sha": job["request_json"].get("commitSha"),
                                "project_digest": job["request_json"].get("projectDigest"),
                                "config_version": job["config_version"],
                                "toolchain_version": job["toolchain_version"],
                            }
                            if any(value is None or getattr(output, name) != value for name, value in expected.items()):
                                raise ValueError("build output does not match fixed source evidence")
                        files = set(
                            session.scalars(
                                select(ArtifactFile.relative_path).where(ArtifactFile.set_id == output_set_id)
                            )
                        )
                        if not REQUIRED_BUILD_FILES.issubset(files):
                            raise ValueError("BUILD_RUN output is missing required native artifacts")
                        if not all(output.validation_json.get(flag) is True for flag in REQUIRED_VALIDATION_FLAGS):
                            raise ValueError("BUILD_RUN output lacks successful validation evidence")
                    if seal is None:
                        from .artifacts import ArtifactStore

                        seal = ArtifactStore(self.db).seal
                    seal(output_set_id, session)
                session.add(
                    JobResult(
                        job_id=str(job_id),
                        attempt_id=job["attempt_id"],
                        payload_json=payload or {},
                        stdout_tail=stdout_tail.encode()[-self.max_diagnostic_bytes :].decode(errors="ignore"),
                        stderr_tail=stderr_tail.encode()[-self.max_diagnostic_bytes :].decode(errors="ignore"),
                        exit_code=exit_code,
                        output_truncated=output_truncated
                        or len(stdout_tail.encode()) > self.max_diagnostic_bytes
                        or len(stderr_tail.encode()) > self.max_diagnostic_bytes,
                    )
                )
                session.flush()
                # releaseId 候选在封存事务内发布；buildId 构建由任务终态触发器更新构建记录。
                if job["kind"] == BUILD_RUN and job["request_json"].get("releaseId"):
                    if publish is None:
                        raise ValueError("Publication completion requires its transaction coordinator")
                    publish(session, job, cast(str, output_set_id))
                # 封存校验可能耗时；提交前重新 fencing，失效时连同已封存文件状态一起回滚。
                if not self._authorized(session, job_id, token):
                    raise _FinishLeaseLost
                session.execute(
                    update(RuntimeAttempt)
                    .where(RuntimeAttempt.attempt_id == job["attempt_id"])
                    .values(
                        state=SUCCEEDED, stop_confirmed_at=func.clock_timestamp(), finished_at=func.clock_timestamp()
                    )
                )
                session.execute(
                    update(RuntimeJob)
                    .where(RuntimeJob.job_id == str(job_id))
                    .values(
                        status=SUCCEEDED,
                        output_set_id=output_set_id,
                        finished_at=func.clock_timestamp(),
                        error_code=None,
                        error_detail=None,
                    )
                )
                if job["kind"] == RUN_CLEANUP:
                    session.execute(
                        update(RuntimeJob)
                        .where(RuntimeJob.job_id == job["parent_run_id"])
                        .values(run_lifecycle=CLEANED)
                    )
                    # schema 删除已经确认；保留任务/结果与幂等墓碑，将无引用文件交给 GC。
                    session.execute(
                        update(RuntimeJob)
                        .where(
                            or_(
                                RuntimeJob.job_id == job["parent_run_id"],
                                RuntimeJob.parent_run_id == job["parent_run_id"],
                            ),
                            ~select(Build.build_id).where(Build.run_id == RuntimeJob.job_id).exists(),
                            ~select(Build.build_id).where(Build.run_id == RuntimeJob.parent_run_id).exists(),
                        )
                        .values(input_set_id=None, output_set_id=None)
                    )
                if job["kind"] == DBT_COMMAND and output_set_id is not None:
                    # 保留 SQL NULL 等值比较语义；没有输入集合不代表匹配无源码的项目。
                    session.execute(
                        update(RuntimeProject)
                        .where(
                            RuntimeProject.project_id == job["project_id"],
                            RuntimeProject.config_version == job["config_version"],
                            or_(
                                RuntimeProject.source_set_id
                                == literal(job["input_set_id"], type_=RuntimeProject.source_set_id.type),
                                RuntimeProject.source_set_id
                                == select(ArtifactSet.source_set_id)
                                .where(ArtifactSet.set_id == job["input_set_id"])
                                .scalar_subquery(),
                            ),
                        )
                        .values(current_output_set_id=output_set_id, revision=RuntimeProject.revision + 1)
                    )
                self._release_project(session, job_id)
                return True
        except _FinishLeaseLost:
            return False

    def fail(
        self,
        job_id: UUID | str,
        token: UUID | str,
        error_code: str,
        detail: JsonObject | None = None,
        *,
        stopped: bool = True,
    ) -> bool:
        # 错误详情也有字节预算，禁止在失败路径写入无限增长的异常堆栈。
        if len(json.dumps(detail or {}, ensure_ascii=False).encode()) > self.max_diagnostic_bytes:
            detail = {"message": "Diagnostic exceeded configured size limit"}
        with self.db.fact_session() as session:
            job = self._authorized(session, job_id, token)
            if not job:
                return False
            session.execute(
                update(RuntimeAttempt)
                .where(RuntimeAttempt.attempt_id == job["attempt_id"])
                .values(
                    state=FAILED if stopped else EXPIRED_UNCONFIRMED,
                    stop_confirmed_at=func.clock_timestamp() if stopped else None,
                    finished_at=func.clock_timestamp(),
                )
            )
            session.execute(
                update(RuntimeJob)
                .where(RuntimeJob.job_id == str(job_id))
                .values(
                    status=FAILED, error_code=error_code, error_detail=detail or {}, finished_at=func.clock_timestamp()
                )
            )

            session.execute(
                update(Release)
                .where(Release.run_id == str(job_id), Release.state.not_in((PUBLISHED, SUPERSEDED)))
                .values(state=FAILED, error_code=error_code)
            )
            self._release_project(session, job_id)
            return True

    def recover(self) -> int:
        # 恢复只处理过期租约/截止时间，健康实例的 RUNNING 任务不会被启动流程打断。
        with self.db.fact_session() as session:
            jobs = session.scalars(
                select(RuntimeJob)
                .where(
                    RuntimeJob.status.in_((QUEUED, RUNNING)),
                    or_(
                        RuntimeJob.deadline_at <= func.clock_timestamp(),
                        and_(
                            RuntimeJob.input_mode == VOLATILE,
                            RuntimeJob.input_lease_expires_at <= func.clock_timestamp(),
                        ),
                        select(RuntimeAttempt.attempt_id)
                        .where(
                            RuntimeAttempt.attempt_id == RuntimeJob.current_attempt_id,
                            RuntimeAttempt.lease_expires_at <= func.clock_timestamp(),
                        )
                        .exists(),
                    ),
                )
                .order_by(RuntimeJob.created_at)
                .with_for_update(of=RuntimeJob, skip_locked=True)
            ).all()
            for job in jobs:
                now = session.scalar(select(func.clock_timestamp()))
                attempt = (
                    session.get(RuntimeAttempt, job.current_attempt_id, with_for_update=True)
                    if job.current_attempt_id
                    else None
                )
                external = bool(attempt and attempt.execution_stage == EXTERNAL)
                lost = job.input_mode == VOLATILE and job.input_lease_expires_at <= now
                timed_out = job.deadline_at <= now
                if attempt:
                    attempt.state = EXPIRED_UNCONFIRMED if external else STOPPED
                    attempt.finished_at = func.clock_timestamp()
                    attempt.stop_confirmed_at = None if external else func.clock_timestamp()
                    session.flush()
                retry = (
                    not lost
                    and not timed_out
                    and job.input_mode == DURABLE
                    and job.attempt_no < job.max_attempts
                    and job.retry_policy != NEVER
                    and (not external or job.retry_policy == READ_ONLY)
                )
                if retry:
                    job.status = QUEUED
                    job.current_attempt_id = None
                    delay = 5 if job.attempt_no <= 1 else 15
                    job.available_at = func.clock_timestamp() + timedelta(seconds=delay)
                    session.flush()
                else:
                    code = (
                        INPUT_LOST if lost else TIMEOUT if timed_out else OUTCOME_UNKNOWN if external else LEASE_EXPIRED
                    )
                    job.status = FAILED
                    job.error_code = code
                    job.error_detail = {"externalOutcomeUnknown": external}
                    job.finished_at = func.clock_timestamp()
                    # 先提交任务/attempt 修改到事务，再让发布状态和项目解锁读取最新状态。
                    session.flush()
                    session.execute(
                        update(Release)
                        .where(
                            Release.run_id == job.job_id,
                            Release.state.not_in((PUBLISHED, SUPERSEDED)),
                        )
                        .values(state=FAILED, error_code=code)
                    )
                    self._release_project(session, job.job_id)
            return len(jobs)

    def confirm_stopped(self, attempt_id: UUID | str, token: UUID | str) -> bool:
        # 失效执行者仅可凭自己的 token 确认停止，不能改写公开结果。
        with self.db.fact_session() as session:
            job_id = session.scalar(
                select(RuntimeAttempt.job_id).where(
                    RuntimeAttempt.attempt_id == str(attempt_id), RuntimeAttempt.lease_token == str(token)
                )
            )
            if not job_id:
                return False
            job = session.get(RuntimeJob, job_id, with_for_update=True)
            session.execute(
                update(RuntimeAttempt)
                .where(RuntimeAttempt.attempt_id == str(attempt_id), RuntimeAttempt.state == EXPIRED_UNCONFIRMED)
                .values(state=STOPPED, stop_confirmed_at=func.clock_timestamp())
            )
            if job.status in (SUCCEEDED, FAILED):
                self._release_project(session, job_id)
            return True

    def reserve_cleanup(self, parent_run_id: UUID | str, toolchain_version: str = "default") -> StoredJob:
        # 与查询受理锁同一 parent；阻止所有排队/执行任务和任何未确认停止的历史 attempt。
        parent_run_id = str(parent_run_id)
        with self.db.fact_session() as session:
            parent = session.get(RuntimeJob, parent_run_id, with_for_update=True)
            if not parent or parent.kind != BUILD_RUN:
                raise ValueError("Run does not exist")
            # 发布历史、复用关系和 v3 当前部署共同保护物理 run。
            protected = session.scalar(
                select(
                    or_(
                        select(Release.release_id).where(Release.run_id == parent_run_id).exists(),
                        select(ReleaseRelation.release_id)
                        .where(ReleaseRelation.creator_run_id == parent_run_id)
                        .exists(),
                        select(DeploymentTarget.active_build_id)
                        .join(
                            Build,
                            Build.build_id == DeploymentTarget.active_build_id,
                        )
                        .where(Build.run_id == parent_run_id)
                        .exists(),
                    )
                )
            )
            if protected:
                raise CleanupBlocked("run_cleanup_blocked")
            existing = session.scalar(
                select(RuntimeJob).where(
                    RuntimeJob.kind == RUN_CLEANUP,
                    RuntimeJob.parent_run_id == parent_run_id,
                )
            )
            if existing:
                if existing.status == FAILED:
                    if session.scalar(
                        select(RuntimeAttempt.attempt_id)
                        .where(
                            RuntimeAttempt.job_id == existing.job_id,
                            RuntimeAttempt.execution_stage == EXTERNAL,
                            RuntimeAttempt.stop_confirmed_at.is_(None),
                        )
                        .limit(1)
                    ):
                        raise CleanupBlocked("Cleanup outcome is still unknown")
                    existing.status = QUEUED
                    existing.current_attempt_id = None
                    existing.available_at = func.clock_timestamp()
                    existing.deadline_at = func.clock_timestamp() + timedelta(minutes=10)
                    existing.finished_at = None
                    existing.error_code = None
                    session.flush()
                return cast(StoredJob, entity_dict(existing))
            run_family = or_(RuntimeJob.job_id == parent_run_id, RuntimeJob.parent_run_id == parent_run_id)
            if session.scalar(
                select(
                    or_(
                        select(RuntimeJob.job_id).where(run_family, RuntimeJob.status.in_((QUEUED, RUNNING))).exists(),
                        select(RuntimeAttempt.attempt_id)
                        .join(RuntimeJob, RuntimeJob.job_id == RuntimeAttempt.job_id)
                        .where(
                            run_family,
                            RuntimeAttempt.execution_stage == EXTERNAL,
                            RuntimeAttempt.stop_confirmed_at.is_(None),
                        )
                        .exists(),
                    )
                )
            ):
                raise CleanupBlocked("run_cleanup_blocked")
            if not (parent.output_set_id or parent.input_set_id) and session.scalar(
                select(RuntimeAttempt.attempt_id)
                .where(
                    RuntimeAttempt.job_id == parent_run_id,
                    RuntimeAttempt.execution_stage == EXTERNAL,
                )
                .limit(1)
            ):
                raise CleanupBlocked("Restore source artifacts before cleaning a run that executed externally")
            parent.run_lifecycle = CLEANING
            session.flush()
            cleanup = RuntimeJob(
                job_id=str(uuid4()),
                kind=RUN_CLEANUP,
                project_id=parent.project_id,
                parent_run_id=parent_run_id,
                idempotency_scope=CLEANUP_SCOPE,
                idempotency_key=parent_run_id,
                request_fingerprint=parent.request_fingerprint,
                config_version=parent.config_version,
                toolchain_version=parent.toolchain_version,
                schema_name=parent.schema_name,
                profile_binding_id=parent.profile_binding_id,
                input_set_id=parent.output_set_id or parent.input_set_id,
                deadline_at=func.clock_timestamp() + timedelta(minutes=10),
                retry_policy=PREPARATION_ONLY,
            )
            session.add(cleanup)
            session.flush()
            return cast(StoredJob, entity_dict(cleanup))
