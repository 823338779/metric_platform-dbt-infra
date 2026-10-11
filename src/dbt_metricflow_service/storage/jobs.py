"""数据库队列、执行租约与发布事务；外部 SQL 的停止必须单独确认。"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import timedelta
from typing import cast, overload
from uuid import UUID, uuid4

from sqlalchemy import func, or_, select, true, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from dbt_metricflow_service.models.payloads import JsonObject, JsonValue
from dbt_metricflow_service.storage.records import DatabaseRow, LeasedJob, StoredJob
from dbt_metricflow_service.storage.rows import entity_dict

from .entities import (
    ArtifactFile,
    ArtifactSet,
    Build,
    DeploymentTarget,
    RuntimeAttempt,
    RuntimeJob,
    RuntimeProject,
)
from .postgres import Database

# ORM 表达式显式保留条件更新和行锁，时间边界由数据库判断。
EXECUTING = "EXECUTING"
PREPARATION_ONLY = "PREPARATION_ONLY"
PREPARING = "PREPARING"
QUEUED = "QUEUED"
RUNNING = "RUNNING"
SUCCEEDED = "SUCCEEDED"
FAILED = "FAILED"
BUILD_RUN = "BUILD_RUN"
RUN_CLEANUP = "RUN_CLEANUP"
NEVER = "NEVER"
EXTERNAL = "EXTERNAL"
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
    {"target/manifest.json", "target/semantic_manifest.json", "target/catalog.json", "target/published_catalog.json"}
)
REQUIRED_VALIDATION_FLAGS = ("allTestsPassed", "representativeQueryPassed", "relationsVerified")
MAX_DIAGNOSTIC = 1024 * 1024


FORBIDDEN_REQUEST_KEYS = frozenset({"resources", "credentials", "password", "token", "secret", "environment", "env"})


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
    # resources 和凭据不进入持久请求。
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
        max_diagnostic_bytes: int = MAX_DIAGNOSTIC,
    ) -> None:
        # 数据库共享控制状态；租约长度只决定后续新租约的有效期。
        self.db = db
        self.lease_seconds = lease_seconds
        # 错误诊断设限，避免单条记录撑满内容存储。
        self.max_diagnostic_bytes = max_diagnostic_bytes

    def register_project(
        self,
        project_id: str,
        binding_config: JsonObject | None = None,
        config_version: str = "1",
        source_set_id: str | None = None,
    ) -> DatabaseRow:
        # 项目导入与任务受理锁同一项目行，保持固定输入归属。
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
            if binding_config is not None:
                project.binding_config = binding_config
            if source_set_id is not None:
                project.source_set_id = source_set_id
            project.config_version = config_version
            project.revision += 1
            session.flush()
            return entity_dict(project)

    def project(self, project_id: str) -> DatabaseRow | None:
        with self.db.session() as session:
            return entity_dict(session.get(RuntimeProject, project_id))

    def get(self, job_id: UUID | str) -> StoredJob | None:
        with self.db.session() as session:
            return cast(StoredJob | None, entity_dict(session.get(RuntimeJob, str(job_id))))

    @contextmanager
    def query_run(self, run_id: UUID | str) -> Iterator[StoredJob]:
        """只持有会话锁保护实时查询，不创建任务、租约或结果记录。"""
        key = func.hashtextextended("query:" + str(run_id), 0)
        # 会话锁覆盖引擎执行；autocommit 避免持有长事务和行锁。
        with self.db.engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
            connection.execute(select(func.pg_advisory_lock_shared(key)))
            try:
                row = connection.execute(
                    select(RuntimeJob).where(RuntimeJob.job_id == str(run_id))
                ).mappings().one_or_none()
                parent = cast(StoredJob, dict(row)) if row is not None else None
                if not parent or parent["kind"] != BUILD_RUN or parent["status"] != SUCCEEDED or (
                    parent["run_lifecycle"] != ACTIVE
                ):
                    raise CleanupBlocked("Run is not ready and active")
                yield parent
            finally:
                try:
                    connection.execute(select(func.pg_advisory_unlock_shared(key)))
                except Exception:
                    # 不能把仍可能持有会话锁的连接归还连接池。
                    connection.invalidate()
                    raise

    def external_started(self, attempt_id: UUID | str) -> bool:
        # 取消时以持久 attempt 为准；无记录时不能证明外部执行已经停止。
        with self.db.session() as session:
            return (
                session.scalar(
                    select(RuntimeAttempt.execution_stage).where(RuntimeAttempt.attempt_id == str(attempt_id))
                )
                != PREPARING
            )

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
        config_version: str = "1",
        toolchain_version: str = "default",
        schema_name: str | None = None,
        profile_binding_id: str | None = None,
        retry_policy: str = "PREPARATION_ONLY",
        max_attempts: int = 3,
        timeout_seconds: int = 600,
        write: bool = False,
        expected_revision: int | None = None,
    ) -> StoredJob:
        if kind not in (BUILD_RUN, RUN_CLEANUP):
            raise ValueError(f"Unsupported job kind: {kind}")
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
                    config_version=config_version,
                    toolchain_version=toolchain_version,
                    schema_name=schema_name,
                    profile_binding_id=profile_binding_id,
                    retry_policy=retry_policy,
                    max_attempts=max_attempts,
                    timeout_seconds=timeout_seconds,
                    write=write,
                    expected_revision=expected_revision,
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
        config_version: str = "1",
        toolchain_version: str = "default",
        schema_name: str | None = None,
        profile_binding_id: str | None = None,
        retry_policy: str = "PREPARATION_ONLY",
        max_attempts: int = 3,
        timeout_seconds: int = 600,
        write: bool = False,
        expected_revision: int | None = None,
    ) -> StoredJob:
        if kind not in (BUILD_RUN, RUN_CLEANUP):
            raise ValueError(f"Unsupported job kind: {kind}")
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
        if input_set_id is None and parent:
            input_set_id = parent.output_set_id
        safe_request = _safe_request(request_json)
        encoded = json.dumps(
            [
                fingerprint or safe_request,
                kind,
                project_id,
                str(parent_run_id) if parent_run_id else None,
                input_set_id,
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
        job = RuntimeJob(
            job_id=str(job_id or uuid4()),
            kind=kind,
            project_id=project_id,
            parent_run_id=str(parent_run_id) if parent_run_id is not None else None,
            idempotency_scope=idempotency_scope,
            idempotency_key=idempotency_key,
            request_fingerprint=fingerprint,
            request_json=safe_request,
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

    def claim(
        self,
        worker_id: UUID | str,
        *,
        config_version: str = "1",
        config_versions: list[str] | None = None,
        toolchain_version: str = "default",
        kinds: list[str] | None = None,
        job_id: UUID | str | None = None,
    ) -> LeasedJob | None:
        # 短事务领取一个任务，跳过其他 worker 已锁住的任务。
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
                    # 指定任务时只认领该记录。
                    true() if job_id is None else RuntimeJob.job_id == str(job_id),
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
        *,
        succeeded: bool = True,
        error_code: str | None = None,
        output_set_id: str | None = None,
        seal: Callable[[str, Session], None] | None = None,
    ) -> bool:
        # 产物封存和终态共用一个事务；过期 worker 无权提交。
        if not succeeded:
            return self.fail(job_id, token, error_code or "EXECUTION_FAILED")
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
                    # schema 删除已经确认；保留任务与幂等墓碑，将无引用文件交给 GC。
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
                timed_out = job.deadline_at <= now
                if attempt:
                    attempt.state = EXPIRED_UNCONFIRMED if external else STOPPED
                    attempt.finished_at = func.clock_timestamp()
                    attempt.stop_confirmed_at = None if external else func.clock_timestamp()
                    session.flush()
                retry = (
                    not timed_out
                    and job.attempt_no < job.max_attempts
                    and job.retry_policy != NEVER
                    and not external
                )
                if retry:
                    job.status = QUEUED
                    job.current_attempt_id = None
                    delay = 5 if job.attempt_no <= 1 else 15
                    job.available_at = func.clock_timestamp() + timedelta(seconds=delay)
                    session.flush()
                else:
                    code = TIMEOUT if timed_out else OUTCOME_UNKNOWN if external else LEASE_EXPIRED
                    job.status = FAILED
                    job.error_code = code
                    job.error_detail = {"externalOutcomeUnknown": external}
                    job.finished_at = func.clock_timestamp()
                    # 先提交任务/attempt 修改到事务，再让项目解锁读取最新状态。
                    session.flush()
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
        # 先与实时查询的会话锁互斥，再检查持久任务及未确认停止的历史 attempt。
        parent_run_id = str(parent_run_id)
        with self.db.fact_session() as session:
            if not session.scalar(select(func.pg_try_advisory_xact_lock(
                func.hashtextextended("query:" + parent_run_id, 0)
            ))):
                raise CleanupBlocked("run_cleanup_blocked")
            parent = session.get(RuntimeJob, parent_run_id, with_for_update=True)
            if not parent or parent.kind != BUILD_RUN:
                raise ValueError("Run does not exist")
            # 当前部署保护物理 run。
            protected = session.scalar(
                select(
                    select(DeploymentTarget.active_build_id)
                    .join(Build, Build.build_id == DeploymentTarget.active_build_id)
                    .where(Build.run_id == parent_run_id)
                    .exists()
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
