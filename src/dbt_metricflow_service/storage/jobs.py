"""数据库队列、执行租约与发布事务；外部 SQL 的停止必须单独确认。"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from datetime import timedelta
from typing import Final
from uuid import UUID, uuid4

from sqlalchemy import func, select, true, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from dbt_metricflow_service import constants as strings
from dbt_metricflow_service.models.execution import ExecutionSnapshot
from dbt_metricflow_service.models.payloads import JsonObject
from dbt_metricflow_service.storage.records import (
    EmptyConfig,
    JobErrorDetail,
    JobRequest,
    LeasedJob,
    StoredJob,
    StoredProject,
)
from dbt_metricflow_service.storage.rows import entity_record

from .entities import (
    ArtifactFile,
    ArtifactSet,
    RuntimeAttempt,
    RuntimeJob,
    RuntimeProject,
)
from .postgres import Database

BUILD_SOURCE_EVIDENCE_CONFLICT_MESSAGE: Final = "build output does not match fixed source evidence"
DIAGNOSTIC_SIZE_LIMIT_MESSAGE: Final = "Diagnostic exceeded configured size limit"
IDEMPOTENCY_REQUEST_CONFLICT_MESSAGE: Final = "Idempotency key belongs to another request"
IMMUTABLE_JOB_INPUT_MESSAGE: Final = "Job input is immutable after admission"
INACTIVE_RUN_MESSAGE: Final = "Run is not ready and active"
INVALID_PARENT_RUN_MESSAGE: Final = "Parent must be a BUILD_RUN of this project"
INVALID_PROJECT_SOURCE_MESSAGE: Final = "Project source must be a sealed source of the same project"
INVALID_SOURCE_SET_MESSAGE: Final = "Invalid source artifact set"
MISSING_NATIVE_ARTIFACTS_MESSAGE: Final = "BUILD_RUN output is missing required native artifacts"
OUTPUT_ATTEMPT_CONFLICT_MESSAGE: Final = "Output must belong to the authorized attempt"
OUTPUT_PROJECT_CONFLICT_MESSAGE: Final = "Output belongs to a different project"
PROJECT_CHANGED_MESSAGE: Final = "Project changed during request validation; retry"
REQUIRED_BUILD_OUTPUT_MESSAGE: Final = "BUILD_RUN requires a complete validated output artifact set"
UNAVAILABLE_INPUT_SET_MESSAGE: Final = "Input artifact set is not available"
UNPUBLISHED_PARENT_MESSAGE: Final = "Parent has no published artifact set"
UNREGISTERED_PROJECT_MESSAGE: Final = "Project is not registered"

# ORM 表达式显式保留条件更新和行锁，时间边界由数据库判断。
EXECUTING = strings.EXECUTING
PREPARATION_ONLY = strings.PREPARATION_ONLY
PREPARING = strings.PREPARING
QUEUED = strings.QUEUED
RUNNING = strings.RUNNING
SUCCEEDED = strings.SUCCEEDED
FAILED = strings.FAILED
BUILD_RUN = strings.BUILD_RUN
NEVER = strings.NEVER
EXTERNAL = strings.EXTERNAL
OUTCOME_UNKNOWN = strings.EXECUTION_OUTCOME_UNKNOWN
LEASE_EXPIRED = strings.LEASE_EXPIRED
SEALED = strings.SEALED
EXECUTION = strings.EXECUTION
SOURCE = strings.SOURCE
EXPIRED_UNCONFIRMED = strings.EXPIRED_UNCONFIRMED
STOPPED = strings.STOPPED
REQUIRED_BUILD_FILES = frozenset({strings.MANIFEST_PATH})
MAX_DIAGNOSTIC = 1024 * 1024


class _FinishLeaseLost(Exception):
    """Abort all writes when the final lease check fails."""


class StoreConflict(ValueError):
    """幂等键已用于不同请求。"""


class ProjectBusy(ValueError):
    """项目存在尚未确认结束的写操作。"""


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
        binding_config: ExecutionSnapshot | EmptyConfig | None = None,
        config_version: str = strings.DEFAULT_CONFIG_VERSION,
        source_set_id: str | None = None,
    ) -> StoredProject:
        # 项目导入与任务受理锁同一项目行，保持固定输入归属。
        with self.db.session() as session:
            session.execute(
                pg_insert(RuntimeProject)
                .values(
                    project_id=project_id,
                    binding_config=binding_config.model_dump(mode=strings.JSON_MODE, by_alias=True)
                    if binding_config
                    else {},
                    config_version=config_version,
                )
                .on_conflict_do_nothing(index_elements=[RuntimeProject.project_id])
            )
            project = session.get(RuntimeProject, project_id, with_for_update=True, populate_existing=True)
            if source_set_id is not None:
                source = session.get(ArtifactSet, source_set_id)
                if not source or source.state != SEALED or source.kind != SOURCE or source.project_id != project_id:
                    raise ValueError(INVALID_PROJECT_SOURCE_MESSAGE)
            if binding_config is not None:
                project.binding_config = binding_config.model_dump(mode=strings.JSON_MODE, by_alias=True)
            if source_set_id is not None:
                project.source_set_id = source_set_id
            project.config_version = config_version
            project.revision += 1
            session.flush()
            return entity_record(project, StoredProject)

    def project(self, project_id: str) -> StoredProject | None:
        with self.db.session() as session:
            return entity_record(session.get(RuntimeProject, project_id), StoredProject)

    def get(self, job_id: UUID | str) -> StoredJob | None:
        with self.db.session() as session:
            return entity_record(session.get(RuntimeJob, str(job_id)), StoredJob)

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
        project_id: str,
        request_json: JobRequest,
        *,
        job_id: UUID | str | None = None,
        fingerprint: str | None = None,
        idempotency_scope: str | None = None,
        idempotency_key: str | None = None,
        parent_run_id: UUID | str | None = None,
        input_set_id: str | None = None,
        config_version: str = strings.DEFAULT_CONFIG_VERSION,
        toolchain_version: str = strings.DEFAULT_TOOLCHAIN_NAMESPACE,
        schema_name: str | None = None,
        profile_binding_id: str | None = None,
        retry_policy: str = strings.PREPARATION_ONLY,
        max_attempts: int = 3,
        write: bool = False,
        expected_revision: int | None = None,
    ) -> StoredJob:
        # 幂等作用域先串行化；parent 锁统一先于项目行和子任务，保持父子任务锁定顺序。
        with self.db.fact_session() as session:
            return self.reserve_in_transaction(
                session,
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
                write=write,
                expected_revision=expected_revision,
            )

    def reserve_in_transaction(
        self,
        connection: Session,
        project_id: str,
        request_json: JobRequest,
        *,
        job_id: UUID | str | None = None,
        fingerprint: str | None = None,
        idempotency_scope: str | None = None,
        idempotency_key: str | None = None,
        parent_run_id: UUID | str | None = None,
        input_set_id: str | None = None,
        config_version: str = strings.DEFAULT_CONFIG_VERSION,
        toolchain_version: str = strings.DEFAULT_TOOLCHAIN_NAMESPACE,
        schema_name: str | None = None,
        profile_binding_id: str | None = None,
        retry_policy: str = strings.PREPARATION_ONLY,
        max_attempts: int = 3,
        write: bool = False,
        expected_revision: int | None = None,
    ) -> StoredJob:
        # 幂等作用域先串行化；parent 锁统一先于项目行和子任务，保持父子任务锁定顺序。
        session = connection
        if idempotency_key is not None:
            idempotency_scope = idempotency_scope or BUILD_RUN
            session.execute(
                select(func.pg_advisory_xact_lock(func.hashtextextended(idempotency_scope + ":" + idempotency_key, 0)))
            )
        parent = None
        if parent_run_id is not None:
            parent = session.get(RuntimeJob, str(parent_run_id), with_for_update=True, populate_existing=True)
            if not parent or parent.project_id != project_id:
                raise ValueError(INVALID_PARENT_RUN_MESSAGE)
        project = session.get(RuntimeProject, project_id, with_for_update=True, populate_existing=True)
        if not project:
            raise ValueError(UNREGISTERED_PROJECT_MESSAGE)
        # 前置 manifest 校验之后若项目发生导入，拒绝混用旧输入与新配置。
        if expected_revision is not None and (
            project.revision != expected_revision or project.config_version != config_version
        ):
            raise StoreConflict(PROJECT_CHANGED_MESSAGE)
        if input_set_id is None and parent:
            input_set_id = parent.output_set_id
        safe_request = request_json.model_dump(
            mode=strings.JSON_MODE, by_alias=True,
            exclude=set(JobRequest.model_fields) - request_json.model_fields_set,
        )
        encoded = json.dumps(
            [
                fingerprint or safe_request,
                BUILD_RUN,  # 保留既有幂等摘要格式，避免升级后重试冲突。
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
                    raise StoreConflict(IDEMPOTENCY_REQUEST_CONFLICT_MESSAGE)
                return entity_record(prior, StoredJob)
        if parent and parent.status != SUCCEEDED:
            raise ValueError(INACTIVE_RUN_MESSAGE)
        if input_set_id is not None:
            artifact = session.get(ArtifactSet, input_set_id)
            if not artifact or artifact.state != SEALED or artifact.project_id != project_id:
                raise ValueError(UNAVAILABLE_INPUT_SET_MESSAGE)
        elif parent:
            raise ValueError(UNPUBLISHED_PARENT_MESSAGE)
        if write and project.busy_job_id is not None:
            raise ProjectBusy(strings.PROJECT_BUSY)
        job = RuntimeJob(
            job_id=str(job_id or uuid4()),
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
            retry_policy=retry_policy,
            max_attempts=max_attempts,
        )
        session.add(job)
        # 先写任务并取得数据库默认值，再设置引用该任务的项目忙碌指针。
        session.flush()
        if write:
            project.busy_job_id = job.job_id
            session.flush()
        return entity_record(job, StoredJob)

    def claim(
        self,
        worker_id: UUID | str,
        *,
        config_version: str = strings.DEFAULT_CONFIG_VERSION,
        config_versions: list[str] | None = None,
        toolchain_version: str = strings.DEFAULT_TOOLCHAIN_NAMESPACE,
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
                    RuntimeJob.config_version.in_(versions),
                    RuntimeJob.toolchain_version == toolchain_version,
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
            return LeasedJob(
                **entity_record(job, StoredJob).model_dump(exclude_unset=True),
                attempt_id=attempt_id, lease_token=token,
            )

    def _authorized(self, session: Session, job_id: UUID | str, token: UUID | str) -> LeasedJob | None:
        # 子任务更新先锁 parent；每次修改同时核对 current attempt、token 和数据库时间。
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
            )
            .with_for_update(of=(RuntimeJob, RuntimeAttempt))
            .execution_options(populate_existing=True)
        ).one_or_none()
        if row is None:
            return None
        job, attempt = row
        return LeasedJob(
            **entity_record(job, StoredJob).model_dump(exclude_unset=True), attempt_id=attempt.attempt_id,
            execution_stage=attempt.execution_stage, lease_token=attempt.lease_token,
        )

    def heartbeat(self, job_id: UUID | str, token: UUID | str) -> bool:
        with self.db.session() as session:
            job = self._authorized(session, job_id, token)
            if not job:
                return False
            session.execute(
                update(RuntimeAttempt)
                .where(RuntimeAttempt.attempt_id == job.attempt_id)
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
        # External engine session identifiers form an adapter-defined map.
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
                    .where(RuntimeAttempt.attempt_id == job.attempt_id)
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
            if not source or source.state != SEALED or source.project_id != job.project_id:
                raise ValueError(INVALID_SOURCE_SET_MESSAGE)
            if job.input_set_id is not None and job.input_set_id != str(set_id):
                raise ValueError(IMMUTABLE_JOB_INPUT_MESSAGE)
            session.execute(update(RuntimeJob).where(RuntimeJob.job_id == str(job_id)).values(input_set_id=set_id))
            if project_digest is not None:
                session.execute(
                    update(RuntimeJob)
                    .where(RuntimeJob.job_id == str(job_id))
                    .values(request_json=RuntimeJob.request_json.concat(JobRequest(project_digest=project_digest).model_dump(
                        mode=strings.JSON_MODE, by_alias=True, exclude_unset=True,
                    )))
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
            return self.fail(job_id, token, error_code or strings.EXECUTION_FAILED)
        try:
            with self.db.fact_session() as session:
                job = self._authorized(session, job_id, token)
                if not job:
                    return False
                if output_set_id is None:
                    raise ValueError(REQUIRED_BUILD_OUTPUT_MESSAGE)
                output = session.get(ArtifactSet, output_set_id, with_for_update=True, populate_existing=True)
                if not output or output.producer_attempt_id != job.attempt_id or output.kind != EXECUTION:
                    raise ValueError(OUTPUT_ATTEMPT_CONFLICT_MESSAGE)
                if output.project_id != job.project_id:
                    raise ValueError(OUTPUT_PROJECT_CONFLICT_MESSAGE)
                # 新构建的输出必须引用同一固定源码、摘要、配置及工具链。
                if job.request_json.build_id:
                    if (
                        job.input_set_id is None or output.source_set_id != job.input_set_id
                        or job.request_json.commit_sha is None
                        or output.source_commit_sha != job.request_json.commit_sha
                        or job.request_json.project_digest is None
                        or output.project_digest != job.request_json.project_digest
                        or output.config_version != job.config_version
                        or output.toolchain_version != job.toolchain_version
                    ):
                        raise ValueError(BUILD_SOURCE_EVIDENCE_CONFLICT_MESSAGE)
                files = set(
                    session.scalars(
                        select(ArtifactFile.relative_path).where(ArtifactFile.set_id == output_set_id)
                    )
                )
                if not REQUIRED_BUILD_FILES.issubset(files):
                    raise ValueError(MISSING_NATIVE_ARTIFACTS_MESSAGE)
                if seal is None:
                    from .artifacts import ArtifactStore
                    seal = ArtifactStore(self.db).seal
                seal(output_set_id, session)
                # 封存校验可能耗时；提交前重新 fencing，失效时连同已封存文件状态一起回滚。
                if not self._authorized(session, job_id, token):
                    raise _FinishLeaseLost
                session.execute(
                    update(RuntimeAttempt)
                    .where(RuntimeAttempt.attempt_id == job.attempt_id)
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
                self._release_project(session, job_id)
                return True
        except _FinishLeaseLost:
            return False

    def fail(
        self,
        job_id: UUID | str,
        token: UUID | str,
        error_code: str,
        detail: JobErrorDetail | None = None,
        *,
        stopped: bool = True,
    ) -> bool:
        # 错误详情也有字节预算，禁止在失败路径写入无限增长的异常堆栈。
        diagnostic = (detail or JobErrorDetail()).model_dump(mode=strings.JSON_MODE, by_alias=True, exclude_unset=True)
        if len(json.dumps(diagnostic, ensure_ascii=False).encode()) > self.max_diagnostic_bytes:
            diagnostic = JobErrorDetail(message=DIAGNOSTIC_SIZE_LIMIT_MESSAGE).model_dump(
                mode=strings.JSON_MODE, by_alias=True, exclude_unset=True,
            )
        with self.db.fact_session() as session:
            job = self._authorized(session, job_id, token)
            if not job:
                return False
            session.execute(
                update(RuntimeAttempt)
                .where(RuntimeAttempt.attempt_id == job.attempt_id)
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
                    status=FAILED, error_code=error_code, error_detail=diagnostic, finished_at=func.clock_timestamp()
                )
            )

            self._release_project(session, job_id)
            return True

    def recover(self) -> int:
        # 恢复只处理过期租约，健康实例的 RUNNING 任务不会被启动流程打断。
        with self.db.fact_session() as session:
            jobs = session.scalars(
                select(RuntimeJob)
                .where(
                    RuntimeJob.status.in_((QUEUED, RUNNING)),
                    select(RuntimeAttempt.attempt_id)
                    .where(
                        RuntimeAttempt.attempt_id == RuntimeJob.current_attempt_id,
                        RuntimeAttempt.lease_expires_at <= func.clock_timestamp(),
                    )
                    .exists(),
                )
                .order_by(RuntimeJob.created_at)
                .with_for_update(of=RuntimeJob, skip_locked=True)
            ).all()
            for job in jobs:
                attempt = (
                    session.get(RuntimeAttempt, job.current_attempt_id, with_for_update=True)
                    if job.current_attempt_id
                    else None
                )
                external = bool(attempt and attempt.execution_stage == EXTERNAL)
                if attempt:
                    attempt.state = EXPIRED_UNCONFIRMED if external else STOPPED
                    attempt.finished_at = func.clock_timestamp()
                    attempt.stop_confirmed_at = None if external else func.clock_timestamp()
                    session.flush()
                retry = (
                    job.attempt_no < job.max_attempts
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
                    code = OUTCOME_UNKNOWN if external else LEASE_EXPIRED
                    job.status = FAILED
                    job.error_code = code
                    job.error_detail = JobErrorDetail(external_outcome_unknown=external).model_dump(
                        mode=strings.JSON_MODE, by_alias=True, exclude_unset=True,
                    )
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
