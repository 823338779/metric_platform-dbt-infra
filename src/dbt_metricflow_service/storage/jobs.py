"""数据库队列、执行租约与发布事务；外部 SQL 的停止必须单独确认。"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import timedelta
from typing import Final
from uuid import UUID, uuid4

from sqlalchemy import func, select, true, update
from sqlalchemy.orm import Session

from dbt_metricflow_service import constants as strings
from dbt_metricflow_service.models.contracts import JsonObject
from dbt_metricflow_service.storage.records import (
    JobErrorDetail,
    LeasedBuild,
)

from .entities import (
    ArtifactFile,
    ArtifactSet,
    Build,
    RuntimeAttempt,
)
from .postgres import Database

BUILD_SOURCE_EVIDENCE_CONFLICT_MESSAGE: Final = "build output does not match fixed source evidence"
DIAGNOSTIC_SIZE_LIMIT_MESSAGE: Final = "Diagnostic exceeded configured size limit"
IMMUTABLE_JOB_INPUT_MESSAGE: Final = "Job input is immutable after admission"
INVALID_SOURCE_SET_MESSAGE: Final = "Invalid source artifact set"
MISSING_NATIVE_ARTIFACTS_MESSAGE: Final = "BUILD_RUN output is missing required native artifacts"
OUTPUT_ATTEMPT_CONFLICT_MESSAGE: Final = "Output must belong to the authorized attempt"
OUTPUT_PROJECT_CONFLICT_MESSAGE: Final = "Output belongs to a different project"
REQUIRED_BUILD_OUTPUT_MESSAGE: Final = "BUILD_RUN requires a complete validated output artifact set"

# ORM 表达式显式保留条件更新和行锁，时间边界由数据库判断。
EXECUTING = strings.EXECUTING
PREPARATION_ONLY = strings.PREPARATION_ONLY
PREPARING = strings.PREPARING
QUEUED = strings.QUEUED
RUNNING = strings.RUNNING
SUCCEEDED = strings.SUCCEEDED
FAILED = strings.FAILED
NEVER = strings.NEVER
EXTERNAL = strings.EXTERNAL
OUTCOME_UNKNOWN = strings.EXECUTION_OUTCOME_UNKNOWN
LEASE_EXPIRED = strings.LEASE_EXPIRED
SEALED = strings.SEALED
EXECUTION = strings.EXECUTION
EXPIRED_UNCONFIRMED = strings.EXPIRED_UNCONFIRMED
STOPPED = strings.STOPPED
REQUIRED_BUILD_FILES = frozenset({strings.MANIFEST_PATH})
MAX_DIAGNOSTIC = 1024 * 1024


class _FinishLeaseLost(Exception):
    """Abort all writes when the final lease check fails."""


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

    def external_started(self, attempt_id: UUID | str) -> bool:
        # 取消时以持久 attempt 为准；无记录时不能证明外部执行已经停止。
        with self.db.session() as session:
            return (
                session.scalar(
                    select(RuntimeAttempt.execution_stage).where(RuntimeAttempt.attempt_id == str(attempt_id))
                )
                != PREPARING
            )

    def claim(
        self,
        worker_id: UUID | str,
        *,
        toolchain_version: str = strings.DEFAULT_TOOLCHAIN_NAMESPACE,
        build_id: UUID | str | None = None,
    ) -> LeasedBuild | None:
        # 短事务领取一个任务，跳过其他 worker 已锁住的任务。
        with self.db.fact_session() as session:
            job = session.scalar(
                select(Build)
                .where(
                    Build.build_status == QUEUED,
                    Build.available_at <= func.clock_timestamp(),
                    Build.toolchain_version == toolchain_version,
                    # 指定任务时只认领该记录。
                    true() if build_id is None else Build.build_id == str(build_id),
                )
                .order_by(Build.available_at, Build.created_at)
                .limit(1)
                .with_for_update(skip_locked=True)
            )
            if not job:
                return None
            attempt_id, token = str(uuid4()), str(uuid4())
            session.add(
                RuntimeAttempt(
                    attempt_id=attempt_id,
                    build_id=job.build_id,
                    attempt_no=job.attempt_no + 1,
                    worker_id=str(worker_id),
                    lease_token=token,
                    lease_expires_at=func.clock_timestamp() + timedelta(seconds=self.lease_seconds),
                )
            )
            # attempt 必须先落库，再关联到构建并标记运行中。
            session.flush()
            job.build_status = RUNNING
            job.attempt_no += 1
            job.current_attempt_id = attempt_id
            if job.started_at is None:
                job.started_at = func.clock_timestamp()
            return LeasedBuild(
                build_id=job.build_id, attempt_id=attempt_id, lease_token=token, project_id=job.project_id,
                repository=job.repository, branch_name=job.branch_name, commit_sha=job.commit_sha,
                input_set_id=job.input_set_id, project_digest=job.project_digest,
                toolchain_version=job.toolchain_version, execution=job.request_json["execution"],
            )

    def _authorized(self, session: Session, build_id: UUID | str, token: UUID | str) -> Build | None:
        return session.scalar(
            select(Build)
            .join(RuntimeAttempt, RuntimeAttempt.attempt_id == Build.current_attempt_id)
            .where(
                Build.build_id == str(build_id),
                Build.build_status == RUNNING,
                RuntimeAttempt.state == EXECUTING,
                RuntimeAttempt.lease_token == str(token),
                RuntimeAttempt.lease_expires_at > func.clock_timestamp(),
            )
            .with_for_update(of=(Build, RuntimeAttempt))
            .execution_options(populate_existing=True)
        )

    def pin_source(self, build_id: str, commit_sha: str, token: str) -> None:
        with self.db.fact_session() as session:
            build = self._authorized(session, build_id, token)
            if build is None:
                raise ValueError("execution lease is no longer valid")
            if build.commit_sha is not None and build.commit_sha != commit_sha:
                raise ValueError("commitSha is immutable after resolution")
            build.commit_sha = commit_sha

    def heartbeat(self, build_id: UUID | str, token: UUID | str) -> bool:
        with self.db.session() as session:
            job = self._authorized(session, build_id, token)
            if not job:
                return False
            session.execute(
                update(RuntimeAttempt)
                .where(RuntimeAttempt.attempt_id == job.current_attempt_id)
                .values(
                    heartbeat_at=func.clock_timestamp(),
                    lease_expires_at=func.clock_timestamp() + timedelta(seconds=self.lease_seconds),
                )
            )
            return True

    def phase(
        self,
        build_id: UUID | str,
        token: UUID | str,
        phase: str,
        *,
        external: bool = False,
        # External engine session identifiers form an adapter-defined map.
        external_execution_refs: JsonObject | None = None,
    ) -> bool:
        with self.db.fact_session() as session:
            job = self._authorized(session, build_id, token)
            if not job:
                return False
            job.phase = strings.RESOLVING_SOURCE if phase == PREPARING else phase
            if external:
                session.execute(
                    update(RuntimeAttempt)
                    .where(RuntimeAttempt.attempt_id == job.current_attempt_id)
                    .values(execution_stage=EXTERNAL, external_execution_refs=external_execution_refs or {})
                )
            return True

    def attach_input(
        self, build_id: UUID | str, token: UUID | str, set_id: str, *, project_digest: str | None = None
    ) -> bool:
        # 首次构建的源码在外部写入前固定，后续准备阶段重试可复用该集合。
        with self.db.fact_session() as session:
            job = self._authorized(session, build_id, token)
            if not job:
                return False
            source = session.get(ArtifactSet, set_id)
            if not source or source.state != SEALED or source.project_id != job.project_id:
                raise ValueError(INVALID_SOURCE_SET_MESSAGE)
            if job.input_set_id is not None and job.input_set_id != str(set_id):
                raise ValueError(IMMUTABLE_JOB_INPUT_MESSAGE)
            job.input_set_id = set_id
            if project_digest is not None:
                job.project_digest = project_digest
            return True

    def finish(
        self,
        build_id: UUID | str,
        token: UUID | str,
        *,
        succeeded: bool = True,
        error_code: str | None = None,
        output_set_id: str | None = None,
        seal: Callable[[str, Session], None] | None = None,
    ) -> bool:
        # 产物封存和终态共用一个事务；过期 worker 无权提交。
        if not succeeded:
            return self.fail(build_id, token, error_code or strings.EXECUTION_FAILED)
        try:
            with self.db.fact_session() as session:
                job = self._authorized(session, build_id, token)
                if not job:
                    return False
                if output_set_id is None:
                    raise ValueError(REQUIRED_BUILD_OUTPUT_MESSAGE)
                output = session.get(ArtifactSet, output_set_id, with_for_update=True, populate_existing=True)
                if not output or output.producer_attempt_id != job.current_attempt_id or output.kind != EXECUTION:
                    raise ValueError(OUTPUT_ATTEMPT_CONFLICT_MESSAGE)
                if output.project_id != job.project_id:
                    raise ValueError(OUTPUT_PROJECT_CONFLICT_MESSAGE)
                # 新构建的输出必须引用同一固定源码、摘要及工具链。
                if (
                    job.input_set_id is None or output.source_set_id != job.input_set_id
                    or job.commit_sha is None
                    or output.source_commit_sha != job.commit_sha
                    or job.project_digest is None
                    or output.project_digest != job.project_digest
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
                if not self._authorized(session, build_id, token):
                    raise _FinishLeaseLost
                session.execute(
                    update(RuntimeAttempt)
                    .where(RuntimeAttempt.attempt_id == job.current_attempt_id)
                    .values(
                        state=SUCCEEDED, stop_confirmed_at=func.clock_timestamp(), finished_at=func.clock_timestamp()
                    )
                )
                job.build_status = SUCCEEDED
                job.phase = "COMPLETE"
                job.output_set_id = output_set_id
                job.finished_at = func.clock_timestamp()
                job.error_code = job.error_detail = None
                return True
        except _FinishLeaseLost:
            return False

    def fail(
        self,
        build_id: UUID | str,
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
            job = self._authorized(session, build_id, token)
            if not job:
                return False
            session.execute(
                update(RuntimeAttempt)
                .where(RuntimeAttempt.attempt_id == job.current_attempt_id)
                .values(
                    state=FAILED if stopped else EXPIRED_UNCONFIRMED,
                    stop_confirmed_at=func.clock_timestamp() if stopped else None,
                    finished_at=func.clock_timestamp(),
                )
            )
            if error_code == strings.CANCELLED:
                job.build_status = strings.CANCELLED
            elif error_code == OUTCOME_UNKNOWN or diagnostic.get(strings.EXTERNAL_OUTCOME_UNKNOWN_ALIAS):
                job.build_status = strings.OUTCOME_UNKNOWN
            else:
                job.build_status = FAILED
            job.error_code = error_code
            job.error_detail = diagnostic
            job.finished_at = func.clock_timestamp()
            return True

    def recover(self) -> int:
        # 恢复只处理过期租约，健康实例的 RUNNING 任务不会被启动流程打断。
        with self.db.fact_session() as session:
            jobs = session.scalars(
                select(Build)
                .where(
                    Build.build_status.in_((QUEUED, RUNNING)),
                    select(RuntimeAttempt.attempt_id)
                    .where(
                        RuntimeAttempt.attempt_id == Build.current_attempt_id,
                        RuntimeAttempt.lease_expires_at <= func.clock_timestamp(),
                    )
                    .exists(),
                )
                .order_by(Build.created_at)
                .with_for_update(of=Build, skip_locked=True)
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
                    job.build_status = QUEUED
                    job.current_attempt_id = None
                    delay = 5 if job.attempt_no <= 1 else 15
                    job.available_at = func.clock_timestamp() + timedelta(seconds=delay)
                    session.flush()
                else:
                    code = OUTCOME_UNKNOWN if external else LEASE_EXPIRED
                    job.build_status = strings.OUTCOME_UNKNOWN if external else FAILED
                    job.error_code = code
                    job.error_detail = JobErrorDetail(external_outcome_unknown=external).model_dump(
                        mode=strings.JSON_MODE, by_alias=True, exclude_unset=True,
                    )
                    job.finished_at = func.clock_timestamp()
            return len(jobs)

    def confirm_stopped(self, attempt_id: UUID | str, token: UUID | str) -> bool:
        # 失效执行者仅可凭自己的 token 确认停止，不能改写公开结果。
        with self.db.fact_session() as session:
            build_id = session.scalar(
                select(RuntimeAttempt.build_id).where(
                    RuntimeAttempt.attempt_id == str(attempt_id), RuntimeAttempt.lease_token == str(token)
                )
            )
            if not build_id:
                return False
            session.execute(
                update(RuntimeAttempt)
                .where(RuntimeAttempt.attempt_id == str(attempt_id), RuntimeAttempt.state == EXPIRED_UNCONFIRMED)
                .values(state=STOPPED, stop_confirmed_at=func.clock_timestamp())
            )
            return True
