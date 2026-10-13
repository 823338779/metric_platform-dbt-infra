"""固定构建的具体存储；与现有任务队列共用短事务。"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Final, cast
from uuid import UUID, uuid4

from pydantic import JsonValue
from sqlalchemy import func, or_, select, tuple_, update

from dbt_metricflow_service import constants as strings
from dbt_metricflow_service.models.execution import SNAPSHOT_TARGET, SnapshotBuildRequest, physical_target_key
from dbt_metricflow_service.storage.postgres import Database
from dbt_metricflow_service.storage.records import BuildLogRecord, JobRequest, StoredBuild

from .entities import Build, Change, RuntimeJob, RuntimeProject
from .jobs import JobStore, StoreConflict
from .paging import HISTORY_CURSOR_VERSION, read_cursor, write_cursor
from .rows import entity_record

CURSOR_TIMESTAMP_OFFSET_MESSAGE: Final = "cursor timestamp requires an offset"
EXPIRED_EXECUTION_LEASE_MESSAGE: Final = "execution lease is no longer valid"
IDEMPOTENCY_INPUT_CONFLICT_MESSAGE: Final = "idempotency key already binds another input"
IMMUTABLE_COMMIT_MESSAGE: Final = "commitSha is immutable after resolution"
INVALID_BUILD_LOG_CURSOR_MESSAGE: Final = "invalid build log cursor"
INVALID_CURSOR_MESSAGE: Final = strings.INVALID_CURSOR_MESSAGE
MISSING_ENVIRONMENT_MESSAGE: Final = "environmentId is required"
TERMINAL_BUILD_MESSAGE: Final = "terminal build cannot be cancelled"

JSON_MODE = strings.JSON_MODE
BUILD_KIND = strings.BUILD_RUN
PROJECT_PREFIX = strings.ENGINE_IDEMPOTENCY_PREFIX
QUEUED = strings.QUEUED
FAILED = strings.FAILED
CANCELLED = strings.CANCELLED
JSONB_CONCAT = "||"
BUILD_TABLE_NAME = strings.ENGINE_BUILD_TABLE

# 共享读取表达式保留关联任务的生命周期。
BUILD_VIEW = select(Build, RuntimeJob.run_lifecycle).join(RuntimeJob, RuntimeJob.job_id == Build.run_id)


def digest(value: JsonValue) -> str:
    """默认值已由协议模型补齐，摘要不依赖 JSON 键顺序。"""
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


class BuildStore:
    def __init__(self, db: Database) -> None:
        # 只接收具体持久依赖，不拥有 Runtime 或 worker。
        self.db = db
        self.jobs = JobStore(db)

    def get(self, build_id: UUID | str) -> StoredBuild | None:
        with self.db.session() as session:
            row = session.execute(BUILD_VIEW.where(Build.build_id == str(build_id))).one_or_none()
            if row is None:
                return None
            result = entity_record(row[0], StoredBuild)
            result.run_lifecycle = row[1]
            return result

    def accept_snapshot(
        self, request: SnapshotBuildRequest, caller: str, toolchain: str, timeout: int, runtime_config_version: str,
    ) -> StoredBuild:
        from .deployments import DeploymentStore

        body = request.model_dump(mode=JSON_MODE, by_alias=True)
        fingerprint = digest(body)
        with self.db.fact_session() as session:
            # 查旧请求先于读取配置；Git 不在受理路径内。
            scope = json.dumps([request.repository, caller, request.idempotency_key])
            session.execute(select(func.pg_advisory_xact_lock(func.hashtextextended(scope, 0))))
            prior = session.scalar(
                select(Build).where(
                    Build.repository == request.repository,
                    Build.caller == caller,
                    Build.idempotency_key == request.idempotency_key,
                )
            )
            if prior:
                if prior.request_digest != fingerprint:
                    raise StoreConflict(IDEMPOTENCY_INPUT_CONFLICT_MESSAGE)
                return entity_record(prior, StoredBuild)
            snapshot = request.execution
            config = snapshot.model_dump(mode=JSON_MODE, by_alias=True)
            environment = str(snapshot.environment_id)
            config_version, schema_name = str(snapshot.config_version), snapshot.schema_name
            profile_target = SNAPSHOT_TARGET
            # 归属由已有构建快照证明；同版本输入不可变，旧协议目标不明确时保守拒绝重叠。
            target_key = physical_target_key(snapshot)
            session.execute(select(func.pg_advisory_xact_lock(func.hashtextextended(target_key, 0))))
            other = session.scalar(select(Build.build_id).where(
                Build.physical_target_key == target_key, Build.environment_id != str(snapshot.environment_id),
            ).limit(1))
            if other:
                raise StoreConflict(strings.EXECUTION_TARGET_CONFLICT)
            version = session.scalar(select(Build).where(
                Build.environment_id == str(snapshot.environment_id), Build.config_version == config_version,
            ).limit(1))
            if version and version.config_snapshot != config:
                raise StoreConflict(strings.EXECUTION_SNAPSHOT_CONFLICT)
            build_id, run_id = str(uuid4()), uuid4()
            # 队列表的 project 列是内部执行容器；每次构建独立，不是公开业务项目。
            project = PROJECT_PREFIX + build_id
            session.add(
                RuntimeProject(
                    project_id=project,
                    binding_config=config,
                    config_version=runtime_config_version,
                )
            )
            session.flush()
            payload = JobRequest(
                repository=request.repository, branch_name=request.branch_name, commit_sha=request.commit_sha,
                execution=request.execution, deployment_policy=request.deployment_policy,
                idempotency_key=request.idempotency_key, origin=request.origin, build_id=build_id, binding=snapshot,
            )
            job = self.jobs.reserve_in_transaction(
                session,
                BUILD_KIND,
                project,
                payload,
                job_id=str(run_id),
                config_version=runtime_config_version,
                toolchain_version=toolchain,
                profile_binding_id=profile_target,
                schema_name=schema_name,
                timeout_seconds=timeout,
            )
            session.get(RuntimeJob, job.job_id).physical_target_key = target_key
            build = Build(
                build_id=build_id,
                run_id=job.job_id,
                repository=request.repository,
                branch_name=request.branch_name,
                environment=environment,
                environment_id=str(snapshot.environment_id),
                physical_target_key=target_key,
                config_version=config_version,
                toolchain_version=toolchain,
                caller=caller,
                idempotency_key=request.idempotency_key,
                request_digest=fingerprint,
                request_json=body,
                config_snapshot=config,
                requested_commit_sha=request.commit_sha,
                commit_sha=request.commit_sha,
            )
            session.add(build)
            session.flush()
            row = entity_record(build, StoredBuild)
            if request.deployment_policy == strings.ON_SUCCESS:
                DeploymentStore(self.db).accept_in_transaction(
                    session,
                    row,
                    caller,
                    strings.BUILD_IDEMPOTENCY_PREFIX + build_id,
                    fingerprint,
                    cast(str, request.branch_name),
                    operation=strings.BUILD,
                )
            return row

    def logs(self, build_id: str, cursor: str | None) -> tuple[list[BuildLogRecord], str]:
        # 游标绑定构建，不依赖实例内存中的日志缓冲区。
        after = 0
        if cursor is not None:
            prefix, separator, sequence = cursor.partition(":")
            if prefix != build_id or not separator or not sequence.isascii() or not sequence.isdecimal():
                raise ValueError(INVALID_BUILD_LOG_CURSOR_MESSAGE)
            after = int(sequence)
            if after > 9223372036854775807:
                raise ValueError(INVALID_BUILD_LOG_CURSOR_MESSAGE)
        with self.db.session() as session:
            statement = (
                select(Change.sequence, Change.summary)
                .where(
                    Change.object_type == BUILD_TABLE_NAME,
                    Change.object_id == build_id,
                    Change.sequence > after,
                )
                .order_by(Change.sequence)
                .limit(100)
            )
            rows = [BuildLogRecord.model_validate(row) for row in session.execute(statement).mappings()]
        return rows, build_id + ":" + str(rows[-1].sequence if rows else after)

    def list(
        self, repository: str | None, branch_name: str | None = None, cursor: str | None = None, limit: int = 50,
        *, environment_id: str | None = None, status: str | None = None,
    ) -> tuple[list[StoredBuild], str | None]:
        if environment_id is None:
            raise ValueError(MISSING_ENVIRONMENT_MESSAGE)
        scope = [environment_id, repository, branch_name, status]
        after = read_cursor(scope, cursor, limit, version=HISTORY_CURSOR_VERSION)
        statement = BUILD_VIEW
        if repository is not None:
            statement = statement.where(Build.repository == repository)
        if environment_id is not None:
            statement = statement.where(Build.environment_id == environment_id)
        if status is not None:
            statement = statement.where(Build.build_status == status)
        if branch_name is not None:
            statement = statement.where(Build.branch_name == branch_name)
        if after is not None:
            try:
                created_at, build_id = datetime.fromisoformat(after[:-36]), str(UUID(after[-36:]))
                if created_at.tzinfo is None:
                    raise ValueError(CURSOR_TIMESTAMP_OFFSET_MESSAGE)
            except ValueError as error:
                raise ValueError(INVALID_CURSOR_MESSAGE) from error
            statement = statement.where(tuple_(Build.created_at, Build.build_id) < tuple_(created_at, build_id))
        with self.db.session() as session:
            statement = statement.order_by(Build.created_at.desc(), Build.build_id.desc()).limit(limit + 1)
            rows = []
            for build, lifecycle in session.execute(statement):
                record = entity_record(build, StoredBuild)
                record.run_lifecycle = lifecycle
                rows.append(record)
        selected = rows[:limit]
        next_cursor = (
            write_cursor(
                scope, str(selected[-1].created_at) + selected[-1].build_id, version=HISTORY_CURSOR_VERSION,
            )
            if len(rows) > limit else None
        )
        return selected, next_cursor

    def pin_source(self, build_id: UUID | str, commit_sha: str, token: UUID | str) -> StoredBuild:
        row = self.get(build_id)
        with self.db.fact_session() as session:
            # 与完成/取消统一先锁执行记录；过期 worker 不能变更固定输入。
            if not row or not self.jobs._authorized(session, row.run_id, token):
                raise StoreConflict(EXPIRED_EXECUTION_LEASE_MESSAGE)
            # SHA 比较、写入和版本递增仍在同一条条件 UPDATE 内完成。
            result = session.scalar(
                update(Build)
                .where(
                    Build.build_id == str(build_id),
                    or_(Build.commit_sha.is_(None), Build.commit_sha == commit_sha),
                )
                .values(commit_sha=commit_sha, version=Build.version + 1, updated_at=func.clock_timestamp())
                .returning(Build)
            )
            if not result:
                raise ValueError(IMMUTABLE_COMMIT_MESSAGE)
            session.execute(
                update(RuntimeJob)
                .where(RuntimeJob.job_id == row.run_id)
                .values(
                    request_json=RuntimeJob.request_json.op(JSONB_CONCAT)(JobRequest(commit_sha=commit_sha).model_dump(
                        mode=strings.JSON_MODE, by_alias=True, exclude_unset=True,
                    )),
                )
            )
            return entity_record(result, StoredBuild)

    def cancel(self, build_id: UUID | str) -> int:
        row = self.get(build_id)
        if row is None:
            raise KeyError(build_id)
        with self.db.fact_session() as session:
            # 先锁任务行再写取消意图，保持与执行完成路径相同的锁顺序。
            job = session.get(RuntimeJob, row.run_id, with_for_update=True)
            if job.error_code == CANCELLED:
                return 200
            if job.status not in {strings.QUEUED, strings.RUNNING}:
                raise StoreConflict(TERMINAL_BUILD_MESSAGE)
            session.execute(
                update(Build)
                .where(Build.build_id == str(build_id))
                .values(
                    cancel_requested=True,
                    version=Build.version + 1,
                    updated_at=func.clock_timestamp(),
                )
            )
            session.execute(
                update(RuntimeJob)
                .where(RuntimeJob.job_id == row.run_id, RuntimeJob.status == QUEUED)
                .values(
                    status=FAILED,
                    error_code=CANCELLED,
                    finished_at=func.clock_timestamp(),
                )
            )
            return 202
