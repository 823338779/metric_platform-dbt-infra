"""固定构建的具体存储；与现有任务队列共用短事务。"""

from __future__ import annotations

import hashlib
import json
from typing import cast
from uuid import UUID, uuid4

from sqlalchemy import func, or_, select, update

from dbt_metricflow_service.models.builds import BuildRequest
from dbt_metricflow_service.models.payloads import JsonObject
from dbt_metricflow_service.storage.postgres import Database
from dbt_metricflow_service.storage.records import DatabaseRow, StoredBuild

from .entities import Build, Change, ExecutionBinding, RuntimeJob, RuntimeProject
from .jobs import JobStore, StoreConflict
from .rows import entity_dict

JSON_MODE = "json"
BUILD_KIND = "BUILD_RUN"
BUILD_FIELD = "buildId"
SCHEMA_PREFIX = "run_"
PROJECT_PREFIX = "engine:"
QUEUED = "QUEUED"
FAILED = "FAILED"
CANCELLED = "CANCELLED"
JSONB_CONCAT = "||"
BUILD_TABLE_NAME = "engine_build"

# 共享读取表达式保留关联任务的生命周期。
BUILD_VIEW = select(Build, RuntimeJob.run_lifecycle).join(RuntimeJob, RuntimeJob.job_id == Build.run_id)


def digest(value: object) -> str:
    """默认值已由协议模型补齐，摘要不依赖 JSON 键顺序。"""
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


class BuildStore:
    def __init__(self, db: Database) -> None:
        # 只接收具体持久依赖，不拥有 Runtime 或 worker。
        self.db = db
        self.jobs = JobStore(db)

    def register_binding(self, repository: str, binding: str, version: str, config: JsonObject) -> None:
        # 同版本禁止修改语义，凭据仅由已有 profile 环境解析。
        from urllib.parse import urlsplit
        from zoneinfo import ZoneInfo

        from ..models.builds import ConfigSnapshot

        parsed = urlsplit(repository)
        if (
            not repository
            or any(char in repository for char in ("\n", "\r"))
            or parsed.password
            or parsed.query
            or parsed.fragment
            or parsed.scheme in {"http", "https"}
            and parsed.username
        ):
            raise ValueError("repository must not contain credentials or URL parameters")
        config = ConfigSnapshot.model_validate(config).model_dump(mode=JSON_MODE, by_alias=True, exclude_none=True)
        ZoneInfo(config["businessTimezone"])
        with self.db.session() as session:
            session.execute(
                select(func.pg_advisory_xact_lock(func.hashtextextended(repository + binding + version, 0)))
            )
            prior = session.get(ExecutionBinding, (repository, binding, version))
            if prior is not None:
                snapshot = ConfigSnapshot.model_validate(prior.config_json)
                if snapshot.model_dump(mode=JSON_MODE, by_alias=True, exclude_none=True) != config:
                    raise StoreConflict("execution binding version is immutable")
            else:
                session.add(
                    ExecutionBinding(
                        repository=repository,
                        execution_binding=binding,
                        config_version=version,
                        config_json=config,
                    )
                )
                session.flush()

    def get(self, build_id: UUID | str) -> StoredBuild | None:
        with self.db.session() as session:
            row = session.execute(BUILD_VIEW.where(Build.build_id == str(build_id))).one_or_none()
            return cast(StoredBuild, {**entity_dict(row[0]), "run_lifecycle": row[1]}) if row else None

    def accept(self, request: BuildRequest, caller: str, toolchain: str, timeout: int) -> StoredBuild:
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
                    raise StoreConflict("idempotency key already binds another input")
                return cast(StoredBuild, entity_dict(prior))
            binding = session.get(
                ExecutionBinding,
                (
                    request.repository,
                    request.execution_binding,
                    request.config_version,
                ),
            )
            if binding is None or request.environment not in binding.config_json.get("environments", []):
                raise ValueError("repository and execution binding are not allowed")
            config = binding.config_json
            build_id, run_id = str(uuid4()), uuid4()
            # 队列表的 project 列是内部执行容器；每次构建独立，不是公开业务项目。
            project = PROJECT_PREFIX + build_id
            session.add(
                RuntimeProject(
                    project_id=project,
                    binding_config=config,
                    config_version=request.config_version,
                )
            )
            session.flush()
            payload = {**body, BUILD_FIELD: build_id, "binding": config}
            job = self.jobs.reserve_in_transaction(
                session,
                BUILD_KIND,
                project,
                payload,
                job_id=str(run_id),
                config_version=request.config_version,
                toolchain_version=toolchain,
                profile_binding_id=config["profileBindingId"],
                schema_name=config.get("schemaName") or SCHEMA_PREFIX + run_id.hex,
                timeout_seconds=timeout,
            )
            build = Build(
                build_id=build_id,
                run_id=job["job_id"],
                repository=request.repository,
                branch_name=request.branch_name,
                environment=request.environment,
                execution_binding=request.execution_binding,
                config_version=request.config_version,
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
            row = cast(StoredBuild, entity_dict(build))
            if request.deployment_policy == "ON_SUCCESS":
                DeploymentStore(self.db).accept_in_transaction(
                    session,
                    row,
                    caller,
                    "build:" + build_id,
                    fingerprint,
                    cast(str, request.branch_name),
                    operation="BUILD",
                )
            return row

    def logs(self, build_id: str, cursor: str | None) -> tuple[list[DatabaseRow], str]:
        # 游标绑定构建，不依赖实例内存中的日志缓冲区。
        after = 0
        if cursor is not None:
            prefix, separator, sequence = cursor.partition(":")
            if prefix != build_id or not separator or not sequence.isascii() or not sequence.isdecimal():
                raise ValueError("invalid build log cursor")
            after = int(sequence)
            if after > 9223372036854775807:
                raise ValueError("invalid build log cursor")
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
            rows = [dict(row) for row in session.execute(statement).mappings()]
        return rows, build_id + ":" + str(rows[-1]["sequence"] if rows else after)

    def list(self, repository: str) -> list[StoredBuild]:
        with self.db.session() as session:
            statement = BUILD_VIEW.where(Build.repository == repository).order_by(
                Build.created_at.desc(),
                Build.build_id.desc(),
            )
            return cast(
                list[StoredBuild],
                [{**entity_dict(build), "run_lifecycle": lifecycle} for build, lifecycle in session.execute(statement)],
            )

    def pin_source(self, build_id: UUID | str, commit_sha: str, token: UUID | str) -> StoredBuild:
        row = self.get(build_id)
        with self.db.fact_session() as session:
            # 与完成/取消统一先锁执行记录；过期 worker 不能变更固定输入。
            if not row or not self.jobs._authorized(session, row["run_id"], token):
                raise StoreConflict("execution lease is no longer valid")
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
                raise ValueError("commitSha is immutable after resolution")
            session.execute(
                update(RuntimeJob)
                .where(RuntimeJob.job_id == row["run_id"])
                .values(
                    request_json=RuntimeJob.request_json.op(JSONB_CONCAT)({"commitSha": commit_sha}),
                )
            )
            return cast(StoredBuild, entity_dict(result))

    def cancel(self, build_id: UUID | str) -> int:
        row = self.get(build_id)
        if row is None:
            raise KeyError(build_id)
        with self.db.fact_session() as session:
            # 先锁任务行再写取消意图，保持与执行完成路径相同的锁顺序。
            job = session.get(RuntimeJob, row["run_id"], with_for_update=True)
            if job.error_code == CANCELLED:
                return 200
            if job.status not in {"QUEUED", "RUNNING"}:
                raise StoreConflict("terminal build cannot be cancelled")
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
                .where(RuntimeJob.job_id == row["run_id"], RuntimeJob.status == QUEUED)
                .values(
                    status=FAILED,
                    error_code=CANCELLED,
                    finished_at=func.clock_timestamp(),
                )
            )
            return 202
