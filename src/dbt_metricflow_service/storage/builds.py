"""固定构建的具体存储；与现有任务队列共用短事务。"""

from __future__ import annotations

import hashlib
import json
from typing import cast
from uuid import UUID, uuid4

from sqlalchemy import bindparam, func, insert, or_, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from dbt_metricflow_service.models.builds import BuildRequest
from dbt_metricflow_service.models.payloads import JsonObject
from dbt_metricflow_service.storage.postgres import Database
from dbt_metricflow_service.storage.records import DatabaseRow, StoredBuild

from .build_tables import BINDING, BUILD, BUILD_TABLE_NAME, CHANGE, JOB, PROJECT
from .jobs import JobStore, StoreConflict
from .rows import row_dict

JSON_MODE = "json"
BUILD_KIND = "BUILD_RUN"
BUILD_FIELD = "buildId"
SCHEMA_PREFIX = "run_"
PROJECT_PREFIX = "engine:"
QUEUED = "QUEUED"
FAILED = "FAILED"
CANCELLED = "CANCELLED"
JSONB_CONCAT = "||"

# 共享读取表达式保留关联任务的生命周期；配置条件使用具名绑定参数。
BUILD_VIEW = select(BUILD, JOB.c.run_lifecycle).select_from(BUILD.outerjoin(JOB, JOB.c.job_id == BUILD.c.run_id))
BINDING_LOOKUP = select(BINDING.c.config_json).where(
    BINDING.c.repository == bindparam("repository"),
    BINDING.c.execution_binding == bindparam("execution_binding"),
    BINDING.c.config_version == bindparam("config_version"),
)


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
        with self.db.transaction() as connection:
            connection.execute(
                select(func.pg_advisory_xact_lock(func.hashtextextended(repository + binding + version, 0)))
            )
            prior = connection.execute(
                BINDING_LOOKUP,
                {
                    "repository": repository,
                    "execution_binding": binding,
                    "config_version": version,
                },
            ).scalar_one_or_none()
            if (
                prior is not None
                and ConfigSnapshot.model_validate(prior).model_dump(mode=JSON_MODE, by_alias=True, exclude_none=True)
                != config
            ):
                raise StoreConflict("execution binding version is immutable")
            connection.execute(
                pg_insert(BINDING)
                .values(
                    repository=repository,
                    execution_binding=binding,
                    config_version=version,
                    config_json=config,
                )
                .on_conflict_do_nothing()
            )

    def get(self, build_id: UUID | str) -> StoredBuild | None:
        with self.db.transaction() as connection:
            return cast(
                StoredBuild | None, row_dict(connection.execute(BUILD_VIEW.where(BUILD.c.build_id == str(build_id))))
            )

    def accept(self, request: BuildRequest, caller: str, toolchain: str, timeout: int) -> StoredBuild:
        from .deployments import DeploymentStore

        body = request.model_dump(mode=JSON_MODE, by_alias=True)
        fingerprint = digest(body)
        with self.db.fact_transaction() as connection:
            # 查旧请求先于读取配置；Git 不在受理路径内。
            scope = json.dumps([request.repository, caller, request.idempotency_key])
            connection.execute(select(func.pg_advisory_xact_lock(func.hashtextextended(scope, 0))))
            prior = row_dict(
                connection.execute(
                    select(BUILD).where(
                        BUILD.c.repository == request.repository,
                        BUILD.c.caller == caller,
                        BUILD.c.idempotency_key == request.idempotency_key,
                    )
                )
            )
            if prior:
                if prior["request_digest"] != fingerprint:
                    raise StoreConflict("idempotency key already binds another input")
                return cast(StoredBuild, prior)
            config = connection.execute(
                BINDING_LOOKUP,
                {
                    "repository": request.repository,
                    "execution_binding": request.execution_binding,
                    "config_version": request.config_version,
                },
            ).scalar_one_or_none()
            if config is None or request.environment not in config.get("environments", []):
                raise ValueError("repository and execution binding are not allowed")
            build_id, run_id = str(uuid4()), uuid4()
            # 旧队列表的 project 列只是内部执行容器；每次构建独立，不是公开业务项目。
            project = PROJECT_PREFIX + build_id
            connection.execute(
                insert(PROJECT).values(
                    project_id=project,
                    binding_config=config,
                    config_version=request.config_version,
                )
            )
            payload = {**body, BUILD_FIELD: build_id, "binding": config}
            job = self.jobs.reserve_in_transaction(
                connection,
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
            row = row_dict(
                connection.execute(
                    insert(BUILD)
                    .values(
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
                    .returning(BUILD)
                )
            )
            if request.deployment_policy == "ON_SUCCESS":
                DeploymentStore(self.db).accept_in_transaction(
                    connection,
                    cast(StoredBuild, row),
                    caller,
                    "build:" + build_id,
                    fingerprint,
                    cast(str, request.branch_name),
                    operation="BUILD",
                )
            return cast(StoredBuild, row)

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
        with self.db.transaction() as connection:
            statement = (
                select(CHANGE.c.sequence, CHANGE.c.summary)
                .where(
                    CHANGE.c.object_type == BUILD_TABLE_NAME,
                    CHANGE.c.object_id == build_id,
                    CHANGE.c.sequence > after,
                )
                .order_by(CHANGE.c.sequence)
                .limit(100)
            )
            rows = [dict(row) for row in connection.execute(statement).mappings()]
        return rows, build_id + ":" + str(rows[-1]["sequence"] if rows else after)

    def list(self, repository: str) -> list[StoredBuild]:
        with self.db.transaction() as connection:
            statement = BUILD_VIEW.where(BUILD.c.repository == repository).order_by(
                BUILD.c.created_at.desc(),
                BUILD.c.build_id.desc(),
            )
            return cast(list[StoredBuild], [dict(row) for row in connection.execute(statement).mappings()])

    def pin_source(self, build_id: UUID | str, commit_sha: str, token: UUID | str) -> StoredBuild:
        row = self.get(build_id)
        with self.db.fact_transaction() as connection:
            # 与完成/取消统一先锁执行记录；过期 worker 不能变更固定输入。
            if not row or not self.jobs._authorized(connection, cast(str, row["run_id"]), token):
                raise StoreConflict("execution lease is no longer valid")
            # SHA 比较、写入和版本递增仍在同一条条件 UPDATE 内完成。
            result = row_dict(
                connection.execute(
                    update(BUILD)
                    .where(
                        BUILD.c.build_id == str(build_id),
                        or_(BUILD.c.commit_sha.is_(None), BUILD.c.commit_sha == commit_sha),
                    )
                    .values(
                        commit_sha=commit_sha,
                        version=BUILD.c.version + 1,
                        updated_at=func.clock_timestamp(),
                    )
                    .returning(BUILD)
                )
            )
            if not result:
                raise ValueError("commitSha is immutable after resolution")
            connection.execute(
                update(JOB)
                .where(JOB.c.job_id == row["run_id"])
                .values(
                    request_json=JOB.c.request_json.op(JSONB_CONCAT)({"commitSha": commit_sha}),
                )
            )
            return cast(StoredBuild, result)

    def cancel(self, build_id: UUID | str) -> int:
        row = self.get(build_id)
        if row is None:
            raise KeyError(build_id)
        with self.db.fact_transaction() as connection:
            # 先锁任务行再写取消意图，保持与执行完成路径相同的锁顺序。
            job = (
                connection.execute(
                    select(JOB.c.error_code, JOB.c.status)
                    .where(
                        JOB.c.job_id == row["run_id"],
                    )
                    .with_for_update()
                )
                .mappings()
                .one()
            )
            if job["error_code"] == CANCELLED:
                return 200
            if job["status"] not in {"QUEUED", "RUNNING"}:
                raise StoreConflict("terminal build cannot be cancelled")
            connection.execute(
                update(BUILD)
                .where(BUILD.c.build_id == str(build_id))
                .values(
                    cancel_requested=True,
                    version=BUILD.c.version + 1,
                    updated_at=func.clock_timestamp(),
                )
            )
            connection.execute(
                update(JOB)
                .where(JOB.c.job_id == row["run_id"], JOB.c.status == QUEUED)
                .values(
                    status=FAILED,
                    error_code=CANCELLED,
                    finished_at=func.clock_timestamp(),
                )
            )
            return 202
