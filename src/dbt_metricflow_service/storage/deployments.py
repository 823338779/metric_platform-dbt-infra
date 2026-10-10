"""部署自然键与接收顺序；活动指针不属于构建终态。"""

from __future__ import annotations

from typing import cast
from uuid import UUID

from sqlalchemy import Connection, func, select

from dbt_metricflow_service.models.deployments import DeploymentAttemptView, DeploymentKey, DeploymentRequest
from dbt_metricflow_service.storage.postgres import Database
from dbt_metricflow_service.storage.records import StoredBuild, StoredDeploymentAttempt, StoredDeploymentTarget

from .jobs import StoreConflict
from .rows import row_dict

SQL_ENSURE = (
    "INSERT INTO engine_deployment_target(repository,environment,branch_name) VALUES(%s,%s,%s) ON CONFLICT DO NOTHING"
)
SQL_TARGET = "SELECT * FROM engine_deployment_target WHERE repository=%s AND environment=%s AND branch_name=%s"
SQL_LOCK_TARGET = SQL_TARGET + " FOR UPDATE"
SQL_ADVANCE = """UPDATE engine_deployment_target SET desired_generation=desired_generation+1,version=version+1
 WHERE repository=%s AND environment=%s AND branch_name=%s RETURNING *"""
SQL_STALE = """UPDATE engine_deployment_attempt SET deployment_status='STALE',reason='NEWER_INTENT',version=version+1
 WHERE repository=%s AND environment=%s AND branch_name=%s
 AND deployment_status IN ('WAITING_FOR_BUILD','PENDING','CHECKING')"""
SQL_INSERT = """INSERT INTO engine_deployment_attempt(repository,environment,branch_name,generation,build_id,
 caller,idempotency_key,request_digest,deployment_status,operation) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *"""
SQL_INITIAL = "SELECT * FROM engine_deployment_attempt WHERE build_id=%s AND operation='BUILD' AND idempotency_key=%s"
SQL_ATTEMPT = """SELECT * FROM engine_deployment_attempt WHERE repository=%s AND environment=%s AND branch_name=%s
 AND generation=%s"""
SQL_ATTEMPT_KEY = """SELECT * FROM engine_deployment_attempt WHERE repository=%s AND caller=%s
 AND operation='DEPLOYMENT' AND idempotency_key=%s"""
SQL_FINISH = """UPDATE engine_deployment_attempt SET deployment_status=%s,reason=%s,version=version+1,
 deployed_at=CASE WHEN %s='DEPLOYED' THEN clock_timestamp() ELSE deployed_at END
 WHERE repository=%s AND environment=%s AND branch_name=%s AND generation=%s RETURNING *"""
SQL_SWITCH = """UPDATE engine_deployment_target SET active_build_id=%s,version=version+1,source_state='MATCHED'
 WHERE repository=%s AND environment=%s AND branch_name=%s"""
SQL_OBSERVE = """UPDATE engine_deployment_target SET observed_head_sha=%s,head_observed_at=clock_timestamp(),
 source_state=%s,active_build_id=CASE WHEN %s THEN NULL ELSE active_build_id END,
 version=version+CASE WHEN %s AND active_build_id IS NOT NULL THEN 1 ELSE 0 END
 WHERE repository=%s AND environment=%s AND branch_name=%s RETURNING *"""
SQL_PENDING = """WITH candidates AS (
 SELECT repository,environment,branch_name,generation FROM engine_deployment_attempt WHERE deployment_status IN
 ('WAITING_FOR_BUILD','PENDING','CHECKING')
 ORDER BY last_checked_at NULLS FIRST,created_at LIMIT 100 FOR UPDATE SKIP LOCKED
 ) UPDATE engine_deployment_attempt a SET last_checked_at=clock_timestamp() FROM candidates c
 WHERE (a.repository,a.environment,a.branch_name,a.generation)=(c.repository,c.environment,c.branch_name,c.generation)
 RETURNING a.*"""
SQL_LIST = "SELECT * FROM engine_deployment_attempt WHERE repository=%s ORDER BY created_at DESC,generation DESC"
SQL_BUILD_LOCK = """SELECT b.*,j.run_lifecycle FROM engine_build b JOIN runtime_job j ON b.run_id=j.job_id
 WHERE b.build_id=%s FOR UPDATE OF j,b"""
TERMINAL = frozenset({"DEPLOYED", "STALE", "FAILED", "CANCELLED"})


class DeploymentStore:
    def __init__(self, db: Database) -> None:
        # 与构建队列共用连接池；不独立执行外部 Git 操作。
        self.db = db

    def accept_in_transaction(
        self,
        connection: Connection,
        build: StoredBuild,
        caller: str,
        key: str,
        fingerprint: str,
        branch: str,
        operation: str = "DEPLOYMENT",
    ) -> StoredDeploymentAttempt:
        natural_key = (build["repository"], build["environment"], branch)
        connection.exec_driver_sql(SQL_ENSURE, natural_key)
        connection.exec_driver_sql(SQL_LOCK_TARGET, natural_key)
        connection.exec_driver_sql(SQL_STALE, natural_key)
        target = cast(StoredDeploymentTarget, row_dict(connection.exec_driver_sql(SQL_ADVANCE, natural_key)))
        status = "PENDING" if build["build_status"] == "SUCCEEDED" else "WAITING_FOR_BUILD"
        return cast(StoredDeploymentAttempt, row_dict(
            connection.exec_driver_sql(
                SQL_INSERT,
                (
                    *natural_key,
                    target["desired_generation"],
                    build["build_id"],
                    caller,
                    key,
                    fingerprint,
                    status,
                    operation,
                ),
            )
        ))

    def initial(self, build_id: UUID | str) -> StoredDeploymentAttempt | None:
        with self.db.transaction() as connection:
            return cast(
                StoredDeploymentAttempt | None,
                row_dict(connection.exec_driver_sql(SQL_INITIAL, (str(build_id), "build:" + str(build_id)))),
            )

    def current(self, key: DeploymentKey) -> StoredDeploymentTarget | None:
        with self.db.transaction() as connection:
            return cast(
                StoredDeploymentTarget | None,
                row_dict(connection.exec_driver_sql(SQL_TARGET, (key.repository, key.environment, key.branch_name))),
            )

    def attempt(self, key: DeploymentKey, generation: int) -> StoredDeploymentAttempt | None:
        with self.db.transaction() as connection:
            return cast(
                StoredDeploymentAttempt | None,
                row_dict(connection.exec_driver_sql(SQL_ATTEMPT, (*natural(key), generation))),
            )

    def by_key(self, repository: str, caller: str, key: str) -> StoredDeploymentAttempt | None:
        with self.db.transaction() as connection:
            return cast(
                StoredDeploymentAttempt | None,
                row_dict(connection.exec_driver_sql(SQL_ATTEMPT_KEY, (repository, caller, key))),
            )

    def submit(
        self, build: StoredBuild, caller: str, request: DeploymentRequest, fingerprint: str
    ) -> StoredDeploymentAttempt:
        with self.db.fact_transaction() as connection:
            # 使用同一 advisory lock 键序列化部署幂等受理，不依赖构建层的 SQL 常量。
            scope = build["repository"] + caller + request.idempotency_key
            connection.execute(select(func.pg_advisory_xact_lock(func.hashtextextended(scope, 0))))
            prior = row_dict(
                connection.exec_driver_sql(SQL_ATTEMPT_KEY, (build["repository"], caller, request.idempotency_key))
            )
            if prior:
                if prior["request_digest"] != fingerprint:
                    raise StoreConflict("deployment key already binds another input")
                return cast(StoredDeploymentAttempt, prior)
            key = (build["repository"], build["environment"], request.branch_name)
            connection.exec_driver_sql(SQL_ENSURE, key)
            current = cast(StoredDeploymentTarget, row_dict(connection.exec_driver_sql(SQL_LOCK_TARGET, key)))
            if current["version"] != request.expected_target_version:
                raise StoreConflict("deployment target version changed")
            return cast(StoredDeploymentAttempt, self.accept_in_transaction(
                connection, build, caller, request.idempotency_key, fingerprint, request.branch_name
            ))

    def observe(
        self,
        key: DeploymentKey,
        head: str | None,
        state: str,
        *,
        expected_version: int,
        expected_active_build_id: str | None,
    ) -> StoredDeploymentTarget | None:
        with self.db.fact_transaction() as connection:
            target = row_dict(connection.exec_driver_sql(SQL_LOCK_TARGET, natural(key)))
            if (
                not target
                or target["version"] != expected_version
                or target["active_build_id"] != expected_active_build_id
            ):
                return cast(StoredDeploymentTarget | None, None)
            missing = state == "MISSING"
            target = row_dict(connection.exec_driver_sql(SQL_OBSERVE, (head, state, missing, missing, *natural(key))))
            if missing:
                connection.exec_driver_sql(SQL_STALE, natural(key))
            return cast(StoredDeploymentTarget | None, target)

    def settle(
        self,
        key: DeploymentKey,
        generation: int,
        status: str,
        reason: str | None = None,
        *,
        expected_version: int | None = None,
        head: str | None = None,
    ) -> StoredDeploymentAttempt | None:
        with self.db.fact_transaction() as connection:
            target = row_dict(connection.exec_driver_sql(SQL_LOCK_TARGET, natural(key)))
            attempt = row_dict(connection.exec_driver_sql(SQL_ATTEMPT, (*natural(key), generation)))
            if not attempt or attempt["deployment_status"] in TERMINAL:
                return cast(StoredDeploymentAttempt | None, attempt)
            if cast(StoredDeploymentTarget, target)["desired_generation"] != generation:
                status, reason = "STALE", "NEWER_INTENT"
            if status == "DEPLOYED":
                build = cast(StoredBuild, row_dict(connection.exec_driver_sql(SQL_BUILD_LOCK, (attempt["build_id"],))))
                # 与人工清理锁同一执行记录；head 只比较事务前已完成的观察值。
                if (
                    cast(StoredDeploymentTarget, target)["version"] != expected_version
                    or build["build_status"] != "SUCCEEDED"
                    or build.get("run_lifecycle") != "ACTIVE"
                    or build["source_incomplete"]
                    or not build["output_set_id"]
                    or build["commit_sha"] != head
                ):
                    status, reason = "STALE", "TARGET_OR_BUILD_CHANGED"
                else:
                    connection.exec_driver_sql(SQL_SWITCH, (build["build_id"], *natural(key)))
            return cast(
                StoredDeploymentAttempt | None,
                row_dict(connection.exec_driver_sql(SQL_FINISH, (status, reason, status, *natural(key), generation))),
            )

    def pending(self) -> list[StoredDeploymentAttempt]:
        with self.db.transaction() as connection:
            return cast(
                list[StoredDeploymentAttempt], [dict(row) for row in connection.exec_driver_sql(SQL_PENDING).mappings()]
            )

    def list(self, repository: str) -> list[StoredDeploymentAttempt]:
        with self.db.transaction() as connection:
            return cast(
                list[StoredDeploymentAttempt],
                [dict(row) for row in connection.exec_driver_sql(SQL_LIST, (repository,)).mappings()],
            )


def natural(key: DeploymentKey) -> tuple[str, str, str]:
    return key.repository, key.environment, key.branch_name


def attempt_view(row: StoredDeploymentAttempt) -> DeploymentAttemptView:
    # 显式白名单防止内部幂等来源和请求摘要泄漏到 API。
    from ..models.deployments import DeploymentAttemptView

    return DeploymentAttemptView(**{key: row[key] for key in DeploymentAttemptView.model_fields})
