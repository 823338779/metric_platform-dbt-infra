"""查询身份读取及与活动发布一致的原子受理。"""

from __future__ import annotations

from typing import cast
from uuid import UUID

from dbt_metricflow_service.models.payloads import JsonObject
from dbt_metricflow_service.storage.jobs import JobStore
from dbt_metricflow_service.storage.records import DatabaseRow, StoredJob

from .branches import SQL_BRANCH_LOCK, SQL_PARENT_LOCK
from .jobs import SQL_SELECT_FROM_RUNTIME_JOB_2, SQL_SELECT_FROM_RUNTIME_JOB_4
from .rows import row_dict

SQL_RELEASE = "SELECT * FROM runtime_release WHERE project_id=%s AND run_id=%s AND state='PUBLISHED'"
SQL_ALIAS = "SELECT target_id FROM runtime_legacy_identity WHERE project_id=%s AND kind='QUERY' AND legacy_id=%s"


class InactiveRelease(ValueError):
    """等待锁期间发布已切换。"""


class QueryStore:
    def __init__(self, jobs: JobStore) -> None:
        self.jobs = jobs
        self.db = jobs.db

    def release_for_run(self, project_id: str, run_id: UUID | str) -> DatabaseRow | None:
        with self.db.transaction() as connection:
            return row_dict(connection.exec_driver_sql(SQL_RELEASE, (project_id, run_id)))

    def query_id(self, project_id: str, query_id: str) -> tuple[str, bool]:
        with self.db.transaction() as connection:
            alias = row_dict(connection.exec_driver_sql(SQL_ALIAS, (project_id, query_id)))
        return (alias["target_id"], True) if alias else (query_id, False)

    def reserve(
        self, project_id: str, release: DatabaseRow, scope: str, key: str, payload: JsonObject, timeout_seconds: int
    ) -> StoredJob:
        # 同键锁先于父 run、项目和发布槽，保持 JobStore 的锁顺序。
        from .jobs import SQL_SELECT_PG_ADVISORY_XACT_LOCK_HASHTEXTEXTENDED

        with self.db.transaction() as connection:
            connection.exec_driver_sql(SQL_SELECT_PG_ADVISORY_XACT_LOCK_HASHTEXTEXTENDED, (scope + ":" + key,))
            parent = row_dict(connection.exec_driver_sql(SQL_SELECT_FROM_RUNTIME_JOB_4, (release["run_id"],)))
            connection.exec_driver_sql(SQL_PARENT_LOCK, (project_id,))
            slot = row_dict(connection.exec_driver_sql(SQL_BRANCH_LOCK, (project_id, None, None)))
            prior = row_dict(connection.exec_driver_sql(SQL_SELECT_FROM_RUNTIME_JOB_2, (scope, key)))
            if prior:
                return cast(StoredJob, prior)
            if slot["status"] != "ACTIVE" or slot["active_release_id"] != release["release_id"]:
                raise InactiveRelease("release replaced")
            return cast(StoredJob, self.jobs.reserve_in_transaction(
                connection, "METRIC_QUERY", project_id, payload,
                idempotency_scope=scope, idempotency_key=key,
                parent_run_id=parent["job_id"], input_set_id=parent["output_set_id"],
                config_version=parent["config_version"], toolchain_version=parent["toolchain_version"],
                profile_binding_id=parent["profile_binding_id"], schema_name=parent["schema_name"],
                timeout_seconds=timeout_seconds,
            ))
