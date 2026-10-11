"""查询身份读取及与活动发布一致的原子受理。"""

from __future__ import annotations

from typing import cast
from uuid import UUID

from sqlalchemy import func, select

from dbt_metricflow_service.models.payloads import JsonObject
from dbt_metricflow_service.storage.jobs import JobStore
from dbt_metricflow_service.storage.records import DatabaseRow, StoredJob

from .branches import ACTIVE, PRODUCTION
from .entities import Branch, Release, RuntimeJob, RuntimeProject
from .rows import entity_dict

PUBLISHED = "PUBLISHED"
METRIC_QUERY = "METRIC_QUERY"


class InactiveRelease(ValueError):
    """等待锁期间发布已切换。"""


class QueryStore:
    def __init__(self, jobs: JobStore) -> None:
        self.jobs = jobs
        self.db = jobs.db

    def release_for_run(self, project_id: str, run_id: UUID | str) -> DatabaseRow | None:
        with self.db.session() as session:
            return entity_dict(session.scalar(select(Release).where(
                Release.project_id == project_id, Release.run_id == str(run_id), Release.state == PUBLISHED)))

    def reserve(
        self, project_id: str, release: DatabaseRow, scope: str, key: str, payload: JsonObject, timeout_seconds: int
    ) -> StoredJob:
        # 同键锁先于父 run、项目和发布槽，保持 JobStore 的锁顺序。
        with self.db.session() as session:
            session.execute(select(func.pg_advisory_xact_lock(func.hashtextextended(scope + ":" + key, 0))))
            parent = entity_dict(session.get(RuntimeJob, release["run_id"], with_for_update=True))
            session.execute(select(RuntimeProject.project_id).where(
                RuntimeProject.project_id == project_id).with_for_update())
            slot = entity_dict(session.scalar(select(Branch).where(
                Branch.project_id == project_id, Branch.mode == PRODUCTION).with_for_update()))
            prior = entity_dict(session.scalar(select(RuntimeJob).where(
                RuntimeJob.idempotency_scope == scope, RuntimeJob.idempotency_key == key)))
            if prior:
                return cast(StoredJob, prior)
            if slot["status"] != ACTIVE or slot["active_release_id"] != release["release_id"]:
                raise InactiveRelease("release replaced")
            return cast(StoredJob, self.jobs.reserve_in_transaction(
                session, METRIC_QUERY, project_id, payload,
                idempotency_scope=scope, idempotency_key=key,
                parent_run_id=parent["job_id"], input_set_id=parent["output_set_id"],
                config_version=parent["config_version"], toolchain_version=parent["toolchain_version"],
                profile_binding_id=parent["profile_binding_id"], schema_name=parent["schema_name"],
                timeout_seconds=timeout_seconds,
            ))
