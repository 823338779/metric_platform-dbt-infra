"""在任务完成事务中封存并发布，任务存储无需认识发布服务。"""

from __future__ import annotations

from typing import Unpack
from uuid import UUID

from sqlalchemy import Connection

from dbt_metricflow_service.models.payloads import JsonObject
from dbt_metricflow_service.storage.jobs import JobStore
from dbt_metricflow_service.storage.records import CompletionOptions, StoredJob

from ..storage.publications import PublicationStore


def complete_job(
    jobs: JobStore,
    job_id: UUID | str,
    token: UUID | str,
    payload: JsonObject | None = None,
    **kwargs: Unpack[CompletionOptions],
) -> bool:
    def publish(connection: Connection, job: StoredJob, output_set_id: str) -> None:
        PublicationStore(jobs.db).publish_in_transaction(
            connection, job_id=job_id, attempt_token=token,
            release_id=job["request_json"]["releaseId"], output_set_id=output_set_id,
        )

    return jobs.finish(job_id, token, payload, publish=publish, **kwargs)
