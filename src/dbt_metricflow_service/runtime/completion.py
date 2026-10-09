"""在任务完成事务中封存并发布，任务存储无需认识发布服务。"""

from ..storage.publications import PublicationStore


def complete_job(jobs, job_id, token, payload=None, **kwargs):
    def publish(connection, job, output_set_id):
        PublicationStore(jobs.db).publish_in_transaction(
            connection, job_id=job_id, attempt_token=token,
            release_id=job["request_json"]["releaseId"], output_set_id=output_set_id,
        )

    return jobs.finish(job_id, token, payload, publish=publish, **kwargs)
