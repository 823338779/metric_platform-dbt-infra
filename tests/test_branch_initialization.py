"""重复初始化不会改变已发布分支、任务身份和产物内容。"""

from dbt_metricflow_service.runtime.completion import complete_job
from dbt_metricflow_service.storage.branches import BranchStore
from dbt_metricflow_service.storage.jobs import JobStore
from dbt_metricflow_service.storage.publications import PublicationStore
from tests.schema_helpers import isolated_database
from tests.test_publication_storage import store as store
from tests.test_publication_transaction import prepared


def test_repeat_initialization_preserves_production_identities_and_bytes(tmp_path):
    with isolated_database() as db:
        db.initialize()
        publications = PublicationStore(db)
        jobs, job, release, output, _ = prepared(publications, tmp_path)
        assert complete_job(jobs, job["job_id"], job["lease_token"], output_set_id=output)
        query = jobs.reserve("METRIC_QUERY", job["project_id"], {}, parent_run_id=job["job_id"])
        branches = BranchStore(db)
        branch = branches.production(job["project_id"])
        assert branch["active_release_id"] == branch["latest_release_id"] == release["release_id"]
        assert branch["publication_sequence"] == 1
        with db.transaction() as connection:
            before_release = dict(connection.exec_driver_sql("SELECT * FROM runtime_release").mappings().one())
            before_jobs = [dict(row) for row in connection.exec_driver_sql(
                "SELECT * FROM runtime_job ORDER BY job_id").mappings()]
            before_files = [dict(row) for row in connection.exec_driver_sql(
                "SELECT * FROM runtime_artifact_file ORDER BY set_id,relative_path").mappings()]
        assert {row["job_id"] for row in before_jobs} == {job["job_id"], query["job_id"]}
        assert before_files
        db.initialize()
        db.initialize()
        db.check()
        assert branches.list(job["project_id"]) == [branch]
        with db.transaction() as connection:
            assert dict(connection.exec_driver_sql("SELECT * FROM runtime_release").mappings().one()) == before_release
            assert [dict(row) for row in connection.exec_driver_sql(
                "SELECT * FROM runtime_job ORDER BY job_id").mappings()] == before_jobs
            assert [dict(row) for row in connection.exec_driver_sql(
                "SELECT * FROM runtime_artifact_file ORDER BY set_id,relative_path").mappings()] == before_files


def test_project_reads_production_pointer(store, tmp_path):
    # 项目读取必须从 main 派生，避免调用方看到过期指针。
    jobs, job, release, output, _ = prepared(store, tmp_path)
    assert complete_job(jobs, job["job_id"], job["lease_token"], output_set_id=output)
    project = JobStore(store.db).project(job["project_id"])
    assert project["active_published_release_id"] == release["release_id"]
    assert project["publication_sequence"] == 1
