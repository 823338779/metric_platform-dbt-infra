"""分支指针、候选和已受理 run 的发布事务隔离。"""

from uuid import uuid4

import pytest

from dbt_metricflow_service.runtime.completion import complete_job
from dbt_metricflow_service.storage.branches import BranchStore
from dbt_metricflow_service.storage.jobs import CleanupBlocked, JobStore
from dbt_metricflow_service.storage.publications import PublicationStore
from tests.test_branch_storage import REQUEST, preview
from tests.test_publication_storage import store as store
from tests.test_publication_transaction import prepared

PROJECT_PREFIX = "branch-publication-"
SQL_DELETE = "UPDATE runtime_branch SET status='DELETED' WHERE branch_id=%s"
SQL_EXPIRE = "UPDATE runtime_attempt SET lease_expires_at=now()-interval '1 second' WHERE attempt_id=%s"
PUBLISHED = "PUBLISHED"
SUPERSEDED = "SUPERSEDED"
FAILED = "FAILED"


class SelectedStore(PublicationStore):
    """测试 helper 只给既有 prepared fixture 增加分支选择。"""

    def __init__(self, db, branch_id):
        super().__init__(db)
        self.branch_id = branch_id

    def create_candidate(self, project_id, request, key):
        return super().create_candidate(project_id, request, key, branch_id=self.branch_id)


def branch_pair(store):
    # 每个测试独立项目和两个分支，不共享活动指针。
    project = PROJECT_PREFIX + uuid4().hex
    JobStore(store.db).register_project(project)
    return project, preview(store, project), preview(store, project)


def test_other_branch_does_not_supersede_candidate(store, tmp_path):
    project, a, b = branch_pair(store)
    jobs, job, release, output, _ = prepared(SelectedStore(store.db, a), tmp_path, project)
    store.create_candidate(project, REQUEST, uuid4().hex, branch_id=b)
    assert complete_job(jobs, job["job_id"], job["lease_token"], output_set_id=output)
    assert store.get_release(project, release["release_id"])["state"] == PUBLISHED
    assert store.get_publication(project, branch_id=b)["activePublication"] is None
    assert store.get_publication(project)["activePublication"] is None


def test_late_candidate_cannot_replace_newer_input(store, tmp_path):
    project, a, _ = branch_pair(store)
    jobs, job, release, output, _ = prepared(SelectedStore(store.db, a), tmp_path, project)
    store.create_candidate(project, REQUEST, uuid4().hex, branch_id=a)
    assert complete_job(jobs, job["job_id"], job["lease_token"], output_set_id=output)
    assert store.get_release(project, release["release_id"])["state"] == SUPERSEDED


def test_failed_latest_keeps_branch_active_release(store, tmp_path):
    project, a, _ = branch_pair(store)
    selected = SelectedStore(store.db, a)
    jobs, job, release, output, _ = prepared(selected, tmp_path, project)
    assert complete_job(jobs, job["job_id"], job["lease_token"], output_set_id=output)
    jobs, next_job, _, _, _ = prepared(selected, tmp_path, project)
    jobs.fail(next_job["job_id"], next_job["lease_token"], FAILED)
    assert BranchStore(store.db).get(project, a)["active_release_id"] == release["release_id"]


def test_deleted_branch_or_expired_attempt_cannot_publish(store, tmp_path):
    project, a, b = branch_pair(store)
    jobs, job, release, output, _ = prepared(SelectedStore(store.db, a), tmp_path, project)
    with store.db.transaction() as connection:
        connection.exec_driver_sql(SQL_DELETE, (a,))
    assert complete_job(jobs, job["job_id"], job["lease_token"], output_set_id=output)
    assert store.get_release(project, release["release_id"])["state"] == SUPERSEDED
    jobs, job, _, output, _ = prepared(SelectedStore(store.db, b), tmp_path, project)
    with store.db.transaction() as connection:
        connection.exec_driver_sql(SQL_EXPIRE, (job["attempt_id"],))
    assert not complete_job(jobs, job["job_id"], job["lease_token"], output_set_id=output)
    assert store.get_publication(project, branch_id=b)["activePublication"] is None


def test_deleted_branch_keeps_published_run_protected(store, tmp_path):
    # 删除逻辑分支不释放其发布引用，不能通过 cleanup 删除已受理查询的数据。
    project, a, _ = branch_pair(store)
    jobs, job, _, output, _ = prepared(SelectedStore(store.db, a), tmp_path, project)
    assert complete_job(jobs, job["job_id"], job["lease_token"], output_set_id=output)
    with store.db.transaction() as connection:
        connection.exec_driver_sql(SQL_DELETE, (a,))
    with pytest.raises(CleanupBlocked):
        jobs.reserve_cleanup(job["job_id"])
