"""目录必须按显式分支与发布联合定位，旧接口只读生产。"""

from types import SimpleNamespace

import pytest

from dbt_metricflow_service.publications.service import PublicationService, ReleaseGone
from tests.test_branch_publication import SelectedStore, branch_pair
from tests.test_publication_storage import store as store
from tests.test_publication_transaction import prepared


def published_pair(store, tmp_path):
    # 两份空目录仍有不同发布身份，测试真实封存产物读取。
    project, a, b = branch_pair(store)
    jobs, ja, ra, oa, artifacts = prepared(SelectedStore(store.db, a), tmp_path, project)
    jobs.finish(ja["job_id"], ja["lease_token"], output_set_id=oa)
    jobs, jb, rb, ob, _ = prepared(SelectedStore(store.db, b), tmp_path, project)
    jobs.finish(jb["job_id"], jb["lease_token"], output_set_id=ob)
    runtime = SimpleNamespace(db=store.db, jobs=jobs, artifacts=artifacts)
    return (runtime, project, a, b, store.get_release(project, ra["release_id"]),
            store.get_release(project, rb["release_id"]))


def test_cross_branch_release_and_production_default_are_rejected(store, tmp_path):
    runtime, project, a, b, ra, rb = published_pair(store, tmp_path)
    selected = PublicationService(runtime, branch_id=a)
    assert selected.catalog(project, ra["release_id"])["total"] == 0
    with pytest.raises(KeyError):
        selected.catalog(project, rb["release_id"])
    with pytest.raises(KeyError):
        PublicationService(runtime).catalog(project, ra["release_id"])
    assert [item["releaseId"] for item in selected.releases(project)] == [ra["release_id"]]
    publication = PublicationService(runtime, branch_id=b).publication(project)
    assert publication["activePublication"]["releaseId"] == rb["release_id"]


def test_deleted_branch_rejects_new_catalog(store, tmp_path):
    runtime, project, a, _, ra, _ = published_pair(store, tmp_path)
    with store.db.transaction() as connection:
        connection.exec_driver_sql("UPDATE runtime_branch SET status='DELETED' WHERE branch_id=%s", (a,))
    with pytest.raises(ReleaseGone):
        PublicationService(runtime, branch_id=a).catalog(project, ra["release_id"])


def test_release_summary_uses_sealed_validation_evidence(store, tmp_path):
    runtime, project, a, _, ra, _ = published_pair(store, tmp_path)
    result = PublicationService(runtime, branch_id=a).release(project, ra["release_id"])
    assert result["validationSummary"]["phase"] == "PUBLISHED"
    assert all(item["status"] == "PASSED" for item in result["validationSummary"]["checks"])
    assert len(result["validationSummary"]["checks"]) == 3
