"""构建先受理，再由 worker 固定源码；配置版本不可变。"""
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest

from dbt_metricflow_service.application.builds import BuildService
from dbt_metricflow_service.application.errors import ServiceError
from dbt_metricflow_service.runtime.executor import RuntimeExecutor
from dbt_metricflow_service.settings import Settings
from dbt_metricflow_service.storage.artifacts import ArtifactStore
from dbt_metricflow_service.storage.builds import BuildStore
from dbt_metricflow_service.storage.jobs import JobStore, StoreConflict
from tests.test_platform_bindings import digest, git
from tests.test_platform_bindings import repository as repository
from tests.test_publication_storage import store as store
from tests.test_v3_build_acceptance import request


def setup(store, repository):
    builds = BuildStore(store.db)
    builds.register_binding(str(repository), "warehouse", "1",
        {"environments": ["PREVIEW"], "profileBindingId": "postgres"})
    app = BuildService(builds, uuid4().hex, 60)
    body = request(repository=str(repository), commitSha=git(repository, "rev-parse", "HEAD"))
    return app, body


async def test_submit_fixes_input_and_reserves_run_atomically(store, repository, tmp_path):
    app, body = setup(store, repository)
    first = app.submit(body, "test")
    assert app.submit(body, "test").build_id == first.build_id
    jobs = JobStore(store.db)
    job = jobs.claim(uuid4().hex, toolchain_version=app.toolchain)
    assert job["status"] == "RUNNING" and job["input_set_id"] is None
    artifacts = ArtifactStore(store.db)
    settings = Settings(tmp_path, 60, 1024, temp_root=tmp_path / "runtime")
    fixed = await RuntimeExecutor(settings, jobs, artifacts)._prepare_build_source(job)
    assert fixed["request_json"]["projectDigest"] == digest(repository, body.commit_sha)
    assert fixed["request_json"]["buildId"] == str(first.build_id)
    assert artifacts.read_file(fixed["input_set_id"], "models/a.sql") == b"select 1 as value\n"
    (repository / "models/a.sql").write_text("select 2 as value\n", encoding="utf-8")
    git(repository, "commit", "-am", "moved remote")
    with pytest.raises(ServiceError):
        app.submit(body.model_copy(update={"commit_sha": git(repository, "rev-parse", "HEAD")}), "test")
    assert app.submit(body, "test").build_id == first.build_id
    assert jobs.get(job["job_id"])["input_set_id"] == fixed["input_set_id"]


def test_concurrent_fixed_commit_requests_create_one_run(store, repository, tmp_path):
    app, body = setup(store, repository)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: app.submit(body, "test"), range(2)))
    assert len({row.build_id for row in results}) == 1
    assert len(app.list(str(repository)).items) == 1


def test_binding_change_during_snapshot_rejects_candidate(store, repository, tmp_path):
    app, body = setup(store, repository)
    first = app.submit(body, "test")
    # 新协议从配置版本入口拒绝修改，无需等到慢 Git 读取结束才发现漂移。
    with pytest.raises(StoreConflict):
        app.store.register_binding(str(repository), "warehouse", "1",
            {"environments": ["PREVIEW"], "profileBindingId": "other"})
    assert app.submit(body, "test").build_id == first.build_id
    assert app.store.get(first.build_id)["config_snapshot"]["profileBindingId"] == "postgres"
