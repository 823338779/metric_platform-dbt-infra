"""构建先受理，再由 worker 固定源码；配置版本不可变。"""
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest

from dbt_metricflow_service.application.builds import BuildService
from dbt_metricflow_service.application.errors import ServiceError
from dbt_metricflow_service.execution.build import BuildExecutor
from dbt_metricflow_service.settings import Settings
from dbt_metricflow_service.storage.artifacts import ArtifactStore
from dbt_metricflow_service.storage.builds import BuildStore
from dbt_metricflow_service.storage.jobs import JobStore
from tests.test_build_acceptance import request
from tests.test_platform_bindings import digest, git
from tests.test_platform_bindings import repository as repository
from tests.test_publication_storage import store as store


def setup(store, repository, monkeypatch):
    builds = BuildStore(store.db)
    app = BuildService(builds, uuid4().hex)
    body = request(
        repository=f"https://fixture.invalid/{uuid4().hex}.git", commitSha=git(repository, "rev-parse", "HEAD"),
    )
    monkeypatch.setenv("GIT_CONFIG_COUNT", "1")
    monkeypatch.setenv("GIT_CONFIG_KEY_0", f"url.{repository.as_posix()}.insteadOf")
    monkeypatch.setenv("GIT_CONFIG_VALUE_0", body.repository)
    return app, body


async def test_submit_fixes_input_and_reserves_run_atomically(store, repository, tmp_path, monkeypatch):
    app, body = setup(store, repository, monkeypatch)
    first = app.submit_snapshot(body, "test")
    assert app.submit_snapshot(body, "test").build_id == first.build_id
    jobs = JobStore(store.db)
    job = jobs.claim(uuid4().hex, toolchain_version=app.toolchain)
    assert job.status == "RUNNING" and job.input_set_id is None
    artifacts = ArtifactStore(store.db)
    settings = Settings(tmp_path, 1024, temp_root=tmp_path / "runtime")
    fixed = await BuildExecutor(settings, jobs, artifacts)._prepare_build_source(job)
    assert fixed.request_json.project_digest == digest(repository, body.commit_sha)
    assert fixed.request_json.build_id == str(first.build_id)
    assert artifacts.read_file(fixed.input_set_id, "models/a.sql") == b"select 1 as value\n"
    (repository / "models/a.sql").write_text("select 2 as value\n", encoding="utf-8")
    git(repository, "commit", "-am", "moved remote")
    with pytest.raises(ServiceError):
        app.submit_snapshot(body.model_copy(update={"commit_sha": git(repository, "rev-parse", "HEAD")}), "test")
    assert app.submit_snapshot(body, "test").build_id == first.build_id
    assert jobs.get(job.job_id).input_set_id == fixed.input_set_id


def test_concurrent_fixed_commit_requests_create_one_run(store, repository, tmp_path, monkeypatch):
    app, body = setup(store, repository, monkeypatch)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: app.submit_snapshot(body, "test"), range(2)))
    assert len({row.build_id for row in results}) == 1
    assert len(app.list_by_environment(body.execution.environment_id, body.repository, None, None, None, 50).items) == 1


def test_binding_change_during_snapshot_rejects_candidate(store, repository, tmp_path, monkeypatch):
    app, body = setup(store, repository, monkeypatch)
    first = app.submit_snapshot(body, "test")
    # 新协议从配置版本入口拒绝修改，无需等到慢 Git 读取结束才发现漂移。
    changed = body.model_copy(update={"idempotency_key": uuid4().hex,
        "execution": body.execution.model_copy(update={"threads": 3})})
    with pytest.raises(ServiceError) as conflict:
        app.submit_snapshot(changed, "test")
    assert conflict.value.error.code == "EXECUTION_SNAPSHOT_CONFLICT"
    assert app.submit_snapshot(body, "test").build_id == first.build_id
    assert app.store.get(first.build_id).config_snapshot == body.execution
