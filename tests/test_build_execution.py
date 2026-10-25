"""真实 Git 输入与带租约的构建状态转移。"""

from uuid import uuid4

import pytest

from dbt_metricflow_service.platform.bindings import ProjectBinding, resolve_commit
from dbt_metricflow_service.storage.jobs import JobStore
from tests.test_build_acceptance import CALLER, TOOLCHAIN, request, service
from tests.test_platform_bindings import git
from tests.test_platform_bindings import repository as repository
from tests.test_publication_storage import store as store


def test_nondefault_project_paths_are_complete(repository, tmp_path):
    (repository / "dbt_project.yml").write_text("name: sample\nmodel-paths: [transformations]\n", encoding="utf-8")
    (repository / "transformations").mkdir()
    (repository / "transformations" / "b.sql").write_text("select 2 as value", encoding="utf-8")
    git(repository, "add", ".")
    git(repository, "commit", "-m", "custom model directory")
    directory, _ = resolve_commit(
        ProjectBinding("engine", str(repository), ".", "postgres"),
        git(repository, "rev-parse", "HEAD"),
        tmp_path / "fetch",
    )
    assert (directory / "transformations" / "b.sql").read_text(encoding="utf-8") == "select 2 as value"


def test_pinned_sha_survives_remote_advance(store):
    app = service(store)
    build = app.submit_snapshot(request(commitSha=None), CALLER)
    jobs = JobStore(store.db)
    job = jobs.claim(str(uuid4()), toolchain_version=TOOLCHAIN)
    # 其他测试可能有排队任务，领取直到本次构建。
    while job and job.job_id != app.store.get(build.build_id).run_id:
        jobs.fail(job.job_id, job.lease_token, "TEST_FINISHED")
        job = jobs.claim(str(uuid4()), toolchain_version=TOOLCHAIN)
    assert app.store.pin_source(build.build_id, "a" * 40, job.lease_token).commit_sha == "a" * 40
    # 固定 SHA 只合并对应 JSONB 字段，原始执行请求必须完整保留。
    pinned_request = jobs.get(job.job_id).request_json
    assert pinned_request == job.request_json.model_copy(update={"commit_sha": "a" * 40})
    with pytest.raises(ValueError):
        app.store.pin_source(build.build_id, "b" * 40, job.lease_token)


def test_cancel_queued_is_idempotent(store):
    app = service(store)
    build = app.submit_snapshot(request(), CALLER)
    assert app.cancel_snapshot(build.build_id, CALLER)[0].build_status == "CANCELLED"
    assert app.cancel_snapshot(build.build_id, CALLER)[1] == 200


def test_unknown_write_is_not_reexecuted(store):
    app = service(store)
    build = app.submit_snapshot(request(), CALLER)
    jobs = JobStore(store.db)
    run_id = app.store.get(build.build_id).run_id
    job = jobs.claim(str(uuid4()), toolchain_version=TOOLCHAIN)
    while job and job.job_id != run_id:
        jobs.fail(job.job_id, job.lease_token, "TEST_FINISHED")
        job = jobs.claim(str(uuid4()), toolchain_version=TOOLCHAIN)
    jobs.phase(run_id, job.lease_token, "BUILDING", external=True)
    jobs.fail(run_id, job.lease_token, "EXECUTION_OUTCOME_UNKNOWN", stopped=False)
    jobs.recover()
    assert app.snapshot_view(build.build_id).build_status == "OUTCOME_UNKNOWN"
    assert jobs.get(run_id).attempt_no == 1


def test_cancelled_execution_state_is_available_in_build_detail(store):
    app = service(store)
    build = app.submit_snapshot(request(), CALLER)
    assert app.snapshot_view(build.build_id).build_status == "QUEUED"
    app.cancel_snapshot(build.build_id, CALLER)
    detail = app.snapshot_view(build.build_id)
    assert detail.build_status == "CANCELLED"
    assert detail.error.code == "CANCELLED"
    job = JobStore(store.db).get(app.store.get(build.build_id).run_id)
    assert job.error_code == "CANCELLED"
