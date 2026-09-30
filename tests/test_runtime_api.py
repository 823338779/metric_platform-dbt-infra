import os
from pathlib import Path
from uuid import uuid4

import pytest

from dbt_metricflow_service.settings import Settings


def test_postgres_settings_keep_credentials_private_and_validate_lease(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("SERVICE_DATABASE_URL", "postgresql://fixture:private@example.invalid/service")
    monkeypatch.setenv("SERVICE_TEMP_ROOT", str(tmp_path))
    monkeypatch.setenv("WORKER_CONCURRENCY", "3")
    settings = Settings.from_environment()
    assert getattr(settings, "database_url", None) is not None
    assert "private" not in repr(settings)
    assert settings.temp_root == tmp_path
    assert settings.worker_concurrency == 3
    monkeypatch.setenv("JOB_LEASE_SECONDS", "10")
    monkeypatch.setenv("JOB_HEARTBEAT_SECONDS", "15")
    with pytest.raises(ValueError):
        Settings.from_environment()


@pytest.fixture
def runtime_pair(tmp_path):
    dsn = os.getenv("SERVICE_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("需要独立 SERVICE_TEST_DATABASE_URL 测试库")
    from dbt_metricflow_service.runtime import Runtime
    from dbt_metricflow_service.storage.postgres import Database
    db = Database(dsn)
    db.migrate()
    db.close()
    toolchain = uuid4().hex
    runtimes = [Runtime(Settings(
        projects_root=tmp_path / "unused", profiles_dir=tmp_path / "profiles",
        command_timeout_seconds=30, max_output_bytes=1024,
        database_url=dsn, temp_root=tmp_path / str(index), toolchain_version=toolchain,
    )) for index in range(2)]
    project = "api_" + uuid4().hex
    source = tmp_path / "source"
    source.mkdir()
    (source / "dbt_project.yml").write_text("name: fixture\nversion: '1.0'\n", encoding="utf-8")
    runtimes[0].jobs.register_project(project)
    source_id = runtimes[0].artifacts.capture(project, source)
    runtimes[0].jobs.register_project(project, source_set_id=source_id)
    yield runtimes, project
    for runtime in runtimes:
        runtime.db.close()


# 平台构建接口必须延续 PREPARING 状态契约，不能透出存储层的 QUEUED。
def test_queued_build_reports_preparing_state(runtime_pair):
    runtimes, project = runtime_pair
    row = runtimes[0].jobs.reserve(
        "BUILD_RUN", project, {"projectId": project},
        idempotency_scope="BUILD_RUN", idempotency_key=str(uuid4()),
    )

    snapshot = runtimes[1].get_run(row["job_id"])

    assert snapshot["state"] == "PREPARING"


async def test_cli_acceptance_and_result_are_shared_without_local_project(runtime_pair):
    from dbt_metricflow_service.models import DbtJobRequest
    runtimes, project = runtime_pair
    accepted = await runtimes[0].submit_cli(DbtJobRequest(project=project, command="parse"))
    record = runtimes[1].cli_record(str(accepted.id))
    assert record.status.value == "queued"
    job = runtimes[1].jobs.claim(str(runtimes[1].instance_id), toolchain_version=runtimes[1].toolchain)
    assert job["job_id"] == str(accepted.id)
    assert runtimes[1].jobs.finish(job["job_id"], job["lease_token"], {"stdout": "done"})
    assert runtimes[0].cli_record(str(accepted.id)).status.value == "succeeded"


async def test_volatile_input_is_not_stored_and_queued_owner_loss_finishes(runtime_pair):
    from dbt_metricflow_service.models import DbtJobRequest
    runtimes, project = runtime_pair
    original = "version: 2\n# private-request-marker\n"
    accepted = await runtimes[0].submit_cli(DbtJobRequest(
        project=project, command="parse", resources={"request.yml": original},
    ))
    job_id = str(accepted.id)
    row = runtimes[1].jobs.get(job_id)
    assert "private-request-marker" not in str(row)
    with runtimes[1].db.transaction() as cursor:
        cursor.execute("UPDATE runtime_job SET input_lease_expires_at=clock_timestamp()-interval '1 second' "
                       "WHERE job_id=%s", (job_id,))
    runtimes[1].jobs.recover()
    failed = runtimes[1].cli_record(job_id)
    assert failed.status.value == "failed"
    assert "INPUT_LOST" in failed.stderr


async def test_postgres_http_uses_shared_store_without_project_mount(runtime_pair):
    from dataclasses import replace

    from httpx import ASGITransport, AsyncClient

    from dbt_metricflow_service.api import create_app
    from dbt_metricflow_service.jobs import JobRunner
    from dbt_metricflow_service.projects import ProjectRegistry

    runtimes, project = runtime_pair
    settings = replace(runtimes[0].settings, toolchain_version=runtimes[0].toolchain)
    app = create_app(settings, ProjectRegistry(settings.projects_root), JobRunner(30, 1024))
    try:
        async with AsyncClient(transport=ASGITransport(app), base_url="http://test") as client:
            response = await client.post("/v1/dbt/jobs", json={"project": project, "command": "parse"})
            assert response.status_code == 202
            job_id = response.json()["id"]
            assert runtimes[1].cli_record(job_id).status.value == "queued"
            duplicate = await client.post("/v1/dbt/jobs", json={"project": project, "command": "parse"})
            assert duplicate.status_code == 409
            assert duplicate.json()["detail"]["code"] == "project_busy"
            poll = await client.get("/v1/jobs/" + job_id)
            assert poll.status_code == 200
            assert "resources" not in poll.json()
    finally:
        app.state.runtime.db.close()


async def test_failed_cli_exposes_bounded_exit_diagnostics(runtime_pair):
    from dbt_metricflow_service.models import DbtJobRequest
    runtimes, project = runtime_pair
    accepted = await runtimes[0].submit_cli(DbtJobRequest(project=project, command="parse"))
    job = runtimes[1].jobs.claim(str(runtimes[1].instance_id), toolchain_version=runtimes[1].toolchain)
    runtimes[1].jobs.fail(job["job_id"], job["lease_token"], "COMMAND_FAILED", {
        "exit_code": 7, "stdout": "bounded stdout", "stderr": "bounded stderr", "output_truncated": True,
    })
    record = runtimes[0].cli_record(str(accepted.id))
    assert record.exit_code == 7
    assert record.stdout == "bounded stdout"
    assert record.stderr == "bounded stderr"
    assert record.output_truncated is True
