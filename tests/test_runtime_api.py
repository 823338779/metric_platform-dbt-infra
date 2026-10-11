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
    from dbt_metricflow_service.runtime.service import Runtime
    from dbt_metricflow_service.storage.postgres import Database
    db = Database(dsn)
    db.initialize()
    db.close()
    toolchain = uuid4().hex
    runtimes = [Runtime(Settings(
         profiles_dir=tmp_path / "profiles",
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


