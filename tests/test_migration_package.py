"""Validate migrations from the built wheel, outside the source checkout."""

import os
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest
from psycopg2.extensions import make_dsn

from tests.schema_helpers import isolated_database


@pytest.fixture
def wheel_path():
    value = os.getenv("SERVICE_TEST_WHEEL")
    if not value:
        pytest.skip("Build a wheel and set SERVICE_TEST_WHEEL")
    path = Path(value).resolve()
    assert path.is_file()
    return path


def test_wheel_contains_migration_resources(wheel_path):
    with zipfile.ZipFile(wheel_path) as archive:
        names = set(archive.namelist())
    prefix = "dbt_metricflow_service/storage/"
    for name in (
        "migrations/001_runtime.sql",
        "migrations/002_publication.sql",
        "migrations/003_agent_draft_validation.sql",
        "migrations/004_branch_publications.sql",
        "migrations/005_branch_baselines.sql",
        "legacy_schema.json",
        "alembic/env.py",
        "alembic/versions/0002_build_deployment_contract.py",
        "alembic/script.py.mako",
        "alembic/versions/0001_runtime_adoption.py",
    ):
        assert prefix + name in names


def test_installed_package_migrates_without_repository_cwd(wheel_path, tmp_path):
    target = tmp_path / "installed"
    configuration = tmp_path / "service.yaml"
    configuration.write_text("{}\n", encoding="utf-8")
    subprocess.run(
        ["uv", "pip", "install", "--no-deps", "--target", str(target), str(wheel_path)],
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    with isolated_database() as db:
        with db.transaction() as connection:
            schema = connection.exec_driver_sql("SELECT current_schema()").scalar_one()
        env = {
            **os.environ,
            "PYTHONPATH": str(target),
            "SERVICE_CONFIG_FILE": str(configuration),
            "SERVICE_DATABASE_URL": make_dsn(
                os.environ["SERVICE_TEST_DATABASE_URL"], options=f"-c search_path={schema}"
            ),
            "INSTALLED_TARGET": str(target),
        }
        script = """
import os
from pathlib import Path
import dbt_metricflow_service
from dbt_metricflow_service.admin import main
from dbt_metricflow_service.storage.postgres import Database
assert Path(dbt_metricflow_service.__file__).is_relative_to(Path(os.environ['INSTALLED_TARGET']))
main(['migrate'])
main(['migrate'])
db = Database(os.environ['SERVICE_DATABASE_URL'])
try:
    db.check()
finally:
    db.close()
print('installed migration verified')
"""
        result = subprocess.run(
            [sys.executable, "-c", script], cwd=tmp_path, env=env, capture_output=True, text=True, timeout=60
        )
        assert result.returncode == 0, result.stderr
        assert "installed migration verified" in result.stdout
        db.check()
