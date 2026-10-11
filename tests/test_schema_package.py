"""验证 wheel 包含初始化结构，并能在源码目录之外初始化数据库。"""

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


def test_wheel_contains_schema_resource(wheel_path):
    with zipfile.ZipFile(wheel_path) as archive:
        names = set(archive.namelist())
    prefix = "dbt_metricflow_service/storage/"
    assert prefix + "schema.sql" in names
    assert not any(name.startswith((prefix + "alembic/", prefix + "migrations/")) for name in names)
    for removed in ("legacy_schema.py", "legacy_schema.json", "migration.py", "publication_import.py",
                    "history_models.py", "validation_audit.py"):
        assert prefix + removed not in names


def test_installed_package_initializes_without_repository_cwd(wheel_path, tmp_path):
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
main(['init-db'])
main(['init-db'])
db = Database(os.environ['SERVICE_DATABASE_URL'])
try:
    db.check()
finally:
    db.close()
print('installed initialization verified')
"""
        result = subprocess.run(
            [sys.executable, "-c", script], cwd=tmp_path, env=env, capture_output=True, text=True, timeout=60
        )
        assert result.returncode == 0, result.stderr
        assert "installed initialization verified" in result.stdout
        db.check()
