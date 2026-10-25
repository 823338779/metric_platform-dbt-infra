import os
from pathlib import Path
from uuid import uuid4

import psycopg2
import pytest
from psycopg2 import sql
from psycopg2.extensions import make_dsn

from dbt_metricflow_service.models.builds import BuildRequest
from dbt_metricflow_service.settings import Settings
from dbt_metricflow_service.storage.postgres import Database


@pytest.fixture
def settings(tmp_path):
    return Settings(
        profiles_dir=tmp_path, temp_root=tmp_path, max_output_bytes=1024,
        database_url="postgresql://unused.invalid/test",
        execution_instances={"local": {"type": "duckdb", "path": str(tmp_path / "warehouse.duckdb")}},
    )


@pytest.fixture
def build_request(settings):
    return BuildRequest.model_validate({
        "repository": "https://example.invalid/repo.git", "branchName": "main",
        "commitSha": "b" * 40, "idempotencyKey": "build-1",
        "execution": {
            "environmentId": "00000000-0000-0000-0000-000000000001", "configVersion": 1,
            "connection": {"type": "duckdb", "instanceId": "local",
                           "path": str(Path(settings.temp_root) / "warehouse.duckdb")},
            "credentialRef": "local", "catalog": "warehouse", "schema": "analytics",
        },
    })


@pytest.fixture
def database():
    dsn = os.environ.get("DBT_SERVICE_TEST_DSN")
    if not dsn:
        pytest.skip("set DBT_SERVICE_TEST_DSN to an isolated PostgreSQL test instance")
    schema = "api_test_" + uuid4().hex
    connection = psycopg2.connect(dsn)
    connection.autocommit = True
    with connection.cursor() as cursor:
        cursor.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    db = Database(make_dsn(dsn, options=f"-c search_path={schema}"))
    try:
        db.initialize()
        yield db
    finally:
        db.close()
        with connection.cursor() as cursor:
            cursor.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
        connection.close()
