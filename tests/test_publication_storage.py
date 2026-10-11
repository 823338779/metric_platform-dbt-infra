"""为构建与部署测试提供独立 PostgreSQL 连接。"""

import os
from types import SimpleNamespace

import pytest

from dbt_metricflow_service.storage.postgres import Database

DSN_ENV = "SERVICE_TEST_DATABASE_URL"
PROJECT_PREFIX = "publication-test-"


@pytest.fixture
def store():
    dsn = os.getenv(DSN_ENV)
    if not dsn:
        pytest.skip("需要独立 SERVICE_TEST_DATABASE_URL")
    db = Database(dsn)
    db.initialize()
    db.check()
    yield SimpleNamespace(db=db)
    db.close()
