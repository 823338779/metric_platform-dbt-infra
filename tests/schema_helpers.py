"""独立 PostgreSQL schema，隔离初始化与业务事务测试。"""

import os
from contextlib import contextmanager
from uuid import uuid4

import pytest
from sqlalchemy import event

from dbt_metricflow_service.storage.postgres import Database


@contextmanager
def isolated_database(suffix="", *, include_public=False):
    dsn = os.getenv("SERVICE_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("SERVICE_TEST_DATABASE_URL is required")
    db = Database(dsn)
    name = "storage_" + uuid4().hex + suffix
    quoted = db.engine.dialect.identifier_preparer.quote(name)
    with db.transaction() as connection:
        connection.exec_driver_sql(f"CREATE SCHEMA {quoted}", execution_options={"no_parameters": True})

    def search_path(connection):
        path = quoted + (",public" if include_public else "")
        connection.exec_driver_sql(f"SET LOCAL search_path TO {path}", execution_options={"no_parameters": True})

    event.listen(db.engine, "begin", search_path)
    try:
        yield db
    finally:
        event.remove(db.engine, "begin", search_path)
        try:
            with db.transaction() as connection:
                connection.exec_driver_sql(f"DROP SCHEMA {quoted} CASCADE", execution_options={"no_parameters": True})
        finally:
            db.close()
