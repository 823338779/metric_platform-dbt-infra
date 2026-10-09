"""Runtime connection contracts against an isolated PostgreSQL schema."""

import os
import time
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import psycopg2
import pytest
from psycopg2 import sql
from psycopg2.extensions import make_dsn
from psycopg2.extras import Json
from sqlalchemy import Connection
from sqlalchemy.exc import DBAPIError, TimeoutError

from dbt_metricflow_service.storage.postgres import Database


@pytest.fixture
def database_dsn():
    dsn = os.getenv("SERVICE_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("SERVICE_TEST_DATABASE_URL is required")
    schema = "connection_" + uuid4().hex
    with psycopg2.connect(dsn) as admin:
        with admin.cursor() as cursor:
            cursor.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
            cursor.execute(sql.SQL("CREATE TABLE {}.records(id integer PRIMARY KEY)").format(sql.Identifier(schema)))
    try:
        yield make_dsn(dsn, options=f"-c search_path={schema}")
    finally:
        with psycopg2.connect(dsn) as admin:
            with admin.cursor() as cursor:
                cursor.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


def test_commit_and_exception_rollback(database_dsn):
    db = Database(database_dsn)
    try:
        with db.transaction() as connection:
            assert isinstance(connection, Connection)
            connection.exec_driver_sql("INSERT INTO records VALUES (1)")
        with pytest.raises(ValueError, match="abort"), db.transaction() as connection:
            connection.exec_driver_sql("INSERT INTO records VALUES (2)")
            raise ValueError("abort")
        with db.transaction() as connection:
            assert connection.exec_driver_sql("SELECT id FROM records").scalars().all() == [1]
    finally:
        db.close()


def test_pool_exhaustion_and_reuse(database_dsn):
    db = Database(database_dsn, max_connections=1)
    try:
        with db.transaction():
            started = time.monotonic()
            with pytest.raises(TimeoutError), db.transaction():
                pass
            assert 4.5 <= time.monotonic() - started < 9
        with db.transaction() as connection:
            assert connection.exec_driver_sql("SELECT 1").scalar_one() == 1
    finally:
        db.close()


def test_concurrent_writes_reuse_bounded_pool(database_dsn):
    db = Database(database_dsn, max_connections=2)

    def insert(number):
        with db.transaction() as connection:
            connection.exec_driver_sql("INSERT INTO records VALUES (%s)", (number,))
            return connection.exec_driver_sql("SELECT pg_backend_pid()").scalar_one()

    try:
        with ThreadPoolExecutor(max_workers=8) as workers:
            pids = list(workers.map(insert, range(24)))
        assert len(set(pids)) <= 2
        with db.transaction() as connection:
            assert connection.exec_driver_sql("SELECT count(*) FROM records").scalar_one() == 24
    finally:
        db.close()


def test_dsn_and_value_compatibility(database_dsn):
    db = Database(database_dsn)
    identifier = str(uuid4())
    try:
        with db.transaction() as connection:
            row = connection.exec_driver_sql(
                "SELECT %s::uuid AS id, NULL::uuid AS optional_id, %s::jsonb AS payload, "
                "%s::bytea AS content, ARRAY[%s::uuid] AS ids",
                (identifier, Json({"中文": "100%"}), b"\x00\xff", identifier),
            ).mappings().one()
            assert row["id"] == identifier
            assert row["optional_id"] is None
            assert row["payload"] == {"中文": "100%"}
            assert bytes(row["content"]) == b"\x00\xff"
            assert row["ids"] == [identifier]
            assert connection.exec_driver_sql("SHOW statement_timeout").scalar_one() == "10s"
            assert connection.exec_driver_sql("SHOW lock_timeout").scalar_one() == "5s"
            # The schema provided through libpq options must survive service defaults.
            assert connection.exec_driver_sql("SELECT count(*) FROM records").scalar_one() == 0
    finally:
        db.close()


@pytest.mark.parametrize("dsn", [
    "postgresql://test:p%25%20%27%40%3A@localhost/example?sslmode=require&options=-c%20application_name%3Dtest",
    "user=test password='p% \\'@:' host=localhost dbname=example sslmode=require options='-c application_name=test'",
])
def test_connection_parameters_preserve_credentials_and_ssl(dsn, monkeypatch):
    parameters = {}

    def connect(dsn="", **kwargs):
        assert dsn == ""
        parameters.update(kwargs)
        raise psycopg2.OperationalError("injected offline")

    monkeypatch.setattr(psycopg2, "connect", connect)
    db = Database(dsn)
    try:
        with pytest.raises(DBAPIError), db.transaction():
            pass
        assert parameters["password"] == "p% '@:"
        assert parameters["sslmode"] == "require"
        assert parameters["connect_timeout"] == 5
        assert "-c application_name=test" in parameters["options"]
    finally:
        db.close()
