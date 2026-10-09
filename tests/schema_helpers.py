"""Disposable PostgreSQL schemas and authentic historical SQL fixtures."""

import os
from contextlib import contextmanager
from uuid import uuid4

import pytest
from sqlalchemy import event

from dbt_metricflow_service.storage.postgres import MIGRATIONS, Database


@contextmanager
def isolated_database(suffix="", *, include_public=False):
    dsn = os.getenv("SERVICE_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("SERVICE_TEST_DATABASE_URL is required")
    db = Database(dsn)
    name = "migration_" + uuid4().hex + suffix
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


def prepare_legacy(db, version):
    with db.transaction() as connection:
        for index, migration in enumerate(MIGRATIONS[:version]):
            connection.exec_driver_sql(migration.read_text(encoding="utf-8"), execution_options={"no_parameters": True})
            if index == 0:
                # Historical 001 checks constraint names across all schemas. Construct
                # the schema it produces in a fresh database, independent of neighbours.
                for table, column, target, target_column, name in (
                    ("runtime_project", "source_set_id", "runtime_artifact_set", "set_id", "runtime_project_source_fk"),
                    (
                        "runtime_project",
                        "current_output_set_id",
                        "runtime_artifact_set",
                        "set_id",
                        "runtime_project_output_fk",
                    ),
                    ("runtime_project", "busy_job_id", "runtime_job", "job_id", "runtime_project_busy_fk"),
                    ("runtime_job", "input_set_id", "runtime_artifact_set", "set_id", "runtime_job_input_fk"),
                    ("runtime_job", "output_set_id", "runtime_artifact_set", "set_id", "runtime_job_output_fk"),
                    ("runtime_job", "current_attempt_id", "runtime_attempt", "attempt_id", "runtime_job_attempt_fk"),
                ):
                    exists = connection.exec_driver_sql(
                        "SELECT 1 FROM pg_constraint WHERE conrelid=%s::regclass AND conname=%s", (table, name),
                    ).first()
                    if not exists:
                        connection.exec_driver_sql(
                            f"ALTER TABLE {table} ADD CONSTRAINT {name} FOREIGN KEY({column}) "
                            f"REFERENCES {target}({target_column})",
                        )
