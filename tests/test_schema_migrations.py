"""Alembic adoption preserves data and rejects unsupported historical schemas."""

from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest
from alembic import command

from tests.schema_helpers import isolated_database, prepare_legacy


@pytest.mark.parametrize("version", range(6))
def test_adopt_supported_versions(version):
    with isolated_database() as db:
        if version:
            prepare_legacy(db, version)
            identifier = str(uuid4())
            with db.transaction() as connection:
                connection.exec_driver_sql("INSERT INTO runtime_project(project_id) VALUES('historical')")
                connection.exec_driver_sql(
                    "INSERT INTO runtime_artifact_set(set_id,project_id,kind) VALUES(%s,'historical','SOURCE')",
                    (identifier,),
                )
                connection.exec_driver_sql(
                    "INSERT INTO runtime_artifact_file"
                    "(set_id,relative_path,content,codec,raw_sha256,raw_size,stored_size) "
                    "VALUES(%s,'proof',%s,'raw','digest',2,2)",
                    (identifier, b"\x00\xff"),
                )
        db.migrate()
        db.check()
        with db.transaction() as connection:
            assert connection.exec_driver_sql("SELECT to_regclass('alembic_version')").scalar_one() is not None
            assert connection.exec_driver_sql("SELECT version_num FROM alembic_version").scalars().all() == [
                "0001_runtime_adoption",
            ]
            assert connection.exec_driver_sql("SELECT version FROM runtime_schema_version").scalars().all() == [5]
            if version:
                row = connection.exec_driver_sql("SELECT set_id,content FROM runtime_artifact_file").mappings().one()
                assert row["set_id"] == identifier
                assert bytes(row["content"]) == b"\x00\xff"
        db.migrate()
        db.check()


@pytest.mark.parametrize("damage", [
    "UPDATE runtime_schema_version SET version=0",
    "UPDATE runtime_schema_version SET version=6",
    "INSERT INTO runtime_schema_version VALUES(4)",
    "DROP TABLE runtime_schema_version",
    "ALTER TABLE runtime_job DROP COLUMN error_detail",
    "ALTER TABLE runtime_job DROP CONSTRAINT runtime_job_input_fk",
    "DROP TRIGGER runtime_artifact_file_guard ON runtime_artifact_file",
    "ALTER TABLE runtime_artifact_set DISABLE TRIGGER runtime_artifact_set_guard",
])
def test_reject_invalid_history(damage):
    with isolated_database() as db:
        prepare_legacy(db, 5)
        with db.transaction() as connection:
            connection.exec_driver_sql(damage)
        with pytest.raises(RuntimeError):
            db.migrate()
        with db.transaction() as connection:
            assert connection.exec_driver_sql("SELECT to_regclass('alembic_version')").scalar_one() is None


def test_check_requires_supported_alembic_head():
    with isolated_database() as db:
        prepare_legacy(db, 5)
        with pytest.raises(RuntimeError):
            db.check()
        db.migrate()
        with db.transaction() as connection:
            connection.exec_driver_sql("UPDATE alembic_version SET version_num='unknown'")
        with pytest.raises(RuntimeError):
            db.check()
        with pytest.raises(RuntimeError):
            db.migrate()


def test_disabled_foreign_key_enforcement_rejects_adoption():
    with isolated_database() as db:
        prepare_legacy(db, 5)
        with db.transaction() as connection:
            connection.exec_driver_sql("ALTER TABLE runtime_project DISABLE TRIGGER ALL")
        with pytest.raises(RuntimeError):
            db.migrate()


@pytest.mark.parametrize("version", [0, 5])
def test_identifier_display_setting_does_not_change_schema_contract(version):
    from dbt_metricflow_service.storage.schema import migrate

    with isolated_database() as db:
        if version:
            prepare_legacy(db, version)
        with db.transaction() as connection:
            connection.exec_driver_sql("SET LOCAL quote_all_identifiers=on")
            migrate(connection)
            assert connection.exec_driver_sql("SHOW quote_all_identifiers").scalar_one() == "on"
        db.check()


def test_concurrent_migration_is_serialized():
    with isolated_database() as db:
        with ThreadPoolExecutor(max_workers=2) as workers:
            list(workers.map(lambda _: db.migrate(), range(2)))
        db.check()


def test_two_schemas_do_not_share_history():
    with isolated_database(include_public=True) as first, isolated_database('_"%') as second:
        first.migrate()
        second.migrate()
        first.check()
        second.check()
        for db in (first, second):
            with db.transaction() as connection:
                count = connection.exec_driver_sql(
                    "SELECT count(*) FROM pg_constraint WHERE conrelid IN "
                    "('runtime_project'::regclass,'runtime_job'::regclass) AND conname IN "
                    "('runtime_project_source_fk','runtime_project_output_fk','runtime_project_busy_fk',"
                    "'runtime_job_input_fk','runtime_job_output_fk','runtime_job_attempt_fk')",
                ).scalar_one()
                assert count == 6


@pytest.mark.parametrize("change", [
    "INSERT INTO alembic_version VALUES('second_head')",
    "UPDATE runtime_schema_version SET version=4",
])
def test_inconsistent_adopted_versions_are_rejected(change):
    with isolated_database() as db:
        db.migrate()
        with db.transaction() as connection:
            connection.exec_driver_sql(change)
        with pytest.raises(RuntimeError):
            db.check()
        with pytest.raises(RuntimeError):
            db.migrate()


def test_migration_failure_is_atomic(monkeypatch):
    from dbt_metricflow_service.storage import legacy_schema

    with isolated_database() as db:
        prepare_legacy(db, 3)
        original = legacy_schema.validate_legacy

        def fail_after_upgrade(connection, version):
            original(connection, version)
            if version == 5:
                raise RuntimeError("injected after historical upgrade")

        monkeypatch.setattr(legacy_schema, "validate_legacy", fail_after_upgrade)
        with pytest.raises(RuntimeError, match="injected"):
            db.migrate()
        with db.transaction() as connection:
            assert connection.exec_driver_sql("SELECT version FROM runtime_schema_version").scalar_one() == 3
            assert connection.exec_driver_sql("SELECT to_regclass('runtime_branch')").scalar_one() is None
            assert connection.exec_driver_sql("SELECT to_regclass('alembic_version')").scalar_one() is None
        monkeypatch.setattr(legacy_schema, "validate_legacy", original)
        db.migrate()
        db.check()


def test_downgrade_is_rejected_without_changes():
    from dbt_metricflow_service.storage.schema import alembic_config

    with isolated_database() as db:
        db.migrate()
        with pytest.raises(RuntimeError, match="downgrade"), db.transaction() as connection:
            command.downgrade(alembic_config(connection), "base")
        db.check()
