"""空数据库初始化的完整性、幂等性与事务边界。"""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import event, inspect
from sqlalchemy.exc import IntegrityError

from dbt_metricflow_service.storage.entities import Base
from tests.schema_helpers import isolated_database


def test_remove_job_deadline_preserves_jobs_and_leases():
    from dbt_metricflow_service.storage import schema

    with isolated_database() as db:
        db.initialize()
        with db.transaction() as connection:
            connection.exec_driver_sql(
                "ALTER TABLE runtime_job ADD COLUMN deadline_at timestamptz NOT NULL DEFAULT clock_timestamp()"
            )
            connection.exec_driver_sql("INSERT INTO runtime_project(project_id) VALUES('deadline-migration')")
            job_id, attempt_id = str(uuid4()), str(uuid4())
            connection.exec_driver_sql(
                "INSERT INTO runtime_job(job_id,project_id,request_fingerprint,config_version,toolchain_version) "
                "VALUES(%s,'deadline-migration','request','1','test')", (job_id,),
            )
            connection.exec_driver_sql(
                "INSERT INTO runtime_attempt(attempt_id,job_id,attempt_no,worker_id,lease_token,lease_expires_at) "
                "VALUES(%s,%s,1,%s,%s,clock_timestamp()+interval '90 seconds')",
                (attempt_id, job_id, str(uuid4()), str(uuid4())),
            )
            before_job = dict(connection.exec_driver_sql("SELECT * FROM runtime_job").mappings().one())
            before_attempt = dict(connection.exec_driver_sql("SELECT * FROM runtime_attempt").mappings().one())
        with pytest.raises(RuntimeError, match="remove_job_deadline.sql"):
            db.check()
        migration = Path(schema.__file__).with_name("remove_job_deadline.sql").read_text(encoding="utf-8")
        with db.transaction() as connection:
            connection.exec_driver_sql(migration)
            connection.exec_driver_sql(migration)
        db.check()
        before_job.pop("deadline_at")
        with db.transaction() as connection:
            assert dict(connection.exec_driver_sql("SELECT * FROM runtime_job").mappings().one()) == before_job
            assert dict(connection.exec_driver_sql("SELECT * FROM runtime_attempt").mappings().one()) == before_attempt


def test_initialize_complete_schema_and_repeat_without_changes():
    with isolated_database() as db:
        db.initialize()
        db.check()
        with db.transaction() as connection:
            schema = connection.exec_driver_sql("SELECT current_schema()").scalar_one()
            assert set(inspect(connection).get_table_names(schema=schema)) == set(Base.metadata.tables)
            assert not {"engine_change", "engine_change_counter"} & set(
                inspect(connection).get_table_names(schema=schema)
            )
            # 初始化后的真实数据和二进制内容不能被重复初始化改写。
            identifier = str(uuid4())
            connection.exec_driver_sql("INSERT INTO runtime_project(project_id) VALUES('initialized')")
            connection.exec_driver_sql(
                "INSERT INTO runtime_artifact_set(set_id,project_id,kind) VALUES(%s,'initialized','SOURCE')",
                (identifier,),
            )
            connection.exec_driver_sql(
                "INSERT INTO runtime_artifact_file"
                "(set_id,relative_path,content,codec,raw_sha256,raw_size,stored_size) "
                "VALUES(%s,'proof',%s,'raw','digest',2,2)",
                (identifier, b"\x00\xff"),
            )
            before = dict(connection.exec_driver_sql("SELECT * FROM runtime_artifact_set").mappings().one())
        db.initialize()
        db.check()
        with db.transaction() as connection:
            assert dict(connection.exec_driver_sql("SELECT * FROM runtime_artifact_set").mappings().one()) == before
            row = connection.exec_driver_sql("SELECT set_id,content FROM runtime_artifact_file").mappings().one()
            assert row["set_id"] == identifier
            assert bytes(row["content"]) == b"\x00\xff"


@pytest.mark.parametrize("invalid_insert", [
    "INSERT INTO runtime_artifact_set(set_id,project_id,kind) "
    "VALUES(gen_random_uuid(),'missing','SOURCE')",
    "INSERT INTO runtime_artifact_set(set_id,project_id,kind) "
    "VALUES(gen_random_uuid(),'initialized','INVALID')",
    "INSERT INTO runtime_artifact_set(set_id,project_id,kind) "
    "VALUES(gen_random_uuid(),'initialized','VALIDATION_INPUT')",
    "INSERT INTO runtime_project(project_id) VALUES('initialized')",
])
def test_initialized_schema_enforces_constraints(invalid_insert):
    with isolated_database() as db:
        db.initialize()
        with db.transaction() as connection:
            connection.exec_driver_sql("INSERT INTO runtime_project(project_id) VALUES('initialized')")
        with pytest.raises(IntegrityError), db.transaction() as connection:
            connection.exec_driver_sql(invalid_insert)
        db.check()


def test_initialized_foreign_keys_and_artifact_guards_are_enabled():
    with isolated_database() as db:
        db.initialize()
        with db.transaction() as connection:
            assert connection.exec_driver_sql(
                "SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid "
                "JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE n.nspname=current_schema() AND t.tgenabled NOT IN ('O','A')",
            ).scalar_one() == 0
            guards = connection.exec_driver_sql(
                "SELECT tgname FROM pg_trigger WHERE tgrelid IN "
                "('runtime_artifact_set'::regclass,'runtime_artifact_file'::regclass) AND NOT tgisinternal",
            ).scalars().all()
            assert {"runtime_artifact_set_guard", "runtime_artifact_file_guard"}.issubset(guards)


def test_check_requires_initialized_schema():
    with isolated_database() as db:
        with pytest.raises(RuntimeError):
            db.check()
        db.initialize()
        db.check()


@pytest.mark.parametrize("missing_table", [None, "runtime_execution_log"])
def test_partial_schema_is_rejected_without_repair(missing_table):
    with isolated_database() as db:
        if missing_table:
            db.initialize()
        with db.transaction() as connection:
            if missing_table:
                connection.exec_driver_sql("DROP TABLE runtime_execution_log")
            else:
                connection.exec_driver_sql("CREATE TABLE runtime_project(project_id text PRIMARY KEY)")
            connection.exec_driver_sql("INSERT INTO runtime_project(project_id) VALUES('preserved')")
            schema = connection.exec_driver_sql("SELECT current_schema()").scalar_one()
            before = inspect(connection).get_table_names(schema=schema)
        with pytest.raises(RuntimeError):
            db.check()
        with pytest.raises(RuntimeError):
            db.initialize()
        # 部分结构不会被自动修复，也不会覆盖已存在的业务数据。
        with db.transaction() as connection:
            assert inspect(connection).get_table_names(schema=schema) == before
            assert connection.exec_driver_sql("SELECT project_id FROM runtime_project").scalars().all() == ["preserved"]


def test_identifier_display_setting_does_not_change_schema_contract():
    from dbt_metricflow_service.storage.schema import initialize

    with isolated_database() as db:
        with db.transaction() as connection:
            connection.exec_driver_sql("SET LOCAL quote_all_identifiers=on")
            initialize(connection)
            assert connection.exec_driver_sql("SHOW quote_all_identifiers").scalar_one() == "on"
        db.check()


def test_concurrent_initialization_is_serialized():
    with isolated_database() as db:
        with ThreadPoolExecutor(max_workers=2) as workers:
            list(workers.map(lambda _: db.initialize(), range(2)))
        db.check()


def test_two_schemas_do_not_share_tables_or_constraints():
    with isolated_database(include_public=True) as first, isolated_database('_"%') as second:
        first.initialize()
        second.initialize()
        first.check()
        second.check()
        for db in (first, second):
            with db.transaction() as connection:
                count = connection.exec_driver_sql(
                    "SELECT count(*) FROM pg_constraint WHERE conrelid IN "
                    "('runtime_project'::regclass,'runtime_job'::regclass) AND conname IN "
                    "('runtime_project_source_fk','runtime_project_busy_fk',"
                    "'runtime_job_input_fk','runtime_job_output_fk','runtime_job_attempt_fk')",
                ).scalar_one()
                assert count == 5
                schema = connection.exec_driver_sql("SELECT current_schema()").scalar_one()
                assert set(inspect(connection).get_table_names(schema=schema)) == set(Base.metadata.tables)


def test_initialization_failure_is_atomic():
    # DDL 已实际执行后抛错，验证表和触发器随外层事务全部回滚。
    def fail_after_ddl(connection, cursor, statement, parameters, context, executemany):
        if "CREATE TRIGGER runtime_artifact_file_guard" in statement:
            raise RuntimeError("injected after initialization DDL")

    with isolated_database() as db:
        event.listen(db.engine, "after_cursor_execute", fail_after_ddl)
        try:
            with pytest.raises(RuntimeError, match="injected"):
                db.initialize()
        finally:
            event.remove(db.engine, "after_cursor_execute", fail_after_ddl)
        with db.transaction() as connection:
            schema = connection.exec_driver_sql("SELECT current_schema()").scalar_one()
            assert inspect(connection).get_table_names(schema=schema) == []
        db.initialize()
        db.check()


def test_remove_cleanup_preserves_history_and_rejects_new_cleanup_jobs():
    from dbt_metricflow_service.storage import schema
    from dbt_metricflow_service.storage.jobs import JobStore

    with isolated_database() as db:
        db.initialize()
        with db.transaction() as connection:
            connection.exec_driver_sql("ALTER TABLE runtime_job ADD COLUMN run_lifecycle text")
            connection.exec_driver_sql("ALTER TABLE runtime_job ADD COLUMN kind text NOT NULL DEFAULT 'BUILD_RUN'")
            connection.exec_driver_sql("INSERT INTO runtime_project(project_id) VALUES('cleanup-migration')")
            build_job, cleanup_job = str(uuid4()), str(uuid4())
            for identifier, kind in [(build_job, 'BUILD_RUN'), (cleanup_job, 'RUN_CLEANUP')]:
                connection.exec_driver_sql(
                    "INSERT INTO runtime_job(job_id,kind,project_id,request_fingerprint,"
                    "config_version,toolchain_version) "
                    "VALUES(%s,%s,'cleanup-migration','request','1','test')", (identifier, kind),
                )
            connection.exec_driver_sql(
                "UPDATE runtime_job SET run_lifecycle='CLEANING' WHERE job_id=%s", (build_job,),
            )
        with pytest.raises(RuntimeError, match="remove_run_cleanup.sql"):
            db.check()
        migration = Path(schema.__file__).with_name("remove_run_cleanup.sql").read_text(encoding="utf-8")
        with db.transaction() as connection:
            connection.exec_driver_sql(migration)
            connection.exec_driver_sql(migration)
        with pytest.raises(RuntimeError, match="remove_job_kind.sql"):
            db.check()
        jobs = JobStore(db)
        assert jobs.get(build_job).status == "QUEUED"
        assert jobs.get(cleanup_job).status == "FAILED"
        assert jobs.get(cleanup_job).error_code == "CLEANUP_REMOVED"
        with pytest.raises(IntegrityError), db.transaction() as connection:
            connection.exec_driver_sql(
                "INSERT INTO runtime_job(job_id,kind,project_id,request_fingerprint,config_version,toolchain_version) "
                "VALUES(%s,'RUN_CLEANUP','cleanup-migration','request','1','test')", (str(uuid4()),),
            )
        migration = Path(schema.__file__).with_name("remove_job_kind.sql").read_text(encoding="utf-8")
        with db.transaction() as connection:
            connection.exec_driver_sql(migration)
            connection.exec_driver_sql(migration)
        db.check()
        assert jobs.claim(str(uuid4()), toolchain_version="test").job_id == build_job


@pytest.mark.parametrize("legacy_status", ["QUEUED", "RUNNING", "SUCCEEDED"])
def test_remove_job_kind_retires_legacy_jobs_without_losing_attempts(legacy_status):
    from dbt_metricflow_service.storage import schema
    from dbt_metricflow_service.storage.jobs import JobStore
    from dbt_metricflow_service.storage.records import JobRequest

    with isolated_database() as db:
        db.initialize()
        jobs = JobStore(db)
        jobs.register_project("kind-migration")
        build = jobs.reserve("kind-migration", JobRequest(), idempotency_key="build")
        legacy = jobs.reserve("kind-migration", JobRequest())
        attempt_id = str(uuid4())
        with db.transaction() as connection:
            connection.exec_driver_sql("ALTER TABLE runtime_job ADD COLUMN kind text NOT NULL DEFAULT 'BUILD_RUN'")
            connection.exec_driver_sql(
                "UPDATE runtime_job SET kind='RUN_CLEANUP',status=%s WHERE job_id=%s",
                (legacy_status, legacy.job_id),
            )
            connection.exec_driver_sql(
                "INSERT INTO runtime_attempt(attempt_id,job_id,attempt_no,worker_id,lease_token,lease_expires_at) "
                "VALUES(%s,%s,1,%s,%s,clock_timestamp()+interval '90 seconds')",
                (attempt_id, legacy.job_id, str(uuid4()), str(uuid4())),
            )
            before_attempt = dict(connection.exec_driver_sql("SELECT * FROM runtime_attempt").mappings().one())
        with pytest.raises(RuntimeError, match="remove_job_kind.sql"):
            db.check()
        migration = Path(schema.__file__).with_name("remove_job_kind.sql").read_text(encoding="utf-8")
        with db.transaction() as connection:
            connection.exec_driver_sql(migration)
            connection.exec_driver_sql(migration)
            assert dict(connection.exec_driver_sql("SELECT * FROM runtime_attempt").mappings().one()) == before_attempt
        db.check()
        assert jobs.get(legacy.job_id).status == ("SUCCEEDED" if legacy_status == "SUCCEEDED" else "FAILED")
        assert jobs.reserve("kind-migration", JobRequest(), idempotency_key="build").job_id == build.job_id
        assert jobs.claim(str(uuid4())).job_id == build.job_id
        assert jobs.claim(str(uuid4())) is None
