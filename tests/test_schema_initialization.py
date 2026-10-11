"""空数据库初始化的完整性、幂等性与事务边界。"""

from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest
from sqlalchemy import event, inspect
from sqlalchemy.exc import IntegrityError

from dbt_metricflow_service.storage.entities import Base
from tests.schema_helpers import isolated_database


def test_initialize_complete_schema_and_repeat_without_changes():
    with isolated_database() as db:
        db.initialize()
        db.check()
        with db.transaction() as connection:
            schema = connection.exec_driver_sql("SELECT current_schema()").scalar_one()
            assert set(inspect(connection).get_table_names(schema=schema)) == set(Base.metadata.tables)
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


@pytest.mark.parametrize("missing_table", [None, "runtime_job_result"])
def test_partial_schema_is_rejected_without_repair(missing_table):
    with isolated_database() as db:
        if missing_table:
            db.initialize()
        with db.transaction() as connection:
            if missing_table:
                connection.exec_driver_sql("DROP TABLE runtime_job_result")
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
                    "('runtime_project_source_fk','runtime_project_output_fk','runtime_project_busy_fk',"
                    "'runtime_job_input_fk','runtime_job_output_fk','runtime_job_attempt_fk')",
                ).scalar_one()
                assert count == 6
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
