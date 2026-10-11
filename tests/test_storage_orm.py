"""ORM Session 与当前 PostgreSQL 表结构之间的行为契约。"""

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from uuid import uuid4

import pytest
from sqlalchemy import inspect, select, text
from sqlalchemy.orm import Session

from tests.schema_helpers import isolated_database


def test_session_commits_and_rolls_back_without_replacing_connection_api():
    with isolated_database() as db:
        with db.transaction() as connection:
            connection.exec_driver_sql("CREATE TABLE orm_probe(id integer PRIMARY KEY)")
        with db.session() as session:
            assert isinstance(session, Session)
            session.execute(text("INSERT INTO orm_probe VALUES (1)"))
        with pytest.raises(ValueError, match="abort"), db.session() as session:
            session.execute(text("INSERT INTO orm_probe VALUES (2)"))
            raise ValueError("abort")
        with db.transaction() as connection:
            assert connection.exec_driver_sql("SELECT id FROM orm_probe").scalars().all() == [1]


def test_sessions_use_separate_connections_in_parallel_threads():
    with isolated_database() as db:
        barrier = Barrier(2)

        def worker():
            with db.session() as session:
                pid = session.execute(text("SELECT pg_backend_pid()")).scalar_one()
                barrier.wait(timeout=5)
                return pid

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(worker) for _ in range(2)]
            assert len({future.result() for future in futures}) == 2


def test_entity_defaults_and_string_uuid_survive_session_close():
    from dbt_metricflow_service.storage.entities import ArtifactSet, RuntimeProject
    from dbt_metricflow_service.storage.rows import entity_dict

    with isolated_database() as db:
        db.initialize()
        identifier = str(uuid4())
        with db.session() as session:
            session.add(RuntimeProject(project_id="orm-project", config_version="1", binding_config={}))
            session.flush()
            session.add(ArtifactSet(set_id=identifier, project_id="orm-project", kind="SOURCE",
                                    metadata_json={"source": "memory"}))
        with db.session() as session:
            record = entity_dict(session.scalars(select(ArtifactSet).where(ArtifactSet.set_id == identifier)).one())
        assert record["set_id"] == identifier
        assert record["state"] == "STAGING"
        assert record["metadata"] == {"source": "memory"}
        assert record["catalog_json"] == {}
        assert record["sealed_at"] is None


def test_entity_columns_match_initialized_schema():
    from dbt_metricflow_service.storage.entities import Base

    with isolated_database() as db:
        db.initialize()
        with db.transaction() as connection:
            inspector = inspect(connection)
            for table in Base.metadata.tables.values():
                actual = {column["name"]: column for column in inspector.get_columns(table.name)}
                assert set(table.c.keys()) == set(actual), table.name
                assert set(table.primary_key.columns.keys()) == set(
                    inspector.get_pk_constraint(table.name)["constrained_columns"]
                ), table.name
                for column in table.c:
                    assert column.nullable == actual[column.name]["nullable"], (table.name, column.name)
                    assert column.type.compile(dialect=connection.dialect) == actual[column.name]["type"].compile(
                        dialect=connection.dialect
                    ), (table.name, column.name)
                    default = str(column.server_default.arg) if column.server_default is not None else None
                    assert default == actual[column.name]["default"], (table.name, column.name)
