"""当前结构的显式初始化；连接及事务由调用方管理。"""

from pathlib import Path

from sqlalchemy import Connection, inspect

from .entities import Base

SCHEMA_FILE = Path(__file__).with_suffix(".sql")
REQUIRED_TABLES = frozenset(Base.metadata.tables)
NOT_INITIALIZED = "Runtime database is not initialized; run dbt-service-admin init-db on an empty schema"
CURRENT_SCHEMA = "SELECT current_schema()"
MIGRATIONS = Path(__file__).parent / "migrations"
UPGRADE_REQUIRED = "Runtime database requires dbt-service-admin upgrade-db"
INITIALIZATION_LOCK = "SELECT pg_advisory_xact_lock(609302026)"


def _tables(connection: Connection) -> set[str]:
    # 只检查当前 schema，避免 search_path 中其他 schema 的同名表掩盖缺失。
    schema = connection.exec_driver_sql(CURRENT_SCHEMA).scalar_one()
    return set(inspect(connection).get_table_names(schema=schema)) & REQUIRED_TABLES


def check(connection: Connection) -> None:
    if _tables(connection) != REQUIRED_TABLES:
        raise RuntimeError(NOT_INITIALIZED)
    columns = {column["name"] for column in inspect(connection).get_columns("engine_build")}
    if "protocol_version" not in columns:
        raise RuntimeError(UPGRADE_REQUIRED)


def upgrade(connection: Connection) -> None:
    """显式、事务化增量升级；不重建已有事实表。"""
    connection.exec_driver_sql(INITIALIZATION_LOCK)
    if _tables(connection) != REQUIRED_TABLES:
        raise RuntimeError(NOT_INITIALIZED)
    for migration in sorted(MIGRATIONS.glob("*.sql")):
        connection.exec_driver_sql(migration.read_text(encoding="utf-8"), execution_options={"no_parameters": True})
    check(connection)


def initialize(connection: Connection) -> None:
    # 串行化初始化；已建库直接返回，部分结构不尝试修补或升级。
    connection.exec_driver_sql(INITIALIZATION_LOCK)
    existing = _tables(connection)
    if existing == REQUIRED_TABLES:
        return
    if existing:
        raise RuntimeError(NOT_INITIALIZED)
    connection.exec_driver_sql(SCHEMA_FILE.read_text(encoding="utf-8"), execution_options={"no_parameters": True})
    upgrade(connection)
