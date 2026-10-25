"""当前结构的显式初始化；连接及事务由调用方管理。"""

from pathlib import Path
from typing import Final

from sqlalchemy import Connection, inspect

from dbt_metricflow_service import constants as strings

from .entities import Base

SCHEMA_FILE = Path(__file__).with_suffix(strings.SQL_EXTENSION)
REQUIRED_TABLES = frozenset(Base.metadata.tables)
NOT_INITIALIZED: Final = "Runtime database is not initialized; run dbt-service-admin init-db on an empty schema"
CURRENT_SCHEMA = "SELECT current_schema()"
SCHEMA_MISMATCH: Final = "Runtime database schema is incompatible; initialize a new empty schema"
JOB_DEADLINE_MIGRATION_REQUIRED: Final = "Stop services and run remove_job_deadline.sql on the service schema"
CLEANUP_MIGRATION_REQUIRED: Final = "Stop services and run remove_run_cleanup.sql on the service schema"
JOB_KIND_MIGRATION_REQUIRED: Final = "Stop services and run remove_job_kind.sql on the service schema"
LEGACY_JOB_KIND_COLUMN: Final = strings.RESOURCE_KIND_FIELD
LEGACY_RUN_LIFECYCLE_COLUMN: Final = "run_lifecycle"
LEGACY_JOB_DEADLINE_COLUMN: Final = "deadline_at"
INITIALIZATION_LOCK = "SELECT pg_advisory_xact_lock(609302026)"


def _tables(connection: Connection) -> set[str]:
    # 只检查当前 schema，避免 search_path 中其他 schema 的同名表掩盖缺失。
    schema = connection.exec_driver_sql(CURRENT_SCHEMA).scalar_one()
    return set(inspect(connection).get_table_names(schema=schema)) & REQUIRED_TABLES


def check(connection: Connection) -> None:
    if _tables(connection) != REQUIRED_TABLES:
        raise RuntimeError(NOT_INITIALIZED)
    job_columns = {column[strings.NAME_FIELD] for column in inspect(connection).get_columns(strings.RUNTIME_JOB_TABLE)}
    if LEGACY_JOB_DEADLINE_COLUMN in job_columns:
        raise RuntimeError(JOB_DEADLINE_MIGRATION_REQUIRED)
    if LEGACY_RUN_LIFECYCLE_COLUMN in job_columns:
        raise RuntimeError(CLEANUP_MIGRATION_REQUIRED)
    if LEGACY_JOB_KIND_COLUMN in job_columns:
        raise RuntimeError(JOB_KIND_MIGRATION_REQUIRED)
    columns = {column[strings.NAME_FIELD] for column in inspect(connection).get_columns(strings.ENGINE_BUILD_TABLE)}
    if (
        not {strings.ENVIRONMENT_ID_FIELD, strings.PHYSICAL_TARGET_KEY_FIELD}.issubset(columns)
        or strings.EXECUTION_BINDING_FIELD in columns
    ):
        raise RuntimeError(SCHEMA_MISMATCH)


def initialize(connection: Connection) -> None:
    # 串行化初始化；已建库直接返回，部分结构不尝试修补或升级。
    connection.exec_driver_sql(INITIALIZATION_LOCK)
    existing = _tables(connection)
    if existing == REQUIRED_TABLES:
        return
    if existing:
        raise RuntimeError(NOT_INITIALIZED)
    connection.exec_driver_sql(
        SCHEMA_FILE.read_text(encoding=strings.UTF8), execution_options={strings.SQLALCHEMY_NO_PARAMETERS_OPTION: True}
    )
    check(connection)
