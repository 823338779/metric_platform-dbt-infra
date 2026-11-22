"""当前结构的显式初始化；连接及事务由调用方管理。"""

from pathlib import Path
from typing import Final

from sqlalchemy import Connection, Uuid, inspect

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
    return set(inspect(connection).get_table_names(schema=schema))


def check(connection: Connection) -> None:
    tables = _tables(connection)
    if not REQUIRED_TABLES.issubset(tables):
        raise RuntimeError(NOT_INITIALIZED)
    inspector = inspect(connection)
    old_queue = strings.RUNTIME_JOB_TABLE in tables
    job_columns = {c["name"] for c in inspector.get_columns(strings.RUNTIME_JOB_TABLE)} if old_queue else set()
    if LEGACY_JOB_DEADLINE_COLUMN in job_columns:
        raise RuntimeError(JOB_DEADLINE_MIGRATION_REQUIRED)
    if LEGACY_RUN_LIFECYCLE_COLUMN in job_columns:
        raise RuntimeError(CLEANUP_MIGRATION_REQUIRED)
    if LEGACY_JOB_KIND_COLUMN in job_columns:
        raise RuntimeError(JOB_KIND_MIGRATION_REQUIRED)
    columns = {column["name"]: column["type"] for column in inspector.get_columns(strings.ENGINE_BUILD_TABLE)}
    if not isinstance(columns.get("idempotency_key"), Uuid):
        raise RuntimeError("Stop services and run global_build_idempotency.sql on the service schema")
    if "physical_target_key" in columns or {"physical_target_key", "target_claimed"} & job_columns:
        raise RuntimeError("Stop services and run remove_physical_target_key.sql on the service schema")
    if "config_version" in columns or "config_version" in job_columns:
        raise RuntimeError("Stop services and run remove_config_versions.sql on the service schema")
    if "request_digest" in columns:
        raise RuntimeError("Stop services and run remove_build_request_digest.sql on the service schema")
    if connection.exec_driver_sql("""
        SELECT EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE connamespace = current_schema()::regnamespace AND contype = 'f'
              AND conname IN ('runtime_job_project_id_fkey', 'runtime_artifact_set_project_id_fkey',
                              'runtime_project_busy_fk', 'runtime_project_source_fk')
        )
    """).scalar_one():
        raise RuntimeError("Stop services and run detach_runtime_project.sql on the service schema")
    if old_queue or "current_attempt_id" not in columns:
        raise RuntimeError("Stop services and run merge_build_queue.sql on the service schema")
    if strings.ENVIRONMENT_ID_FIELD not in columns or strings.EXECUTION_BINDING_FIELD in columns:
        raise RuntimeError(SCHEMA_MISMATCH)


def initialize(connection: Connection) -> None:
    # 串行化初始化；已建库直接返回，部分结构不尝试修补或升级。
    connection.exec_driver_sql(INITIALIZATION_LOCK)
    existing = _tables(connection) & REQUIRED_TABLES
    if existing == REQUIRED_TABLES:
        check(connection)
        return
    if existing:
        raise RuntimeError(NOT_INITIALIZED)
    connection.exec_driver_sql(
        SCHEMA_FILE.read_text(encoding=strings.UTF8), execution_options={strings.SQLALCHEMY_NO_PARAMETERS_OPTION: True}
    )
    check(connection)
