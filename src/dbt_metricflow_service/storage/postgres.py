"""短事务连接池；迁移仅由管理命令显式执行。"""

from contextlib import AbstractContextManager
from pathlib import Path

from psycopg2.extensions import new_array_type, new_type, parse_dsn, register_type
from sqlalchemy import Connection, create_engine, event

from dbt_metricflow_service.storage.rows import row_dict

SCHEMA_VERSION = 5
MIGRATION_PATH = Path(__file__).parent / "migrations" / "001_runtime.sql"
MIGRATIONS = (MIGRATION_PATH, MIGRATION_PATH.with_name("002_publication.sql"),
              MIGRATION_PATH.with_name("003_agent_draft_validation.sql"),
              MIGRATION_PATH.with_name("004_branch_publications.sql"),
              MIGRATION_PATH.with_name("005_branch_baselines.sql"))
CHECK_SQL = "SELECT version FROM runtime_schema_version"
MIGRATION_LOCK_SQL = "SELECT pg_advisory_xact_lock(609302026)"
CONNECTION_OPTIONS = "-c statement_timeout=10000 -c lock_timeout=5000"
VERSION_TABLE_SQL = "SELECT to_regclass('runtime_schema_version') AS table_name"


class Database:
    """每次事务独占连接，连接数和等待线程均受调用方执行器约束。"""

    def __init__(self, dsn: str, max_connections: int = 8):
        parameters = parse_dsn(dsn)
        parameters["options"] = (parameters.get("options", "") + " " + CONNECTION_OPTIONS).strip()
        parameters["connect_timeout"] = 5
        self.engine = create_engine(
            "postgresql+psycopg2://", connect_args=parameters,
            pool_size=max_connections, max_overflow=0, pool_timeout=5, pool_pre_ping=True,
        )
        event.listen(self.engine, "connect", _string_uuids)

    def transaction(self) -> AbstractContextManager[Connection]:
        return self.engine.begin()

    def migrate(self):
        # 部署管理命令串行化迁移；普通服务启动仅调用 check。
        with self.transaction() as connection:
            sql_result = connection.exec_driver_sql(MIGRATION_LOCK_SQL, execution_options={"no_parameters": True})
            sql_result = connection.exec_driver_sql(VERSION_TABLE_SQL, execution_options={"no_parameters": True})
            version = 0
            if row_dict(sql_result)["table_name"]:
                sql_result = connection.exec_driver_sql(CHECK_SQL, execution_options={"no_parameters": True})
                versions = [row["version"] for row in sql_result.mappings()]
                if len(versions) != 1 or not 1 <= versions[0] <= SCHEMA_VERSION:
                    raise RuntimeError("Unsupported runtime database schema")
                version = versions[0]
            for migration in MIGRATIONS[version:]:
                sql_result = connection.exec_driver_sql(
                    migration.read_text(encoding="utf-8"), execution_options={"no_parameters": True}
                )

    def check(self):
        # 拒绝未迁移数据库以及当前代码不认识的 schema。
        with self.transaction() as connection:
            sql_result = connection.exec_driver_sql(CHECK_SQL, execution_options={"no_parameters": True})
            versions = [row["version"] for row in sql_result.mappings()]
            if versions != [SCHEMA_VERSION]:
                raise RuntimeError("Unsupported runtime database schema")

    def close(self):
        self.engine.dispose()


def _string_uuids(connection, _record):
    # 只配置本 Engine 的连接，保留 Store 既有的字符串 UUID 契约。
    uuid_type = new_type((2950,), "RUNTIME_UUID", lambda value, _cursor: value)
    register_type(uuid_type, connection)
    register_type(new_array_type((2951,), "RUNTIME_UUID_ARRAY", uuid_type), connection)
