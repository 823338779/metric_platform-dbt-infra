"""短事务连接池；迁移仅由管理命令显式执行。"""

from contextlib import AbstractContextManager, contextmanager
from pathlib import Path

from psycopg2.extensions import new_array_type, new_type, parse_dsn, register_type
from sqlalchemy import Connection, create_engine, event

MIGRATION_PATH = Path(__file__).parent / "migrations" / "001_runtime.sql"
MIGRATIONS = (MIGRATION_PATH, MIGRATION_PATH.with_name("002_publication.sql"),
              MIGRATION_PATH.with_name("003_agent_draft_validation.sql"),
              MIGRATION_PATH.with_name("004_branch_publications.sql"),
              MIGRATION_PATH.with_name("005_branch_baselines.sql"))
CONNECTION_OPTIONS = "-c statement_timeout=10000 -c lock_timeout=5000"
SQL_LOCK_FACTS = "SELECT value FROM engine_change_counter WHERE singleton FOR UPDATE"


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

    @contextmanager
    def fact_transaction(self):
        # 事实事务先锁变化序号，再锁业务行，避免触发器与目标/任务锁顺序反转。
        # 只包含短数据库操作；Git、引擎执行和产物传输均在事务之外。
        with self.transaction() as connection:
            connection.exec_driver_sql(SQL_LOCK_FACTS)
            yield connection

    def migrate(self):
        from .schema import migrate

        with self.transaction() as connection:
            migrate(connection)

    def check(self):
        from .schema import check

        with self.transaction() as connection:
            check(connection)

    def close(self):
        self.engine.dispose()


def _string_uuids(connection, _record):
    # 只配置本 Engine 的连接，保留 Store 既有的字符串 UUID 契约。
    uuid_type = new_type((2950,), "RUNTIME_UUID", lambda value, _cursor: value)
    register_type(uuid_type, connection)
    register_type(new_array_type((2951,), "RUNTIME_UUID_ARRAY", uuid_type), connection)
