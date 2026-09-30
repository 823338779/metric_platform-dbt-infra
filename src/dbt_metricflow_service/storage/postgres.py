"""短事务连接池；迁移仅由管理命令显式执行。"""

from contextlib import contextmanager
from pathlib import Path
from threading import BoundedSemaphore

from psycopg2 import OperationalError
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool

SCHEMA_VERSION = 2
MIGRATION_PATH = Path(__file__).parent / "migrations" / "001_runtime.sql"
MIGRATIONS = (MIGRATION_PATH, MIGRATION_PATH.with_name("002_publication.sql"))
CHECK_SQL = "SELECT version FROM runtime_schema_version"
MIGRATION_LOCK_SQL = "SELECT pg_advisory_xact_lock(609302026)"
CONNECTION_OPTIONS = "-c statement_timeout=10000 -c lock_timeout=5000"
VERSION_TABLE_SQL = "SELECT to_regclass('runtime_schema_version') AS table_name"


class Database:
    """每次事务独占连接，连接数和等待线程均受调用方执行器约束。"""

    def __init__(self, dsn: str, max_connections: int = 8):
        # 信号量使短时并发等待连接归还，避免连接池立即抛出耗尽错误。
        self._pool = ThreadedConnectionPool(1, max_connections, dsn, connect_timeout=5, options=CONNECTION_OPTIONS)
        self._slots = BoundedSemaphore(max_connections)

    @contextmanager
    def transaction(self):
        # psycopg2 的事务上下文在异常时回滚，成功时提交后再归还连接。
        if not self._slots.acquire(timeout=5):
            raise OperationalError("Runtime database pool acquisition timed out")
        try:
            connection = self._pool.getconn()
            try:
                with connection, connection.cursor(cursor_factory=RealDictCursor) as cursor:
                    yield cursor
            finally:
                self._pool.putconn(connection)
        finally:
            self._slots.release()

    def migrate(self):
        # 部署管理命令串行化迁移；普通服务启动仅调用 check。
        with self.transaction() as cursor:
            cursor.execute(MIGRATION_LOCK_SQL)
            cursor.execute(VERSION_TABLE_SQL)
            version = 0
            if cursor.fetchone()["table_name"]:
                cursor.execute(CHECK_SQL)
                versions = [row["version"] for row in cursor.fetchall()]
                if len(versions) != 1 or not 1 <= versions[0] <= SCHEMA_VERSION:
                    raise RuntimeError("Unsupported runtime database schema")
                version = versions[0]
            for migration in MIGRATIONS[version:]:
                cursor.execute(migration.read_text(encoding="utf-8"))

    def check(self):
        # 拒绝未迁移数据库以及当前代码不认识的 schema。
        with self.transaction() as cursor:
            cursor.execute(CHECK_SQL)
            versions = [row["version"] for row in cursor.fetchall()]
            if versions != [SCHEMA_VERSION]:
                raise RuntimeError("Unsupported runtime database schema")

    def close(self):
        self._pool.closeall()
