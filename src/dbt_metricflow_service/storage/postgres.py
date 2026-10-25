"""短事务连接池；建表仅由管理命令显式执行。"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager

from psycopg2.extensions import connection as PsycopgConnection
from psycopg2.extensions import new_array_type, new_type, parse_dsn, register_type
from sqlalchemy import Connection, create_engine, event, func, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import ConnectionPoolEntry

from dbt_metricflow_service import constants as strings

CONNECTION_OPTIONS = "-c statement_timeout=10000 -c lock_timeout=5000"
# 保持同一 schema 内事实事务的原有串行约束，不再依赖阶段日志计数器表。
# PostgreSQL advisory lock 属于当前数据库，schema 名使不同服务 schema 相互隔离。
FACT_LOCK = select(func.pg_advisory_xact_lock(func.hashtextextended(func.current_schema(), 609302027)))


class Database:
    """每次事务独占连接，连接数和等待线程均受调用方执行器约束。"""

    def __init__(self, dsn: str, max_connections: int = 8) -> None:
        parameters = parse_dsn(dsn)
        parameters[strings.POSTGRES_OPTIONS_FIELD] = (
            parameters.get(strings.POSTGRES_OPTIONS_FIELD, "") + " " + CONNECTION_OPTIONS
        ).strip()
        parameters[strings.POSTGRES_CONNECT_TIMEOUT_FIELD] = 5
        self.engine = create_engine(
            strings.POSTGRES_URL, connect_args=parameters,
            pool_size=max_connections, max_overflow=0, pool_timeout=5, pool_pre_ping=True,
        )
        event.listen(self.engine, strings.SQLALCHEMY_CONNECT_EVENT, _string_uuids)
        # 每次 Store 事务独立创建 Session；显式写入避免 autoflush 改变加锁顺序。
        self._sessions = sessionmaker(bind=self.engine, autoflush=False, expire_on_commit=False)

    def transaction(self) -> AbstractContextManager[Connection]:
        return self.engine.begin()

    def session(self) -> AbstractContextManager[Session]:
        """提交成功的短事务，异常时回滚并关闭当前 Session。"""
        return self._sessions.begin()

    @contextmanager
    def fact_session(self) -> Iterator[Session]:
        # ORM 事实写入先取得独立事务锁，随后再取得业务行锁。
        with self.session() as session:
            session.execute(FACT_LOCK)
            yield session

    @contextmanager
    def fact_transaction(self) -> Iterator[Connection]:
        # 与 ORM 使用相同事实锁；提交或回滚时由数据库自动释放。
        # 只包含短数据库操作；Git、引擎执行和产物传输均在事务之外。
        with self.transaction() as connection:
            connection.execute(FACT_LOCK)
            yield connection

    def initialize(self) -> None:
        from .schema import initialize

        with self.transaction() as connection:
            initialize(connection)

    def check(self) -> None:
        from .schema import check

        with self.transaction() as connection:
            check(connection)

    def close(self) -> None:
        self.engine.dispose()


def _string_uuids(connection: PsycopgConnection, _record: ConnectionPoolEntry) -> None:
    # 只配置本 Engine 的连接，保留 Store 既有的字符串 UUID 契约。
    uuid_type = new_type((2950,), strings.RUNTIME_UUID, lambda value, _cursor: value)
    register_type(uuid_type, connection)
    register_type(new_array_type((2951,), strings.RUNTIME_UUID_ARRAY, uuid_type), connection)
