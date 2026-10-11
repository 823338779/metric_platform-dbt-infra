"""短事务连接池；建表仅由管理命令显式执行。"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager

from psycopg2.extensions import connection as PsycopgConnection
from psycopg2.extensions import new_array_type, new_type, parse_dsn, register_type
from sqlalchemy import Connection, create_engine, event, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import ConnectionPoolEntry

from .entities import ChangeCounter

CONNECTION_OPTIONS = "-c statement_timeout=10000 -c lock_timeout=5000"
# ORM 与管理事务共用同一个事实锁表达式，保持触发器的全局加锁顺序。
FACT_LOCK = select(ChangeCounter.value).where(ChangeCounter.singleton.is_(True)).with_for_update()


class Database:
    """每次事务独占连接，连接数和等待线程均受调用方执行器约束。"""

    def __init__(self, dsn: str, max_connections: int = 8) -> None:
        parameters = parse_dsn(dsn)
        parameters["options"] = (parameters.get("options", "") + " " + CONNECTION_OPTIONS).strip()
        parameters["connect_timeout"] = 5
        self.engine = create_engine(
            "postgresql+psycopg2://", connect_args=parameters,
            pool_size=max_connections, max_overflow=0, pool_timeout=5, pool_pre_ping=True,
        )
        event.listen(self.engine, "connect", _string_uuids)
        # 每次 Store 事务独立创建 Session；显式写入避免 autoflush 改变加锁顺序。
        self._sessions = sessionmaker(bind=self.engine, autoflush=False, expire_on_commit=False)

    def transaction(self) -> AbstractContextManager[Connection]:
        return self.engine.begin()

    def session(self) -> AbstractContextManager[Session]:
        """提交成功的短事务，异常时回滚并关闭当前 Session。"""
        return self._sessions.begin()

    @contextmanager
    def fact_session(self) -> Iterator[Session]:
        # ORM 事实写入也先锁变化计数器，保持与数据库触发器相同的锁顺序。
        with self.session() as session:
            session.execute(FACT_LOCK)
            yield session

    @contextmanager
    def fact_transaction(self) -> Iterator[Connection]:
        # 事实事务先锁变化序号，再锁业务行，避免触发器与目标/任务锁顺序反转。
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
    uuid_type = new_type((2950,), "RUNTIME_UUID", lambda value, _cursor: value)
    register_type(uuid_type, connection)
    register_type(new_array_type((2951,), "RUNTIME_UUID_ARRAY", uuid_type), connection)
