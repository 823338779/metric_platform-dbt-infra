from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from uuid import UUID, uuid4


class RunState(StrEnum):
    """平台任务的持久生命周期。"""

    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    PREPARING = "PREPARING"
    BUILDING = "BUILDING"
    VALIDATING = "VALIDATING"
    READY = "READY"
    FAILED = "FAILED"
    CLEANING = "CLEANING"
    CLEANED = "CLEANED"


@dataclass(frozen=True, slots=True)
class RunRecord:
    """不含凭据的运行任务索引。"""

    run_id: UUID
    idempotency_key: str
    fingerprint: str
    state: RunState
    artifact_path: Path | None
    error_code: str | None


class PlatformJobStore:
    """SQLite 唯一约束保证并发和重启后幂等键仍只对应一个任务。"""

    _TABLES = {"run": "platform_runs", "query": "platform_queries"}

    def __init__(self, db_path: Path) -> None:
        # 初始化索引并恢复不确定的运行状态，不重放可能已经写入数据库的任务。
        self.db_path = db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            for table in self._TABLES.values():
                query_parent = ", parent_run_id TEXT" if table == "platform_queries" else ""
                connection.execute(f"""CREATE TABLE IF NOT EXISTS {table} (
                    id TEXT PRIMARY KEY,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    fingerprint TEXT NOT NULL,
                    state TEXT NOT NULL,
                    artifact_path TEXT,
                    error_code TEXT{query_parent}
                )""")
                if table == "platform_queries":
                    columns = {row[1] for row in connection.execute("PRAGMA table_info(platform_queries)")}
                    if "parent_run_id" not in columns:
                        connection.execute("ALTER TABLE platform_queries ADD COLUMN parent_run_id TEXT")
                connection.execute(
                    f"UPDATE {table} SET state = ?, error_code = ? WHERE state = ?",
                    (RunState.FAILED, "INTERRUPTED", RunState.RUNNING),
                )
                connection.execute(
                    f"UPDATE {table} SET state = ?, error_code = ? WHERE state IN (?, ?, ?)",
                    (RunState.FAILED, "INTERRUPTED", RunState.PREPARING, RunState.BUILDING, RunState.VALIDATING),
                )
                if table == "platform_runs":
                    connection.execute(
                        "UPDATE platform_runs SET state = ?, error_code = ? WHERE state = ?",
                        (RunState.FAILED, "INTERRUPTED_CLEANUP", RunState.CLEANING),
                    )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path, timeout=30)
        connection.execute("PRAGMA busy_timeout = 30000")
        return connection

    def _reserve(self, kind: str, idempotency_key: str, fingerprint: str) -> UUID:
        if not idempotency_key or not fingerprint:
            raise ValueError("幂等键和请求指纹不能为空")
        table = self._TABLES[kind]
        candidate = uuid4()
        # 单条 INSERT OR IGNORE 在数据库唯一约束下竞争；后读记录核对指纹。
        with self._connect() as connection:
            connection.execute(
                f"INSERT OR IGNORE INTO {table} (id, idempotency_key, fingerprint, state) VALUES (?, ?, ?, ?)",
                (str(candidate), idempotency_key, fingerprint, RunState.QUEUED),
            )
            row = connection.execute(
                f"SELECT id, fingerprint FROM {table} WHERE idempotency_key = ?", (idempotency_key,)
            ).fetchone()
        if row is None or row[1] != fingerprint:
            raise ValueError("幂等键对应的请求内容不同")
        return UUID(row[0])

    def reserve_run(self, idempotency_key: str, request_fingerprint: str) -> UUID:
        return self._reserve("run", idempotency_key, request_fingerprint)

    def reserve_query(self, idempotency_key: str, request_fingerprint: str, run_id: UUID) -> UUID:
        """查询受理和清理认领使用同一个写事务，避免与 schema 删除交错。"""

        candidate = uuid4()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            state = connection.execute("SELECT state FROM platform_runs WHERE id = ?", (str(run_id),)).fetchone()
            if state is None or state[0] != RunState.READY:
                raise ValueError("固定版本尚不可查询")
            connection.execute(
                "INSERT OR IGNORE INTO platform_queries "
                "(id, idempotency_key, fingerprint, state, parent_run_id) VALUES (?, ?, ?, ?, ?)",
                (str(candidate), idempotency_key, request_fingerprint, RunState.QUEUED, str(run_id)),
            )
            row = connection.execute(
                "SELECT id, fingerprint, parent_run_id FROM platform_queries WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
        if row is None or row[1] != request_fingerprint or row[2] != str(run_id):
            raise ValueError("幂等键对应的请求内容不同")
        return UUID(row[0])

    def claim_cleanup(self, run_id: UUID) -> bool:
        """持有 SQLite 写锁检查活动查询并认领清理，重复 CLEANED 返回 False。"""

        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT state FROM platform_runs WHERE id = ?", (str(run_id),)).fetchone()
            if row is None:
                raise KeyError(run_id)
            if row[0] == RunState.CLEANED:
                return False
            if row[0] == RunState.CLEANING:
                return False
            if row[0] not in {RunState.READY, RunState.FAILED}:
                raise ValueError("运行任务尚不可清理")
            active = connection.execute(
                "SELECT 1 FROM platform_queries WHERE parent_run_id = ? AND state IN (?, ?) LIMIT 1",
                (str(run_id), RunState.QUEUED, RunState.RUNNING),
            ).fetchone()
            if active:
                raise ValueError("运行任务有活动查询")
            connection.execute(
                "UPDATE platform_runs SET state = ? WHERE id = ?", (RunState.CLEANING, str(run_id))
            )
        return True

    def claim_run(self, run_id: UUID, artifact_path: Path) -> bool:
        """仅一个提交者可从 QUEUED 抢占构建；重复请求只读取现有任务。"""

        with self._connect() as connection:
            cursor = connection.execute(
                "UPDATE platform_runs SET state = ?, artifact_path = ? WHERE id = ? AND state = ?",
                (RunState.PREPARING, str(artifact_path), str(run_id), RunState.QUEUED),
            )
        return cursor.rowcount == 1

    def claim_query(self, query_id: UUID, artifact_path: Path) -> bool:
        with self._connect() as connection:
            cursor = connection.execute(
                "UPDATE platform_queries SET state = ?, artifact_path = ? WHERE id = ? AND state = ?",
                (RunState.RUNNING, str(artifact_path), str(query_id), RunState.QUEUED),
            )
        return cursor.rowcount == 1

    def _find(self, kind: str, column: str, value: str) -> RunRecord | None:
        table = self._TABLES[kind]
        with self._connect() as connection:
            row = connection.execute(
                f"SELECT id, idempotency_key, fingerprint, state, artifact_path, error_code "
                f"FROM {table} WHERE {column} = ?",
                (value,),
            ).fetchone()
        if row is None:
            return None
        return RunRecord(UUID(row[0]), row[1], row[2], RunState(row[3]), Path(row[4]) if row[4] else None, row[5])

    def find_run(self, run_id: UUID) -> RunRecord | None:
        return self._find("run", "id", str(run_id))

    def find_run_by_key(self, key: str) -> RunRecord | None:
        return self._find("run", "idempotency_key", key)

    def find_query(self, query_id: UUID) -> RunRecord | None:
        return self._find("query", "id", str(query_id))

    def find_query_by_key(self, key: str) -> RunRecord | None:
        return self._find("query", "idempotency_key", key)

    def _transition(
        self, kind: str, item_id: UUID, state: RunState,
        artifact_path: Path | None = None, error_code: str | None = None,
    ) -> RunRecord:
        table = self._TABLES[kind]
        if state is RunState.READY and (artifact_path is None or not (artifact_path / "READY").is_file()):
            raise ValueError("READY 状态缺少已落盘标记")
        with self._connect() as connection:
            cursor = connection.execute(
                f"UPDATE {table} SET state = ?, artifact_path = COALESCE(?, artifact_path), "
                "error_code = ? WHERE id = ?",
                (state, str(artifact_path) if artifact_path else None, error_code, str(item_id)),
            )
        if cursor.rowcount != 1:
            raise KeyError(item_id)
        record = self._find(kind, "id", str(item_id))
        assert record is not None
        return record

    def transition_run(
        self, run_id: UUID, state: RunState,
        artifact_path: Path | None = None, error_code: str | None = None,
    ) -> RunRecord:
        return self._transition("run", run_id, state, artifact_path, error_code)

    def transition_query(
        self, query_id: UUID, state: RunState,
        artifact_path: Path | None = None, error_code: str | None = None,
    ) -> RunRecord:
        return self._transition("query", query_id, state, artifact_path, error_code)
