"""同事务变化日志；没有 callback 丢失窗口或自增提交顺序漏洞。"""

from __future__ import annotations

from pydantic.alias_generators import to_camel
from sqlalchemy import select

from dbt_metricflow_service.storage.postgres import Database

from ..models.builds import ChangePage, ChangeRecord
from .entities import Change

PUBLIC_TYPES = {
    "engine_build": "BUILD",
    "engine_deployment_target": "DEPLOYMENT_TARGET",
    "engine_deployment_attempt": "DEPLOYMENT_ATTEMPT",
}
PRIVATE_FIELDS = frozenset({"run_id", "output_set_id", "catalog_digest", "operation"})


class ChangeStore:
    def __init__(self, db: Database) -> None:
        self.db = db

    def read(self, cursor: str | None = None, limit: int = 50) -> ChangePage:
        # 序号只由持计数器行锁的事实事务分配，读取不使用 MAX(id) 猜测提交水位。
        if not 1 <= limit <= 200 or cursor is not None and (not cursor.isascii() or not cursor.isdecimal()):
            raise ValueError("invalid change cursor or limit")
        after = int(cursor or 0)
        if after > 9223372036854775807:
            raise ValueError("invalid change cursor")
        with self.db.session() as session:
            rows = session.scalars(select(Change).where(Change.sequence > after).order_by(Change.sequence).limit(limit))
            records = [
                ChangeRecord(
                    sequence=row.sequence,
                    repository=row.repository,
                    object_type=PUBLIC_TYPES[row.object_type],
                    object_id=row.object_id,
                    object_version=row.object_version,
                    summary={to_camel(key): value for key, value in row.summary.items() if key not in PRIVATE_FIELDS},
                )
                for row in rows
            ]
        return ChangePage(items=records, next_cursor=str(records[-1].sequence) if records else str(after))
