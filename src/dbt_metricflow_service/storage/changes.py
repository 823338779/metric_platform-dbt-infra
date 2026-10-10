"""同事务变化日志；没有 callback 丢失窗口或自增提交顺序漏洞。"""

from pydantic.alias_generators import to_camel

from ..models.builds import ChangePage, ChangeRecord

SQL_READ = "SELECT * FROM engine_change WHERE sequence>%s ORDER BY sequence LIMIT %s"
PUBLIC_TYPES = {
    "engine_build": "BUILD",
    "engine_deployment_target": "DEPLOYMENT_TARGET",
    "engine_deployment_attempt": "DEPLOYMENT_ATTEMPT",
}
PRIVATE_FIELDS = frozenset({"run_id", "output_set_id", "catalog_digest", "source_incomplete", "operation"})


class ChangeStore:
    def __init__(self, db):
        self.db = db

    def read(self, cursor=None, limit=50):
        # 序号只由持计数器行锁的事实事务分配，读取不使用 MAX(id) 猜测提交水位。
        if not 1 <= limit <= 200 or cursor is not None and (not cursor.isascii() or not cursor.isdecimal()):
            raise ValueError("invalid change cursor or limit")
        after = int(cursor or 0)
        if after > 9223372036854775807:
            raise ValueError("invalid change cursor")
        with self.db.transaction() as connection:
            rows = [dict(row) for row in connection.exec_driver_sql(SQL_READ, (after, limit)).mappings()]
        records = [
            ChangeRecord(
                sequence=row["sequence"],
                repository=row["repository"],
                object_type=PUBLIC_TYPES[row["object_type"]],
                object_id=row["object_id"],
                object_version=row["object_version"],
                summary={to_camel(key): value for key, value in row["summary"].items() if key not in PRIVATE_FIELDS},
            )
            for row in rows
        ]
        return ChangePage(items=records, next_cursor=str(records[-1].sequence) if records else str(after))
