"""独立执行日志的批量追加，不触发构建事实锁或修改任务状态。"""

from collections.abc import Sequence

from sqlalchemy import func, select

from dbt_metricflow_service import constants as strings
from dbt_metricflow_service.execution.logs import EngineLog

from .entities import RuntimeExecutionLog
from .paging import read_cursor, write_cursor
from .postgres import Database
from .records import LeasedJob, StoredExecutionLog
from .rows import entity_record

EXECUTION_LOG_CURSOR_SCOPE = "build-execution-logs"
EXECUTION_LOG_LOCK_PREFIX = "execution-logs:"
MAX_LOG_ID = 2**63 - 1


class ExecutionLogStore:
    """每批使用独立 Session，成功返回时日志已提交。"""

    def __init__(self, db: Database) -> None:
        self.db = db

    def append(self, job: LeasedJob, phase: str, command: str, entries: Sequence[EngineLog]) -> None:
        with self.db.session() as session:
            # 在序列分配前串行化同一构建的日志事务，避免迟提交批次被前端游标越过。
            # 只锁日志命名空间，既不持有构建行锁，也不获取业务事实计数器锁。
            scope = EXECUTION_LOG_LOCK_PREFIX + (job.request_json.build_id or job.job_id)
            session.execute(select(func.pg_advisory_xact_lock(func.hashtextextended(scope, 0))))
            session.add_all(RuntimeExecutionLog(
                build_id=job.request_json.build_id, job_id=job.job_id, attempt_id=job.attempt_id,
                phase=phase, command=command, event_time=entry.event_time, level=entry.level,
                message=entry.message, event_name=entry.event_name, event_code=entry.event_code,
                invocation_id=entry.invocation_id, node_id=entry.node_id, thread_name=entry.thread_name,
            ) for entry in entries)

    def list(
        self, build_id: str, cursor: str | None = None, limit: int = 100,
    ) -> tuple[list[StoredExecutionLog], str, bool]:
        """按构建有界续读；空页保留位置，避免轮询时从头重复加载。"""
        scope = [EXECUTION_LOG_CURSOR_SCOPE, build_id]
        position = read_cursor(scope, cursor, limit)
        after = 0
        if position is not None:
            if not position.isascii() or not position.isdecimal() or len(position) > 19:
                raise ValueError(strings.INVALID_CURSOR_MESSAGE)
            after = int(position)
            if after > MAX_LOG_ID:
                raise ValueError(strings.INVALID_CURSOR_MESSAGE)
        with self.db.session() as session:
            entities = session.scalars(select(RuntimeExecutionLog).where(
                RuntimeExecutionLog.build_id == build_id, RuntimeExecutionLog.log_id > after,
            ).order_by(RuntimeExecutionLog.log_id).limit(limit + 1)).all()
            rows = [entity_record(row, StoredExecutionLog) for row in entities[:limit]]
        following = write_cursor(scope, str(rows[-1].log_id if rows else after))
        return rows, following, len(entities) > limit
