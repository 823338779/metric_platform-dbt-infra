"""每次 dbt 调用独立的有界日志队列，数据库延迟不进入事件回调。"""

import logging
from collections.abc import Callable
from queue import Empty, Full, Queue
from threading import Lock, Thread
from time import monotonic

from dbt_metricflow_service.execution.logs import EngineLog

QUEUE_SIZE = 2048
BATCH_SIZE = 100
FLUSH_SECONDS = 0.5
THREAD_NAME = "dbt-log-writer"
WRITE_FAILED = "dbt log persistence failed; remaining logs for this command will be dropped: %s"
LOGS_DROPPED = "dbt execution logs dropped: %s"
logger = logging.getLogger(__name__)


class ExecutionLogWriter:
    """短事务批量写入；队列满或写入失败明确告警，不改变引擎结果。"""

    def __init__(self, write: Callable[[list[EngineLog]], None]) -> None:
        self._write = write
        self._queue: Queue[EngineLog | None] = Queue(maxsize=QUEUE_SIZE)
        self._drop_lock = Lock()
        self._dropped = 0
        self.run_in_thread = Thread(target=self._drain, name=THREAD_NAME, daemon=True)
        self.run_in_thread.start()

    def emit(self, entry: EngineLog) -> None:
        """只入队，绝不等待数据库；调用者负责在引擎结束后关闭。"""
        try:
            self._queue.put_nowait(entry)
        except Full:
            self._drop(1)

    def _drop(self, count: int) -> None:
        with self._drop_lock:
            self._dropped += count

    def close(self) -> None:
        """引擎退出后排空队列；运行时必须在线程中等待，保持心跳可用。"""
        self._queue.put(None)
        self.run_in_thread.join()
        if self._dropped:
            logger.warning(LOGS_DROPPED, self._dropped)

    def _drain(self) -> None:
        failed = False
        stopping = False
        batch: list[EngineLog] = []
        deadline = monotonic() + FLUSH_SECONDS
        while not stopping:
            try:
                entry = self._queue.get(timeout=max(0, deadline - monotonic()))
                if entry is None:
                    stopping = True
                else:
                    batch.append(entry)
            except Empty:
                pass
            if len(batch) < BATCH_SIZE and not stopping and monotonic() < deadline:
                continue
            if batch:
                if not failed:
                    try:
                        self._write(batch)
                    except Exception as error:
                        # 不记录异常原文或 SQL 参数，避免绕过引擎脱敏边界。
                        failed = True
                        logger.warning(WRITE_FAILED, type(error).__name__)
                if failed:
                    self._drop(len(batch))
                batch = []
            deadline = monotonic() + FLUSH_SECONDS
