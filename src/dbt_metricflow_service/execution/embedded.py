"""dbt 与 MetricFlow 共用的串行、取消和进程环境保护。"""

from __future__ import annotations

import asyncio
import logging
import os
import threading
from collections.abc import Awaitable, Callable, Iterator, Mapping
from contextlib import contextmanager
from typing import TypeVar, cast

from .runner import REDACTION_MARKER, SECRET_ENVIRONMENT_PREFIX, _RedactingTailBuffer

# 共享线程调度只保留调用方的返回类型，不解释引擎请求或结果。
T = TypeVar("T")

# 锁属于整个服务进程，多个 Runtime 或事件循环也不能同时修改 dbt 全局状态。
ENGINE_LOCK = threading.Lock()
LOCK_POLL_SECONDS = 0.01
UTF8 = "utf-8"
NEWLINE = "\n"
PROJECT_ENV = "DBT_PROJECT_DIR"
PROFILES_ENV = "DBT_PROFILES_DIR"
LOG_LEVEL_ENV = "DBT_LOG_LEVEL"
LOG_LEVEL_FILE_ENV = "DBT_LOG_LEVEL_FILE"
NO_LOGGING = "none"
ENGINE_ENVIRONMENT_KEYS = (
    PROJECT_ENV, PROFILES_ENV, "DBT_TARGET_PATH", "DBT_PLATFORM_SCHEMA", "DBT_TARGET",
    "DBT_SEND_ANONYMOUS_USAGE_STATS", "PYTHONUTF8",
)
ENGINE_LOGGERS = ("dbt", "dbt_common", "dbt_metricflow", "metricflow", "metricflow_semantics")


class _EngineLogHandler(logging.Handler):
    """将引擎的普通 Python 日志和异常也送入受限脱敏缓冲区。"""

    def __init__(self, capture: Callable[[str], None]) -> None:
        super().__init__()
        # 与 dbt 事件共用同一输出边界，不向宿主 root logger 传播原文。
        self.capture = capture

    def emit(self, record: logging.LogRecord) -> None:
        self.capture(self.format(record))


async def _settle(task: asyncio.Task[T]) -> T:
    # 重复取消不能越过线程退出屏障，避免释放锁或删除仍被使用的工作目录。
    while True:
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            if task.done():
                return task.result()


async def run_serialized(
    invoke: Callable[[], T],
    timeout_seconds: float,
    before_start: Callable[[], Awaitable[None]],
) -> tuple[T, bool]:
    """获得串行槽位后校验租约，返回调用结果及是否超时；取消也须等线程退出。"""
    # 等待锁时保持可取消，不占用线程池或阻塞服务心跳。
    while not ENGINE_LOCK.acquire(blocking=False):
        await asyncio.sleep(LOCK_POLL_SECONDS)
    try:
        await before_start()
        task = asyncio.create_task(asyncio.to_thread(invoke))
        try:
            return await asyncio.wait_for(asyncio.shield(task), timeout_seconds), False
        except TimeoutError:
            return await _settle(task), True
        except asyncio.CancelledError:
            await _settle(task)
            raise
    finally:
        ENGINE_LOCK.release()


def create_output(max_output_bytes: int) -> _RedactingTailBuffer:
    """为一次引擎调用创建有界且已登记环境秘密的日志缓冲区。"""
    secrets = [value for name, value in os.environ.items() if name.startswith(SECRET_ENVIRONMENT_PREFIX) and value]
    return _RedactingTailBuffer(max_output_bytes, secrets)


@contextmanager
def engine_environment(
    environment: Mapping[str, str], output: _RedactingTailBuffer,
) -> Iterator[Callable[[str], None]]:
    """隔离引擎全局状态并提供脱敏日志入口；调用方必须已经持有串行锁。"""
    from dbt.adapters.factory import reset_adapters
    from dbt.flags import get_flags, set_flags
    from dbt_common.events.event_manager import EventManager, IEventManager
    from dbt_common.events.event_manager_client import ctx_set_event_manager, get_event_manager

    original_environment = dict(os.environ)
    original_flags = get_flags()
    original_events = get_event_manager()
    event_lock = threading.Lock()

    def capture_text(message: str) -> None:
        # SDK 读取 dotenv 后可能新增秘密；整条事件先遮盖，再按字节截断。
        for secret in sorted(
            (value for name, value in os.environ.items() if name.startswith(SECRET_ENVIRONMENT_PREFIX) and value),
            key=len, reverse=True,
        ):
            message = message.replace(secret, REDACTION_MARKER)
        with event_lock:
            output.feed((message + NEWLINE).encode(UTF8))

    handler = _EngineLogHandler(capture_text)
    logger_states: list[tuple[logging.Logger, list[logging.Handler], int, bool]] = []

    try:
        # 使用显式工程路径，不修改进程 cwd；只覆盖引擎所需环境项。
        for name in ENGINE_ENVIRONMENT_KEYS:
            if name in environment:
                os.environ[name] = environment[name]
        os.environ[LOG_LEVEL_ENV] = NO_LOGGING
        os.environ[LOG_LEVEL_FILE_ENV] = NO_LOGGING
        # 仅接管引擎命名空间，保持 HTTP/worker 等宿主日志配置不变。
        for name in ENGINE_LOGGERS:
            logger = logging.getLogger(name)
            logger_states.append((logger, logger.handlers[:], logger.level, logger.propagate))
            logger.handlers = [handler]
            logger.setLevel(logging.INFO)
            logger.propagate = False
        # 上游实际实现满足管理器接口，其 Protocol 的可变属性声明与 property 不一致。
        ctx_set_event_manager(cast(IEventManager, EventManager()))
        reset_adapters()
        yield capture_text
    finally:
        try:
            reset_adapters()
        finally:
            ctx_set_event_manager(original_events)
            set_flags(original_flags)
            for logger, handlers, level, propagate in logger_states:
                logger.handlers = handlers
                logger.setLevel(level)
                logger.propagate = propagate
            handler.close()
            # SDK 也可能读取 dotenv 并追加环境项；退出前恢复调用前的完整环境。
            for name in set(os.environ) - original_environment.keys():
                del os.environ[name]
            os.environ.update(original_environment)
