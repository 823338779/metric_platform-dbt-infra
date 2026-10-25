"""dbt 与 MetricFlow 共用的串行、取消和进程环境保护。"""

from __future__ import annotations

import asyncio
import logging
import os
import threading
from collections.abc import Awaitable, Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime
from typing import TypeVar, cast

from dbt_metricflow_service import constants as strings

from .logs import EngineLog
from .output import REDACTION_MARKER, SECRET_ENVIRONMENT_PREFIX, _RedactingTailBuffer

LOG_SINK_FAILED = "Engine log capture failed: %s"
RUNTIME_PATH_MARKER = "[runtime]"

# 共享线程调度只保留调用方的返回类型，不解释引擎请求或结果。
T = TypeVar("T")

# 锁属于整个服务进程，多个 Runtime 或事件循环也不能同时修改 dbt 全局状态。
ENGINE_LOCK = threading.Lock()
LOCK_POLL_SECONDS = 0.01
UTF8 = strings.UTF8
NEWLINE = "\n"
PROJECT_ENV = strings.DBT_PROJECT_DIR
PROFILES_ENV = strings.DBT_PROFILES_DIR
LOG_LEVEL_ENV = strings.DBT_LOG_LEVEL
LOG_LEVEL_FILE_ENV = strings.DBT_LOG_LEVEL_FILE
NO_LOGGING = strings.DISABLED_LOG_LEVEL
ENGINE_ENVIRONMENT_KEYS = (
    PROJECT_ENV, PROFILES_ENV, strings.DBT_TARGET_PATH, strings.DBT_PLATFORM_SCHEMA, strings.DBT_TARGET,
    strings.DBT_SEND_ANONYMOUS_USAGE_STATS, strings.PYTHONUTF8,
)
ENGINE_LOGGERS = (
    strings.DBT_MODULE,
    strings.DBT_COMMON_MODULE,
    strings.DBT_METRICFLOW_MODULE,
    strings.METRICFLOW_PACKAGE,
    strings.METRICFLOW_SEMANTICS_MODULE,
)


class _EngineLogHandler(logging.Handler):
    """将引擎的普通 Python 日志和异常也送入受限脱敏缓冲区。"""

    def __init__(self, capture: Callable[[EngineLog], None]) -> None:
        super().__init__()
        # 与 dbt 事件共用同一输出边界，不向宿主 root logger 传播原文。
        self.capture = capture

    def emit(self, record: logging.LogRecord) -> None:
        self.capture(EngineLog(
            event_time=datetime.fromtimestamp(record.created, UTC), level=record.levelname.lower(),
            message=self.format(record), event_name=record.name, thread_name=record.threadName or "",
        ))


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
    before_start: Callable[[], Awaitable[None]],
) -> T:
    """获得串行槽位后校验租约，返回调用结果；取消也须等线程退出。"""
    # 等待锁时保持可取消，不占用线程池或阻塞服务心跳。
    while not ENGINE_LOCK.acquire(blocking=False):
        await asyncio.sleep(LOCK_POLL_SECONDS)
    try:
        await before_start()
        task = asyncio.create_task(asyncio.to_thread(invoke))
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            try:
                await _settle(task)
            except Exception:
                # 引擎已在执行上下文中记录错误；迟到错误不覆盖调用方取消。
                pass
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
    log_sink: Callable[[EngineLog], None] | None = None,
) -> Iterator[Callable[[EngineLog], None]]:
    """隔离引擎全局状态并提供脱敏日志入口；调用方必须已经持有串行锁。"""
    from dbt.adapters.factory import reset_adapters
    from dbt.flags import get_flags, set_flags
    from dbt_common.events.event_manager import EventManager, IEventManager
    from dbt_common.events.event_manager_client import ctx_set_event_manager, get_event_manager

    original_environment = dict(os.environ)
    original_flags = get_flags()
    original_events = get_event_manager()
    event_lock = threading.Lock()
    secrets = {value for name, value in os.environ.items() if name.startswith(SECRET_ENVIRONMENT_PREFIX) and value}
    runtime_paths = {
        spelling
        for name in (PROJECT_ENV, PROFILES_ENV, strings.DBT_TARGET_PATH)
        if (path := environment.get(name))
        for spelling in (path, path.replace("\\", "/"), path.replace("\\", "\\\\"))
    }
    sink_failed = False

    def capture_text(entry: EngineLog) -> None:
        nonlocal sink_failed
        # SDK 读取 dotenv 后可能新增秘密；整条事件先遮盖，再按字节截断。
        with event_lock:
            secrets.update(value for name, value in os.environ.items()
                           if name.startswith(SECRET_ENVIRONMENT_PREFIX) and value)

            def redact(value: str) -> str:
                for path in sorted(runtime_paths, key=len, reverse=True):
                    value = value.replace(path, RUNTIME_PATH_MARKER)
                for secret in sorted(secrets, key=len, reverse=True):
                    value = value.replace(secret, REDACTION_MARKER)
                return value

            safe = replace(
                entry, message=redact(entry.message), level=redact(entry.level),
                event_name=redact(entry.event_name), event_code=redact(entry.event_code),
                invocation_id=redact(entry.invocation_id), node_id=redact(entry.node_id),
                thread_name=redact(entry.thread_name),
            )
            output.feed((safe.message + NEWLINE).encode(UTF8))
            if log_sink is not None and not sink_failed:
                try:
                    log_sink(safe)
                except Exception as error:
                    # 日志接收失败不影响引擎结果，也不暴露数据库或异常原文。
                    sink_failed = True
                    logging.getLogger(__name__).warning(LOG_SINK_FAILED, type(error).__name__)

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
