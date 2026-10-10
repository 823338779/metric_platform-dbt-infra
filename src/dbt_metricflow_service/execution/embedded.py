"""串行调用 dbt/MetricFlow SDK；取消和超时必须等待执行线程退出。"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import threading
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, cast
from uuid import uuid4

from .models import CommandSpec, JobRecord, JobStatus
from .runner import REDACTION_MARKER, SECRET_ENVIRONMENT_PREFIX, _RedactingTailBuffer

if TYPE_CHECKING:
    from dbt_common.events.base_types import EventMsg

# 锁属于整个服务进程，多个 Runtime 或事件循环也不能同时修改 dbt 全局状态。
ENGINE_LOCK = threading.Lock()
LOCK_POLL_SECONDS = 0.01
DBT = "dbt"
MODULE_OPTION = "-m"
METRICFLOW_MODULE = "dbt_metricflow_service.platform.metricflow"
UTF8 = "utf-8"
NEWLINE = "\n"
PROJECT_ENV = "DBT_PROJECT_DIR"
PROFILES_ENV = "DBT_PROFILES_DIR"
LOG_LEVEL_ENV = "DBT_LOG_LEVEL"
LOG_LEVEL_FILE_ENV = "DBT_LOG_LEVEL_FILE"
NO_LOGGING = "none"
EVENT_MESSAGE = "msg"
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


async def _settle(task: asyncio.Task[JobRecord]) -> JobRecord:
    # 重复取消不能越过线程退出屏障，避免释放锁或删除仍被使用的工作目录。
    while True:
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            if task.done():
                return task.result()


async def run_embedded(
    project: str,
    command: CommandSpec,
    timeout_seconds: float,
    max_output_bytes: int,
    before_start: Callable[[], Awaitable[None]],
) -> JobRecord:
    """获得串行槽位后再次校验租约；超时只标记结果，不强制终止线程。"""
    # 未知入口在授权和引擎初始化前拒绝，不再回退到外部命令执行。
    if command.argv[:1] != (DBT,) and command.argv[:3] != (sys.executable, MODULE_OPTION, METRICFLOW_MODULE):
        raise ValueError("unsupported engine command")
    # 等待锁时保持可取消，不占用线程池或阻塞服务心跳。
    while not ENGINE_LOCK.acquire(blocking=False):
        await asyncio.sleep(LOCK_POLL_SECONDS)
    try:
        await before_start()
        task = asyncio.create_task(asyncio.to_thread(_invoke, project, command, max_output_bytes))
        try:
            return await asyncio.wait_for(asyncio.shield(task), timeout_seconds)
        except TimeoutError:
            record = await _settle(task)
            return record.model_copy(update={"status": JobStatus.TIMED_OUT, "exit_code": None})
        except asyncio.CancelledError:
            await _settle(task)
            raise
    finally:
        ENGINE_LOCK.release()


def _invoke(project: str, command: CommandSpec, max_output_bytes: int) -> JobRecord:
    from dbt.adapters.factory import reset_adapters
    from dbt.flags import get_flags, set_flags
    from dbt_common.events.event_manager import EventManager, IEventManager
    from dbt_common.events.event_manager_client import ctx_set_event_manager, get_event_manager

    started = datetime.now(UTC)
    original_environment = dict(os.environ)
    original_flags = get_flags()
    original_events = get_event_manager()
    secrets = [
        value for name, value in original_environment.items()
        if name.startswith(SECRET_ENVIRONMENT_PREFIX) and value
    ]
    output = _RedactingTailBuffer(max_output_bytes, secrets)
    event_lock = threading.Lock()
    exit_code = 2
    error_type = ""

    def capture_text(message: str) -> None:
        # SDK 读取 dotenv 后可能新增秘密；整条事件先遮盖，再按字节截断。
        for secret in sorted(
            (value for name, value in os.environ.items() if name.startswith(SECRET_ENVIRONMENT_PREFIX) and value),
            key=len, reverse=True,
        ):
            message = message.replace(secret, REDACTION_MARKER)
        with event_lock:
            output.feed((message + NEWLINE).encode(UTF8))

    def capture(event: EventMsg) -> None:
        # 上游 EventInfo Protocol 未声明 protobuf 的 msg 字段。
        capture_text(str(getattr(event.info, EVENT_MESSAGE)))

    handler = _EngineLogHandler(capture_text)
    logger_states: list[tuple[logging.Logger, list[logging.Handler], int, bool]] = []

    try:
        # 使用显式工程路径，不修改进程 cwd；只覆盖引擎所需环境项。
        for name in ENGINE_ENVIRONMENT_KEYS:
            if name in command.environment:
                os.environ[name] = command.environment[name]
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
        if command.argv[0] == DBT:
            from dbt.cli.main import dbtRunner

            result = dbtRunner(callbacks=[capture]).invoke(list(command.argv[1:]))
            exit_code = 0 if result.success else (2 if result.exception is not None else 1)
            if result.exception is not None:
                error_type = type(result.exception).__name__
        else:
            from ..platform.metricflow import INVALID_OPTIONS_CODE, InvalidOptions, execute_programmatic

            # 复用现有结构化载荷边界；调用本身不再经过 Python CLI 子进程。
            input_path, output_path = map(Path, command.argv[-2:])
            payload = json.loads(input_path.read_text(encoding=UTF8))
            try:
                value = execute_programmatic(
                    Path(command.environment[PROJECT_ENV]), Path(command.environment[PROFILES_ENV]), payload,
                )
            except InvalidOptions:
                output_path.write_text(json.dumps({"errorCode": INVALID_OPTIONS_CODE}), encoding=UTF8)
            else:
                output_path.write_text(json.dumps(value, ensure_ascii=False), encoding=UTF8)
                exit_code = 0
    except Exception as error:
        # 任意异常文本可能包含连接信息；只公开异常类型，详细 dbt 事件走脱敏回调。
        error_type = type(error).__name__
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
            output.finish()
    return JobRecord(
        id=uuid4(), project=project,
        status=JobStatus.SUCCEEDED if exit_code == 0 else JobStatus.FAILED,
        submitted_at=started, started_at=started, finished_at=datetime.now(UTC),
        exit_code=exit_code, stdout=output.decode(), stderr=error_type,
        output_truncated=output.truncated,
    )
