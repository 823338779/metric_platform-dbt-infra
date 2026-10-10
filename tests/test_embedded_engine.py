"""进程内引擎调用的串行、取消和环境隔离契约。"""

import asyncio
import json
import logging
import os
import sys
import threading
from dataclasses import replace
from types import SimpleNamespace

import pytest
from dbt.cli.main import dbtRunner

from dbt_metricflow_service.execution.embedded import run_embedded
from dbt_metricflow_service.execution.models import CommandSpec, JobStatus
from dbt_metricflow_service.runtime.executor import RuntimeExecutor

DBT = "dbt"
BUILD = "build"
PROJECT = "embedded-test"
SCHEMA_KEY = "DBT_PLATFORM_SCHEMA"
SCHEMA = "run_embedded"
SECRET_KEY = "DBT_ENV_SECRET_EMBEDDED"
SECRET = "embedded-private-value"
MODULE = "dbt_metricflow_service.platform.metricflow"
UTF8 = "utf-8"
ENGINE_LOGGER = "dbt_metricflow.cli.dbt_connectors.adapter_backed_client"
UNSUPPORTED_COMMAND = "unsupported-tool"


async def before_start():
    pass


def command(tmp_path):
    return CommandSpec((DBT, BUILD), tmp_path, {**os.environ, SCHEMA_KEY: SCHEMA}, True)


async def wait_started(event):
    async with asyncio.timeout(5):
        while not event.is_set():
            await asyncio.sleep(0.01)


async def test_dbt_runs_in_same_process_and_redacts_events(tmp_path, monkeypatch):
    # SDK 回调的输出仍受脱敏和字节上限约束，调用结束后恢复环境。
    monkeypatch.setenv(SECRET_KEY, SECRET)
    old_schema = os.environ.get(SCHEMA_KEY)
    invoking_pid = os.getpid()

    def invoke(self, args):
        assert os.getpid() == invoking_pid
        assert args == [BUILD]
        assert os.environ[SCHEMA_KEY] == SCHEMA
        for callback in self.callbacks:
            callback(SimpleNamespace(info=SimpleNamespace(msg=SECRET + "x" * 1000)))
        return SimpleNamespace(success=True, exception=None)

    monkeypatch.setattr(dbtRunner, "invoke", invoke)
    result = await run_embedded(PROJECT, command(tmp_path), 5, 64, before_start)
    assert result.status == JobStatus.SUCCEEDED
    assert SECRET not in result.stdout
    assert len(result.stdout.encode()) <= 64
    assert result.output_truncated
    assert os.environ.get(SCHEMA_KEY) == old_schema


async def test_engine_python_logs_are_redacted_before_host_handlers(tmp_path, monkeypatch, caplog):
    # Python logging 也必须经过脱敏，不能因取消子进程边界而泄露到服务日志。
    monkeypatch.setenv(SECRET_KEY, SECRET)
    logger = logging.getLogger(ENGINE_LOGGER)

    def invoke(self, args):
        logger.error(SECRET)
        return SimpleNamespace(success=True, exception=None)

    monkeypatch.setattr(dbtRunner, "invoke", invoke)
    result = await run_embedded(PROJECT, command(tmp_path), 5, 1024, before_start)
    assert SECRET not in caplog.text
    assert SECRET not in result.stdout
    assert "***" in result.stdout


async def test_sdk_failure_restores_environment_and_logging(tmp_path, monkeypatch):
    # 失败分支也必须撤销当前工程的全局配置，后续调用可以正常取得串行锁。
    logger = logging.getLogger("dbt_metricflow")
    original = (logger.handlers[:], logger.level, logger.propagate)
    environment = dict(os.environ)

    def invoke(self, args):
        raise RuntimeError(SECRET)

    monkeypatch.setattr(dbtRunner, "invoke", invoke)
    result = await run_embedded(PROJECT, command(tmp_path), 5, 64, before_start)
    assert result.status == JobStatus.FAILED
    assert result.stderr == RuntimeError.__name__
    assert SECRET not in result.stdout
    assert dict(os.environ) == environment
    assert (logger.handlers, logger.level, logger.propagate) == original


async def test_rejected_lease_never_invokes_sdk(tmp_path, monkeypatch):
    # 串行等待后的租约检查失败时不得进入 SDK，也不能把锁留给已拒绝任务。
    calls = []
    monkeypatch.setattr(dbtRunner, "invoke", lambda *_: calls.append(True))

    async def expired():
        raise RuntimeError("expired lease")

    with pytest.raises(RuntimeError, match="expired lease"):
        await run_embedded(PROJECT, command(tmp_path), 5, 64, expired)
    assert calls == []


@pytest.mark.parametrize("cancel", [False, True])
async def test_timeout_or_cancel_keeps_slot_until_call_exits(tmp_path, monkeypatch, cancel):
    # 取消/超时不能提前释放全局锁，后续调用也不能提前执行租约检查。
    started, release = threading.Event(), threading.Event()
    entered = []
    checked = []

    def invoke(self, args):
        entered.append(len(entered))
        if len(entered) == 1:
            started.set()
            assert release.wait(5)
        return SimpleNamespace(success=True, exception=None)

    async def second_check():
        checked.append(True)

    monkeypatch.setattr(dbtRunner, "invoke", invoke)
    first = asyncio.create_task(run_embedded(PROJECT, command(tmp_path), 5 if cancel else 0.03, 64, before_start))
    await wait_started(started)
    second = asyncio.create_task(run_embedded(PROJECT, command(tmp_path), 5, 64, second_check))
    try:
        if cancel:
            first.cancel()
            await asyncio.sleep(0)
            first.cancel()
        await asyncio.sleep(0.07)
        assert not first.done()
        assert entered == [0]
        assert checked == []
    finally:
        release.set()
        results = await asyncio.gather(first, second, return_exceptions=True)
    if cancel:
        assert isinstance(results[0], asyncio.CancelledError)
    else:
        assert results[0].status == JobStatus.TIMED_OUT
    assert results[1].status == JobStatus.SUCCEEDED
    assert checked == [True]


async def test_executor_calls_dbt_without_subprocess(tmp_path, monkeypatch):
    # 在运行时边界禁止调用旧 runner，防止新增封装没有接入真实执行路径。
    def forbidden(*args, **kwargs):
        raise AssertionError("dbt must execute in process")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", forbidden)
    monkeypatch.setattr(dbtRunner, "invoke", lambda *_: SimpleNamespace(success=True, exception=None))
    settings = SimpleNamespace(command_timeout_seconds=5, max_output_bytes=64)
    jobs = SimpleNamespace(phase=lambda *args, **kwargs: True)
    executor = RuntimeExecutor(settings, jobs, None)
    record = await executor._command(
        {"job_id": PROJECT, "lease_token": PROJECT, "kind": "BUILD_RUN", "project_id": PROJECT},
        command(tmp_path), "BUILDING",
    )
    assert record.status == JobStatus.SUCCEEDED


async def test_unsupported_command_is_rejected_before_authorization(tmp_path):
    # 统一执行入口只接受引擎调用，未知入口不能被当作 MetricFlow 或外部命令执行。
    checked = []

    async def authorize():
        checked.append(True)

    spec = replace(command(tmp_path), argv=(UNSUPPORTED_COMMAND,))
    with pytest.raises(ValueError, match="unsupported engine command"):
        await run_embedded(PROJECT, spec, 5, 64, authorize)
    assert checked == []


async def test_metricflow_calls_python_function_without_child(tmp_path, monkeypatch):
    import dbt_metricflow_service.platform.metricflow as module

    input_path, output_path = tmp_path / "input.json", tmp_path / "output.json"
    payload = {"mode": "PROBE"}
    input_path.write_text(json.dumps(payload), encoding=UTF8)

    def execute(project, profiles, data):
        assert os.getpid() == calling_pid
        assert data == payload
        return {"queryCapability": True}

    calling_pid = os.getpid()
    monkeypatch.setattr(module, "execute_programmatic", execute)
    spec = CommandSpec(
        (sys.executable, "-m", MODULE, str(input_path), str(output_path)), tmp_path,
        {**os.environ, "DBT_PROJECT_DIR": str(tmp_path), "DBT_PROFILES_DIR": str(tmp_path)}, False,
    )
    result = await run_embedded(PROJECT, spec, 5, 64, before_start)
    assert result.status == JobStatus.SUCCEEDED
    assert json.loads(output_path.read_text(encoding=UTF8)) == {"queryCapability": True}
