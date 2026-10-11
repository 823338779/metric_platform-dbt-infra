"""进程内引擎调用的串行、取消和环境隔离契约。"""

import asyncio
import logging
import os
import threading
from dataclasses import replace
from types import SimpleNamespace

import pytest
from dbt.cli.main import dbtRunner

from dbt_metricflow_service.execution.dbt import run_dbt
from dbt_metricflow_service.execution.metricflow import MetricFlowRequest, run_metricflow
from dbt_metricflow_service.execution.models import CommandSpec, JobStatus
from dbt_metricflow_service.runtime.executor import ExecutionError, RuntimeExecutor

DBT = "dbt"
BUILD = "build"
PROJECT = "embedded-test"
SCHEMA_KEY = "DBT_PLATFORM_SCHEMA"
SCHEMA = "run_embedded"
SECRET_KEY = "DBT_ENV_SECRET_EMBEDDED"
SECRET = "embedded-private-value"
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
    result = await run_dbt(PROJECT, command(tmp_path), 5, 64, before_start)
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
    result = await run_dbt(PROJECT, command(tmp_path), 5, 1024, before_start)
    assert SECRET not in caplog.text
    assert SECRET not in result.stdout
    assert "***" in result.stdout


@pytest.mark.parametrize("metricflow", [False, True])
async def test_sdk_failure_restores_environment_and_logging(tmp_path, monkeypatch, metricflow):
    import dbt_metricflow_service.platform.metricflow as module

    # 失败分支也必须撤销当前工程的全局配置，后续调用可以正常取得串行锁。
    logger = logging.getLogger("dbt_metricflow")
    original = (logger.handlers[:], logger.level, logger.propagate)
    environment = dict(os.environ)

    def invoke(self, args):
        os.environ[SECRET_KEY] = SECRET
        raise RuntimeError(SECRET)

    monkeypatch.setattr(dbtRunner, "invoke", invoke)
    if metricflow:
        monkeypatch.setattr(module, "execute_programmatic", lambda *args: invoke(None, []))
        request = MetricFlowRequest(
            {"DBT_PROJECT_DIR": str(tmp_path), "DBT_PROFILES_DIR": str(tmp_path)}, {"mode": "PROBE"},
        )
        result = (await run_metricflow(PROJECT, request, 5, 64, before_start)).record
        assert result.exit_code is None
    else:
        result = await run_dbt(PROJECT, command(tmp_path), 5, 64, before_start)
    assert result.status == JobStatus.FAILED
    assert result.stderr == RuntimeError.__name__
    assert SECRET not in result.stdout
    assert dict(os.environ) == environment
    assert (logger.handlers, logger.level, logger.propagate) == original


@pytest.mark.parametrize("metricflow", [False, True])
async def test_rejected_lease_never_invokes_sdk(tmp_path, monkeypatch, metricflow):
    import dbt_metricflow_service.platform.metricflow as module

    # 串行等待后的租约检查失败时不得进入 SDK，也不能把锁留给已拒绝任务。
    calls = []
    monkeypatch.setattr(dbtRunner, "invoke", lambda *_: calls.append(True))
    monkeypatch.setattr(module, "execute_programmatic", lambda *_: calls.append(True))
    spec = MetricFlowRequest({}, {"mode": "PROBE"}) if metricflow else command(tmp_path)
    run = run_metricflow if metricflow else run_dbt

    async def expired():
        raise RuntimeError("expired lease")

    with pytest.raises(RuntimeError, match="expired lease"):
        await run(PROJECT, spec, 5, 64, expired)
    assert calls == []


@pytest.mark.parametrize("cancel", [False, True])
@pytest.mark.parametrize("metricflow", [False, True])
async def test_timeout_or_cancel_keeps_slot_until_call_exits(tmp_path, monkeypatch, cancel, metricflow):
    import dbt_metricflow_service.platform.metricflow as module

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
    spec = command(tmp_path)
    if metricflow:
        def execute(project, profiles, data):
            invoke(None, [])
            return {"queryCapability": True}

        monkeypatch.setattr(module, "execute_programmatic", execute)
        spec = MetricFlowRequest(
            {"DBT_PROJECT_DIR": str(tmp_path), "DBT_PROFILES_DIR": str(tmp_path)}, {"mode": "PROBE"},
        )
    run = run_metricflow if metricflow else run_dbt
    first = asyncio.create_task(run(PROJECT, spec, 5 if cancel else 0.03, 64, before_start))
    await wait_started(started)
    second = asyncio.create_task(run_dbt(PROJECT, command(tmp_path), 5, 64, second_check))
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
        record = results[0].record if metricflow else results[0]
        assert record.status == JobStatus.TIMED_OUT
        if metricflow:
            assert results[0].payload is None
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
    result = await executor._dbt(
        {"job_id": PROJECT, "lease_token": PROJECT, "kind": "BUILD_RUN", "project_id": PROJECT},
        command(tmp_path), "BUILDING",
    )
    assert result.status == JobStatus.SUCCEEDED


async def test_unsupported_command_is_rejected_before_authorization(tmp_path):
    # dbt 专用入口在授权之前拒绝其他命令。
    checked = []

    async def authorize():
        checked.append(True)

    spec = replace(command(tmp_path), argv=(UNSUPPORTED_COMMAND,))
    with pytest.raises(ValueError, match="unsupported engine command"):
        await run_dbt(PROJECT, spec, 5, 64, authorize)
    assert checked == []


async def test_metricflow_calls_python_function_without_child(tmp_path, monkeypatch):
    import dbt_metricflow_service.platform.metricflow as module

    payload = {"mode": "PROBE"}

    def execute(project, profiles, data):
        assert os.getpid() == calling_pid
        assert data is payload
        return {"queryCapability": True}

    calling_pid = os.getpid()
    monkeypatch.setattr(module, "execute_programmatic", execute)
    spec = MetricFlowRequest(
        {**os.environ, "DBT_PROJECT_DIR": str(tmp_path), "DBT_PROFILES_DIR": str(tmp_path)}, payload,
    )
    result = await run_metricflow(PROJECT, spec, 5, 64, before_start)
    assert result.record.status == JobStatus.SUCCEEDED
    assert result.payload == {"queryCapability": True}
    assert list(tmp_path.iterdir()) == []


async def test_metricflow_success_does_not_use_command_exit_codes(tmp_path, monkeypatch):
    import dbt_metricflow_service.platform.metricflow as module

    monkeypatch.setattr(module, "execute_programmatic", lambda *args: {"queryCapability": True})
    spec = MetricFlowRequest(
        {"DBT_PROJECT_DIR": str(tmp_path), "DBT_PROFILES_DIR": str(tmp_path)}, {"mode": "PROBE"},
    )
    result = await run_metricflow(PROJECT, spec, 5, 64, before_start)
    assert result.record.status == JobStatus.SUCCEEDED
    assert result.record.exit_code is None
    assert result.payload == {"queryCapability": True}


async def test_metricflow_invalid_options_returns_business_error_without_exit_code(tmp_path, monkeypatch):
    import dbt_metricflow_service.platform.metricflow as module

    def execute(project, profiles, data):
        raise module.InvalidOptions(SECRET)

    monkeypatch.setattr(module, "execute_programmatic", execute)
    request = MetricFlowRequest(
        {"DBT_PROJECT_DIR": str(tmp_path), "DBT_PROFILES_DIR": str(tmp_path)}, {"mode": "OPTIONS"},
    )
    result = await run_metricflow(PROJECT, request, 5, 64, before_start)
    assert result.error_code == "INVALID_QUERY"
    assert result.payload is None
    assert result.record.status == JobStatus.FAILED
    assert result.record.exit_code is None
    assert SECRET not in str(result.record.model_dump())


@pytest.mark.parametrize("exception,exit_code", [(None, 1), (RuntimeError(SECRET), 2)])
async def test_dbt_failure_preserves_command_exit_codes(tmp_path, monkeypatch, exception, exit_code):
    monkeypatch.setattr(dbtRunner, "invoke", lambda *args: SimpleNamespace(success=False, exception=exception))
    record = await run_dbt(PROJECT, command(tmp_path), 5, 64, before_start)
    assert record.status == JobStatus.FAILED
    assert record.exit_code == exit_code
    assert record.stderr == ("" if exception is None else "RuntimeError")
    assert SECRET not in str(record.model_dump())


@pytest.mark.parametrize("value", [{}, {"columns": ["名称"], "rows": [["订单"]], "truncated": False}])
async def test_programmatic_request_and_result_stay_in_memory(tmp_path, monkeypatch, value):
    import dbt_metricflow_service.platform.metricflow as module

    payload = {"mode": "OPTIONS", "metrics": ["orders"]}
    settings = SimpleNamespace(
        profiles_dir=tmp_path, command_timeout_seconds=5, max_output_bytes=64, max_result_bytes=1024,
    )
    executor = RuntimeExecutor(settings, SimpleNamespace(phase=lambda *args, **kwargs: True), None)
    job = {
        "job_id": PROJECT, "lease_token": PROJECT, "kind": "QUERY_OPTIONS", "project_id": PROJECT,
        "schema_name": SCHEMA, "profile_binding_id": PROJECT,
    }

    def execute(project, profiles, data):
        assert data is payload
        assert list(tmp_path.iterdir()) == []
        return value

    monkeypatch.setattr(module, "execute_programmatic", execute)
    result = await executor._metricflow(job, tmp_path, payload)
    assert result is value
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("invalid_options", [True, False])
async def test_programmatic_errors_do_not_write_control_files(tmp_path, monkeypatch, invalid_options):
    import dbt_metricflow_service.platform.metricflow as module

    settings = SimpleNamespace(
        profiles_dir=tmp_path, command_timeout_seconds=5, max_output_bytes=64, max_result_bytes=1024,
    )
    executor = RuntimeExecutor(settings, SimpleNamespace(phase=lambda *args, **kwargs: True), None)
    job = {
        "job_id": PROJECT, "lease_token": PROJECT, "kind": "QUERY_OPTIONS", "project_id": PROJECT,
        "schema_name": SCHEMA, "profile_binding_id": PROJECT,
    }

    def execute(project, profiles, data):
        raise module.InvalidOptions(SECRET) if invalid_options else ConnectionError(SECRET)

    monkeypatch.setattr(module, "execute_programmatic", execute)
    with pytest.raises(ExecutionError) as error:
        await executor._metricflow(job, tmp_path, {"mode": "OPTIONS", "metrics": ["missing"]})
    assert error.value.code == ("INVALID_QUERY" if invalid_options else "COMMAND_FAILED")
    assert SECRET not in str(error.value.payload)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("value,limit,error_code", [
    ({"x": "中"}, 12, None),
    ({"x": "中"}, 11, "RESULT_TOO_LARGE"),
    ({}, 2, None),
    ({}, 1, "RESULT_TOO_LARGE"),
    ([], 1024, "RESULT_INVALID"),
    (None, 1024, "RESULT_INVALID"),
])
async def test_memory_result_preserves_shape_and_utf8_size_limits(tmp_path, monkeypatch, value, limit, error_code):
    import dbt_metricflow_service.platform.metricflow as module

    settings = SimpleNamespace(
        profiles_dir=tmp_path, command_timeout_seconds=5, max_output_bytes=64, max_result_bytes=limit,
    )
    executor = RuntimeExecutor(settings, SimpleNamespace(phase=lambda *args, **kwargs: True), None)
    job = {
        "job_id": PROJECT, "lease_token": PROJECT, "kind": "QUERY_OPTIONS", "project_id": PROJECT,
        "schema_name": SCHEMA, "profile_binding_id": PROJECT,
    }
    monkeypatch.setattr(module, "execute_programmatic", lambda *args: value)
    if error_code is None:
        assert await executor._metricflow(job, tmp_path, {"mode": "OPTIONS"}) is value
    else:
        with pytest.raises(ExecutionError) as error:
            await executor._metricflow(job, tmp_path, {"mode": "OPTIONS"})
        assert error.value.code == error_code
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("kind,stopped", [("METRIC_QUERY", True), ("RUN_CLEANUP", False)])
async def test_non_json_result_retains_sanitized_command_failure(tmp_path, monkeypatch, kind, stopped):
    import dbt_metricflow_service.platform.metricflow as module
    from dbt_metricflow_service.platform.queries import _json_cell

    settings = SimpleNamespace(
        profiles_dir=tmp_path, command_timeout_seconds=5, max_output_bytes=64, max_result_bytes=1024,
    )
    executor = RuntimeExecutor(settings, SimpleNamespace(phase=lambda *args, **kwargs: True), None)
    job = {
        "job_id": PROJECT, "lease_token": PROJECT, "kind": kind, "project_id": PROJECT,
        "schema_name": SCHEMA, "profile_binding_id": PROJECT,
    }
    # bytea 等二进制列仍可能经过现有单元格转换到达执行边界。
    monkeypatch.setattr(module, "execute_programmatic", lambda *args: {"rows": [[_json_cell(SECRET.encode())]]})
    with pytest.raises(ExecutionError) as error:
        await executor._metricflow(job, tmp_path, {"mode": "QUERY"})
    assert error.value.code == "COMMAND_FAILED"
    assert error.value.stopped is stopped
    assert error.value.payload["status"] == "failed"
    assert error.value.payload["stderr"] == "TypeError"
    assert SECRET not in str(error.value.payload)
    assert list(tmp_path.iterdir()) == []
