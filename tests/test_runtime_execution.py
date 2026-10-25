from __future__ import annotations

import asyncio
import dataclasses
import logging
import os
import threading
import time
from types import SimpleNamespace
from uuid import uuid4

import pytest
from dbt.cli.main import dbtRunner
from psycopg2.extras import Json

import dbt_metricflow_service.runtime.dbt_executor as runtime_execution
from dbt_metricflow_service.runtime.dbt_executor import ExecutionError, RuntimeExecutor
from dbt_metricflow_service.settings import Settings
from dbt_metricflow_service.storage.artifacts import ArtifactStore
from dbt_metricflow_service.storage.jobs import JobStore
from dbt_metricflow_service.storage.postgres import Database
from dbt_metricflow_service.storage.records import JobRequest
from tests.dbt_execution_helpers import EngineCall, invoke_job_for_test

# 测试使用真实存储和执行线程，仅替换访问外部仓库的 SDK 调用。
DATABASE_ENV = "SERVICE_TEST_DATABASE_URL"
PROJECT_FILE = "dbt_project.yml"
PROJECT_TEXT = "name: execution\nversion: '1.0'\n"
TARGET_FILE = "target/semantic_manifest.json"
EXECUTE_CODE = (
    "from pathlib import Path; "
    "Path('target').mkdir(exist_ok=True); "
    "Path('target/semantic_manifest.json').write_text('{\"metrics\": []}'); "
    "print('execution finished')"
)
SET_PROJECT_SQL = "UPDATE runtime_project SET source_set_id=%s WHERE project_id=%s"
GET_ATTEMPT_SQL = "SELECT execution_stage FROM runtime_attempt WHERE attempt_id=%s"
INSERT_JOB_SQL = """INSERT INTO runtime_job
    (job_id,project_id,request_fingerprint,request_json,config_version,toolchain_version,
     input_set_id,profile_binding_id,schema_name)
    VALUES (%s,%s,'fixture',%s,'1',%s,%s,'fixture','run_fixture')"""
# SDK 替身与慢调用测试使用固定协议字段和诊断内容。
SCHEMA_ENV = "DBT_PLATFORM_SCHEMA"
TARGET_ENV = "DBT_TARGET"
FIXTURE_SCHEMA = "run_fixture"
FIXTURE_TARGET = "fixture"
MODE_KEY = "mode"
CLEANUP_MODE = "CLEANUP"
OBSERVED_KEY = "observed"
ENGINE_LOGGER = "dbt"
DIAGNOSTIC = "x" * 1000
DBT = "dbt"
BUILD = "build"
BUILD_PAYLOAD = JobRequest()
COMPLETED_FILE = "completed"
BUILD_CODE = """
import json, os, sys
from pathlib import Path
target = Path('target')
target.mkdir(exist_ok=True)
if sys.argv[1] == 'docs':
    (target / 'run_results.json').write_text('{"results": []}')
    sys.exit(0)
schema = os.environ['DBT_PLATFORM_SCHEMA']
documents = {
    'manifest.json': {
        'metadata': {'adapter_type': 'postgres', 'dbt_schema_version':
                     'https://schemas.getdbt.com/dbt/manifest/v12.json'},
        'nodes': {'model.fixture.orders': {
            'resource_type': 'model', 'schema': schema, 'relation_name': schema + '.orders',
            'config': {'materialized': 'table'}, 'name': 'orders'}}},
    'semantic_manifest.json': {'semantic_models': [], 'metrics': []},
    'catalog.json': {'metadata': {'dbt_schema_version': 'https://schemas.getdbt.com/dbt/catalog/v1.json'},
                     'nodes': {'model.fixture.orders': {'columns': {}}}},
    'run_results.json': {'results': [{'unique_id': 'model.fixture.orders', 'status': 'success'}]},
}
for name, value in documents.items():
    (target / name).write_text(json.dumps(value))
"""
RESOURCE_CODE = """
import json, os, sys
from pathlib import Path
envelope = json.loads(sys.stdin.buffer.read())
raw = envelope['request']['resources']['private.yml']
assert 'private-resource-marker' in raw
assert all('private-resource-marker' not in path.read_text(errors='ignore')
           for path in Path.cwd().rglob('*') if path.is_file())
Path(os.environ['JOB_ARTIFACT_DIR'], 'manifest.json').write_text(raw)
if sys.argv[1] == 'failure':
    print(raw, file=sys.stderr)
    print(raw)
    sys.exit(7)
print('resource execution finished')
print(raw)
"""


def claimed(execution, request):
    """直接建立有效认领输入，执行测试不重复覆盖 HTTP 受理策略。"""
    executor, database, project_id, set_id = execution
    toolchain = str(uuid4())
    with database.transaction() as connection:
        connection.exec_driver_sql(INSERT_JOB_SQL, (
            str(uuid4()), project_id,
            Json(request.model_dump(mode="json", by_alias=True, exclude_unset=True)), toolchain, set_id,
        ))
    job = executor.jobs.claim(str(uuid4()), toolchain_version=toolchain)
    return job


def programmatic_engine(monkeypatch, execute=None):
    """保留线程、租约和内存载荷传递，仅替换需要仓库访问的 SDK 函数。"""

    def default_execute(project, profiles):
        assert (project / PROJECT_FILE).exists()
        assert os.environ[SCHEMA_ENV] == FIXTURE_SCHEMA
        assert os.environ[TARGET_ENV] == FIXTURE_TARGET
        return SimpleNamespace(success=True, exception=None)

    def forbidden(*args, **kwargs):
        raise AssertionError("runtime must execute the engine in process")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", forbidden)
    def invoke(self, args):
        from pathlib import Path
        operation = execute or default_execute
        return operation(Path(os.environ["DBT_PROJECT_DIR"]), Path(os.environ["DBT_PROFILES_DIR"]))
    monkeypatch.setattr(dbtRunner, "invoke", invoke)


async def run_build(executor, job, project):
    environment = runtime_execution.build_engine_environment(
        project, executor.settings.profiles_dir, job.schema_name, job.profile_binding_id)
    spec = EngineCall(("dbt", "build"), project, environment, True)
    return await invoke_job_for_test(executor, job, spec, "BUILDING")


@pytest.fixture
def execution(tmp_path):
    dsn = os.environ.get(DATABASE_ENV)
    if not dsn:
        pytest.skip("SERVICE_TEST_DATABASE_URL is required for execution integration tests")
    database = Database(dsn)
    database.initialize()
    jobs = JobStore(database)
    artifacts = ArtifactStore(database)
    project_id = str(uuid4())
    jobs.register_project(project_id)
    source = tmp_path / "source"
    source.mkdir()
    (source / PROJECT_FILE).write_text(PROJECT_TEXT, encoding="utf-8")
    set_id = artifacts.capture(project_id, source)
    with database.transaction() as connection:
        connection.exec_driver_sql(SET_PROJECT_SQL, (set_id, project_id))
    settings = Settings(
         profiles_dir=tmp_path / "profiles",
        max_output_bytes=256, temp_root=tmp_path / "work",
    )
    yield RuntimeExecutor(settings, jobs, artifacts), database, project_id, set_id
    database.close()








async def test_expired_lease_does_not_start_engine(execution, monkeypatch, tmp_path):
    executor, database, _, _ = execution
    marker = tmp_path / "must-not-exist"
    job = claimed(execution, JobRequest())
    with database.transaction() as connection:
        connection.exec_driver_sql(
            "UPDATE runtime_attempt SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE attempt_id=%s",
            (job.attempt_id,),
        )
    # 保留真实租约过期检查，SDK 的副作用绝不能出现。
    def execute(project, profiles):
        marker.touch()
        return SimpleNamespace(success=True, exception=None)

    programmatic_engine(monkeypatch, execute)
    with pytest.raises(ExecutionError) as error:
        await run_build(executor, job, tmp_path)
    assert error.value.code == "LEASE_LOST"
    assert not marker.exists()


@pytest.mark.parametrize("cancel", [True, False])
async def test_cancellation_or_slow_call_waits_for_engine_before_workspace_removal(execution, monkeypatch, cancel):
    executor, _, _, _ = execution
    started, release = threading.Event(), threading.Event()
    finished = []
    job = claimed(execution, BUILD_PAYLOAD)
    workspace = executor.settings.temp_root / str(job.job_id) / str(job.attempt_id)

    # 取消或慢调用期间引擎仍可写工作目录；必须等待这次写入结束才能移除目录。
    def execute(project, profiles):
        started.set()
        assert release.wait(5)
        marker = project / COMPLETED_FILE
        marker.touch()
        finished.append(marker.exists())
        return SimpleNamespace(success=True, exception=None)

    programmatic_engine(monkeypatch, execute)

    async def run_engine():
        with runtime_execution.attempt_workspace(
            executor.settings.temp_root, job.job_id, job.attempt_id
        ) as attempt:
            project = attempt / "project"
            await runtime_execution._thread(executor.artifacts.materialize, job.input_set_id, project)
            await run_build(executor, job, project)

    task = asyncio.create_task(run_engine())
    try:
        async with asyncio.timeout(5):
            while not started.is_set():
                await asyncio.sleep(0.01)
        if cancel:
            task.cancel()
            await asyncio.sleep(0)
            task.cancel()
        await asyncio.sleep(0.07)
        assert not task.done()
        assert workspace.exists()
    finally:
        release.set()
        result, = await asyncio.gather(task, return_exceptions=True)
    if cancel:
        assert isinstance(result, asyncio.CancelledError)
    else:
        assert result is None
    assert finished == [True]
    assert not workspace.exists()






async def test_slow_success_retains_bounded_diagnostics(execution, monkeypatch):
    executor, _, project_id, _ = execution
    executor.settings = dataclasses.replace(executor.settings, max_output_bytes=32)
    job = claimed(execution, JobRequest())
    # 诊断经过真实 SDK 日志收集器，等待线程返回后上报。
    def execute(project, profiles):
        logging.getLogger(ENGINE_LOGGER).warning(DIAGNOSTIC)
        time.sleep(0.3)
        return SimpleNamespace(success=True, exception=None)

    programmatic_engine(monkeypatch, execute)
    record = await run_build(executor, job, executor.settings.temp_root)
    assert record.status == "succeeded"
    assert len(record.stdout.encode()) <= 32
    assert record.output_truncated is True


async def test_write_engine_connection_loss_does_not_confirm_external_stop(execution, monkeypatch):
    executor, _, _, _ = execution
    job = claimed(execution, JobRequest())
    spec = EngineCall((DBT, BUILD),
                       executor.settings.temp_root, dict(os.environ), True)
    spec.cwd.mkdir(parents=True, exist_ok=True)
    # SDK 连接异常不能证明外部写入已经停止，保留未知结果保护。
    monkeypatch.setattr(dbtRunner, "invoke", lambda *_: SimpleNamespace(success=False, exception=ConnectionError()))
    with pytest.raises(ExecutionError) as error:
        await invoke_job_for_test(executor, job, spec, "BUILDING")
    assert error.value.stopped is False



async def test_repeated_cancellation_waits_for_filesystem_thread(tmp_path):
    started, release = threading.Event(), threading.Event()
    marker = tmp_path / "completed"

    # 真实线程被两个取消请求打断时，也必须等到最后一次写入结束。
    def operation():
        started.set()
        if release.wait(5):
            marker.touch()

    task = asyncio.create_task(runtime_execution._thread(operation))
    try:
        async with asyncio.timeout(5):
            while not started.is_set():
                await asyncio.sleep(0.01)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0.02)
        assert not task.done()
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
    assert marker.exists()
