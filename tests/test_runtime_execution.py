from __future__ import annotations

import asyncio
import dataclasses
import json
import logging
import os
import threading
import time
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from dbt.cli.main import dbtRunner
from psycopg2 import sql
from psycopg2.extensions import parse_dsn
from psycopg2.extras import Json

import dbt_metricflow_service.runtime.executor as runtime_execution
from dbt_metricflow_service.execution.models import CommandSpec
from dbt_metricflow_service.platform import metricflow
from dbt_metricflow_service.runtime.executor import ExecutionError, RuntimeExecutor
from dbt_metricflow_service.settings import Settings
from dbt_metricflow_service.storage.artifacts import ArtifactStore
from dbt_metricflow_service.storage.jobs import JobStore
from dbt_metricflow_service.storage.postgres import Database
from dbt_metricflow_service.storage.rows import row_dict

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
    (job_id,kind,project_id,request_fingerprint,request_json,config_version,toolchain_version,
     deadline_at,input_set_id,profile_binding_id,schema_name)
    VALUES (%s,%s,%s,'fixture',%s,'1',%s,clock_timestamp()+interval '1 hour',%s,'fixture','run_fixture')"""
# SDK 替身与调用预算测试使用固定协议字段和诊断内容。
SCHEMA_ENV = "DBT_PLATFORM_SCHEMA"
TARGET_ENV = "DBT_TARGET"
FIXTURE_SCHEMA = "run_fixture"
FIXTURE_TARGET = "fixture"
MODE_KEY = "mode"
CLEANUP_MODE = "CLEANUP"
OBSERVED_KEY = "observed"
ENGINE_LOGGER = "metricflow"
DIAGNOSTIC = "x" * 1000
DBT = "dbt"
BUILD = "build"
QUERY_OPTIONS = "QUERY_OPTIONS"
OPTIONS_PAYLOAD = {"mode": "OPTIONS", "metrics": ["orders"]}
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


def claimed(execution, kind, request):
    """直接建立有效认领输入，执行测试不重复覆盖 HTTP 受理策略。"""
    executor, database, project_id, set_id = execution
    toolchain = str(uuid4())
    with database.transaction() as connection:
        connection.exec_driver_sql(INSERT_JOB_SQL, (str(uuid4()), kind, project_id, Json(request), toolchain, set_id))
    return executor.jobs.claim(str(uuid4()), toolchain_version=toolchain)


def programmatic_engine(monkeypatch, execute=None):
    """保留线程、租约和内存载荷传递，仅替换需要仓库访问的 SDK 函数。"""

    def default_execute(project, profiles, data):
        assert (project / PROJECT_FILE).exists()
        assert os.environ[SCHEMA_ENV] == FIXTURE_SCHEMA
        assert os.environ[TARGET_ENV] == FIXTURE_TARGET
        return {} if data[MODE_KEY] == CLEANUP_MODE else {OBSERVED_KEY: data}

    def forbidden(*args, **kwargs):
        raise AssertionError("runtime must execute the engine in process")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", forbidden)
    monkeypatch.setattr(metricflow, "execute_programmatic", execute or default_execute)


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
        command_timeout_seconds=5, max_output_bytes=256, temp_root=tmp_path / "work",
    )
    yield RuntimeExecutor(settings, jobs, artifacts), database, project_id, set_id
    database.close()




@pytest.mark.parametrize("kind,payload,expected", [
    ("QUERY_OPTIONS", {"mode": "OPTIONS", "metrics": ["orders"]},
     {"observed": {"mode": "OPTIONS", "metrics": ["orders"]}}),
    ("RUN_CLEANUP", {"mode": "CLEANUP", "schema": "run_fixture"}, {}),
    ("RUN_CLEANUP", {}, {}),
    ("METRIC_QUERY", {"engineRequest": {"mode": "EXPLAIN", "metrics": ["orders"]}},
     {"observed": {"mode": "QUERY", "request": {"mode": "EXPLAIN", "metrics": ["orders"]}}}),
])
async def test_programmatic_tasks_use_restored_input_and_return_json(execution, monkeypatch, kind, payload, expected):
    executor, _, _, _ = execution
    job = claimed(execution, kind, payload)
    if kind == "RUN_CLEANUP":
        job["parent_run_id"] = str(uuid4())
    programmatic_engine(monkeypatch)
    result = await executor.execute(job)
    assert result.payload == expected
    assert result.output_set_id is None
    assert not (executor.settings.temp_root / str(job["job_id"]) / str(job["attempt_id"])).exists()


async def test_oversized_programmatic_result_is_rejected(execution, monkeypatch):
    executor, _, _, _ = execution
    executor.settings = dataclasses.replace(executor.settings, max_result_bytes=32)
    job = claimed(execution, "QUERY_OPTIONS", {"mode": "OPTIONS", "metrics": ["orders"]})
    programmatic_engine(monkeypatch)
    with pytest.raises(ExecutionError) as error:
        await executor.execute(job)
    assert error.value.code == "RESULT_TOO_LARGE"


async def test_cleanup_before_source_attachment_has_no_external_work(execution):
    executor, database, _, _ = execution
    job = claimed(execution, "RUN_CLEANUP", {})
    with database.transaction() as connection:
        sql_result = connection.exec_driver_sql(
            "UPDATE runtime_job SET input_set_id=NULL WHERE job_id=%s", (job["job_id"],)
        )
    job["input_set_id"] = None
    result = await executor.execute(job)
    assert result.payload == {}
    with database.transaction() as connection:
        sql_result = connection.exec_driver_sql(GET_ATTEMPT_SQL, (job["attempt_id"],))
        assert row_dict(sql_result)["execution_stage"] == "PREPARING"


async def test_expired_lease_does_not_start_engine(execution, monkeypatch, tmp_path):
    executor, database, _, _ = execution
    marker = tmp_path / "must-not-exist"
    job = claimed(execution, "QUERY_OPTIONS", {"mode": "OPTIONS", "metrics": ["orders"]})
    with database.transaction() as connection:
        connection.exec_driver_sql(
            "UPDATE runtime_attempt SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE attempt_id=%s",
            (job["attempt_id"],),
        )
    # 保留真实租约过期检查，SDK 的副作用绝不能出现。
    def execute(project, profiles, data):
        marker.touch()
        return {}

    programmatic_engine(monkeypatch, execute)
    with pytest.raises(ExecutionError) as error:
        await executor.execute(job)
    assert error.value.code == "LEASE_LOST"
    assert not marker.exists()


@pytest.mark.parametrize("cancel", [True, False])
async def test_cancellation_or_timeout_waits_for_engine_before_workspace_removal(execution, monkeypatch, cancel):
    executor, _, _, _ = execution
    started, release = threading.Event(), threading.Event()
    finished = []
    executor.settings = dataclasses.replace(
        executor.settings, command_timeout_seconds=5 if cancel else 0.02,
    )
    job = claimed(execution, QUERY_OPTIONS, OPTIONS_PAYLOAD)
    workspace = executor.settings.temp_root / str(job["job_id"]) / str(job["attempt_id"])

    # 取消或超时后引擎仍可写工作目录；必须等待这次写入结束才能移除目录。
    def execute(project, profiles, data):
        started.set()
        assert release.wait(5)
        marker = project / COMPLETED_FILE
        marker.touch()
        finished.append(marker.exists())
        return {}

    programmatic_engine(monkeypatch, execute)
    task = asyncio.create_task(executor.execute(job))
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
        assert isinstance(result, ExecutionError)
        assert result.code == "COMMAND_TIMEOUT"
        assert result.stopped is False
    assert finished == [True]
    assert not workspace.exists()






async def test_timeout_retains_bounded_diagnostics_and_unknown_stop(execution, monkeypatch):
    executor, _, project_id, _ = execution
    executor.settings = dataclasses.replace(executor.settings, command_timeout_seconds=0.2, max_output_bytes=32)
    job = claimed(execution, "QUERY_OPTIONS", {"mode": "OPTIONS", "metrics": ["orders"]})
    # 诊断经过真实 SDK 日志收集器，超时仍需等线程返回后才能上报。
    def execute(project, profiles, data):
        logging.getLogger(ENGINE_LOGGER).warning(DIAGNOSTIC)
        time.sleep(0.3)
        return {}

    programmatic_engine(monkeypatch, execute)
    with pytest.raises(ExecutionError) as error:
        await executor.execute(job)
    assert error.value.code == "COMMAND_TIMEOUT"
    assert error.value.stopped is False
    assert error.value.payload["status"] == "timed_out"
    assert len(error.value.payload["stdout"].encode()) <= 32
    assert error.value.payload["output_truncated"] is True


async def test_write_engine_connection_loss_does_not_confirm_external_stop(execution, monkeypatch):
    executor, _, _, _ = execution
    job = claimed(execution, "BUILD_RUN", {})
    spec = CommandSpec((DBT, BUILD),
                       executor.settings.temp_root, dict(os.environ), True)
    spec.cwd.mkdir(parents=True, exist_ok=True)
    # SDK 连接异常不能证明外部写入已经停止，保留未知结果保护。
    monkeypatch.setattr(dbtRunner, "invoke", lambda *_: SimpleNamespace(success=False, exception=ConnectionError()))
    with pytest.raises(ExecutionError) as error:
        await executor._dbt(job, spec, "BUILDING")
    assert error.value.stopped is False



async def test_options_invalid_input_uses_structured_error_code(execution, monkeypatch):
    executor, _, _, _ = execution
    job = claimed(execution, "QUERY_OPTIONS", {"mode": "OPTIONS", "metrics": ["missing"]})
    # SDK 参数异常通过内存错误码转换为稳定公开错误码。
    def execute(project, profiles, data):
        raise metricflow.InvalidOptions()

    programmatic_engine(monkeypatch, execute)
    with pytest.raises(ExecutionError) as error:
        await executor.execute(job)
    assert error.value.code == "INVALID_QUERY"
    assert error.value.stopped is True


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


async def test_source_only_cleanup_drops_schema_without_semantic_manifest(execution, monkeypatch, tmp_path):
    executor, database, project_id, _ = execution
    connection = parse_dsn(os.environ[DATABASE_ENV])
    for name, key in [("HOST", "host"), ("PORT", "port"), ("USER", "user"), ("DATABASE", "dbname")]:
        monkeypatch.setenv("CLEANUP_TEST_" + name, connection[key])
    monkeypatch.setenv("DBT_ENV_SECRET_CLEANUP_PASSWORD", connection["password"])
    schema = "run_" + uuid4().hex
    profiles = executor.settings.profiles_dir
    profiles.mkdir()
    (profiles / "profiles.yml").write_text(json.dumps({"cleanup_fixture": {
        "target": "fixture", "outputs": {"fixture": {
            "type": "postgres", "host": "{{ env_var('CLEANUP_TEST_HOST') }}",
            "port": "{{ env_var('CLEANUP_TEST_PORT') | int }}", "user": "{{ env_var('CLEANUP_TEST_USER') }}",
            "password": "{{ env_var('DBT_ENV_SECRET_CLEANUP_PASSWORD') }}",
            "dbname": "{{ env_var('CLEANUP_TEST_DATABASE') }}", "schema": "{{ env_var('DBT_PLATFORM_SCHEMA') }}",
            "threads": 1,
        }},
    }}), encoding="utf-8")
    source = tmp_path / "cleanup-source"
    source.mkdir()
    (source / PROJECT_FILE).write_text(PROJECT_TEXT + "profile: cleanup_fixture\n", encoding="utf-8")
    set_id = executor.artifacts.capture(project_id, source)
    job = claimed(execution, "RUN_CLEANUP", {})
    with database.transaction() as connection:
        sql_result = connection.exec_driver_sql(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        sql_result = connection.exec_driver_sql("UPDATE runtime_job SET input_set_id=%s,schema_name=%s WHERE job_id=%s",
                       (set_id, schema, job["job_id"]))
    job.update(input_set_id=set_id, schema_name=schema, parent_run_id=str(UUID(schema.removeprefix("run_"))))
    executor.settings = dataclasses.replace(executor.settings, command_timeout_seconds=30)
    try:
        result = await executor.execute(job)
        assert result.payload == {"cleaned": True}
        with database.transaction() as connection:
            sql_result = connection.exec_driver_sql("SELECT 1 FROM pg_namespace WHERE nspname=%s", (schema,))
            assert row_dict(sql_result) is None
    finally:
        with database.transaction() as connection:
            sql_result = connection.exec_driver_sql(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema))
            )
