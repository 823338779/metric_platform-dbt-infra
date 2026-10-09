from __future__ import annotations

import asyncio
import dataclasses
import json
import os
import sys
import threading
from uuid import uuid4

import pytest
from psycopg2 import sql
from psycopg2.extensions import parse_dsn
from psycopg2.extras import Json

import dbt_metricflow_service.runtime.executor as runtime_execution
from dbt_metricflow_service.runtime.executor import ExecutionError, RuntimeExecutor
from dbt_metricflow_service.settings import Settings
from dbt_metricflow_service.storage.artifacts import ArtifactStore
from dbt_metricflow_service.storage.jobs import JobStore
from dbt_metricflow_service.storage.postgres import Database
from dbt_metricflow_service.storage.rows import row_dict
from tests.test_platform_bindings import digest, git

# 测试使用真实存储与进程；仅把外部仓库命令替换为 Python 子进程。
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
PROGRAMMATIC_CODE = """
import json, os, sys
from pathlib import Path
data = json.loads(Path(sys.argv[1]).read_text())
assert Path(os.environ['DBT_PROJECT_DIR'], 'dbt_project.yml').exists()
assert os.environ['DBT_PLATFORM_SCHEMA'] == 'run_fixture'
assert os.environ['DBT_TARGET'] == 'fixture'
result = {} if data['mode'] == 'CLEANUP' else {'observed': data}
Path(sys.argv[2]).write_text(json.dumps(result))
"""
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


def programmatic_child(monkeypatch, code=PROGRAMMATIC_CODE):
    """保留实际环境与文件传参，仅替换需要仓库访问的子进程入口。"""
    original = runtime_execution.build_programmatic_command

    def command(*args):
        spec = original(*args)
        return dataclasses.replace(spec, argv=(sys.executable, "-c", code, *spec.argv[-2:]))

    monkeypatch.setattr(runtime_execution, "build_programmatic_command", command)


@pytest.fixture
def execution(tmp_path):
    dsn = os.environ.get(DATABASE_ENV)
    if not dsn:
        pytest.skip("SERVICE_TEST_DATABASE_URL is required for execution integration tests")
    database = Database(dsn)
    database.migrate()
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
        projects_root=tmp_path / "unused", profiles_dir=tmp_path / "profiles",
        command_timeout_seconds=5, max_output_bytes=256, temp_root=tmp_path / "work",
    )
    yield RuntimeExecutor(settings, jobs, artifacts), database, project_id, set_id
    database.close()


async def test_generic_execution_restores_snapshot_and_captures_new_output(execution, monkeypatch, tmp_path):
    executor, database, project_id, source_set = execution
    version = str(uuid4())
    executor.jobs.reserve(
        "DBT_COMMAND", project_id, {"project": project_id, "command": "parse"},
        toolchain_version=version,
    )
    job = executor.jobs.claim(str(uuid4()), toolchain_version=version)
    original = runtime_execution.build_dbt_command

    # 保留真实参数构造，仅把 dbt 外部可执行文件换成受控脚本。
    def command(request, project, profiles):
        spec = original(request, project, profiles)
        return dataclasses.replace(spec, argv=(sys.executable, "-c", EXECUTE_CODE))

    monkeypatch.setattr(runtime_execution, "build_dbt_command", command)
    result = await executor.execute(job)
    assert result.payload["status"] == "succeeded"
    assert result.payload["stdout"].strip() == "execution finished"
    assert result.output_set_id != source_set
    with database.transaction() as connection:
        executor.artifacts.seal(result.output_set_id, connection)
    restored = tmp_path / "output"
    executor.artifacts.materialize(result.output_set_id, restored)
    assert json.loads((restored / TARGET_FILE).read_text())["metrics"] == []
    with database.transaction() as connection:
        sql_result = connection.exec_driver_sql(GET_ATTEMPT_SQL, (job["attempt_id"],))
        assert row_dict(sql_result)["execution_stage"] == "EXTERNAL"
    assert not (executor.settings.temp_root / str(job["job_id"]) / str(job["attempt_id"])).exists()


@pytest.mark.parametrize("kind,payload,expected", [
    ("QUERY_OPTIONS", {"mode": "OPTIONS", "metrics": ["orders"]},
     {"observed": {"mode": "OPTIONS", "metrics": ["orders"]}}),
    ("RUN_CLEANUP", {"mode": "CLEANUP", "schema": "run_fixture"}, {}),
    ("RUN_CLEANUP", {}, {}),
    ("METRIC_QUERY", {"mode": "EXPLAIN", "metrics": ["orders"]},
     {"observed": {"mode": "QUERY", "request": {"mode": "EXPLAIN", "metrics": ["orders"]}}}),
])
async def test_programmatic_tasks_use_restored_input_and_return_json(execution, monkeypatch, kind, payload, expected):
    executor, _, _, _ = execution
    job = claimed(execution, kind, payload)
    programmatic_child(monkeypatch)
    result = await executor.execute(job)
    assert result.payload == expected
    assert result.output_set_id is None
    assert not (executor.settings.temp_root / str(job["job_id"]) / str(job["attempt_id"])).exists()


async def test_oversized_programmatic_result_is_rejected_before_parsing(execution, monkeypatch):
    executor, _, _, _ = execution
    executor.settings = dataclasses.replace(executor.settings, max_result_bytes=32)
    job = claimed(execution, "QUERY_OPTIONS", {"mode": "OPTIONS", "metrics": ["orders"]})
    programmatic_child(monkeypatch)
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


async def test_expired_lease_does_not_start_child(execution, monkeypatch, tmp_path):
    executor, database, _, _ = execution
    marker = tmp_path / "must-not-exist"
    job = claimed(execution, "QUERY_OPTIONS", {"mode": "OPTIONS", "metrics": ["orders"]})
    with database.transaction() as connection:
        connection.exec_driver_sql(
            "UPDATE runtime_attempt SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE attempt_id=%s",
            (job["attempt_id"],),
        )
    programmatic_child(monkeypatch, f"from pathlib import Path; Path({str(marker)!r}).touch()")
    with pytest.raises(ExecutionError) as error:
        await executor.execute(job)
    assert error.value.code == "LEASE_LOST"
    assert not marker.exists()


async def test_cancellation_stops_process_before_workspace_removal(execution, monkeypatch, tmp_path):
    executor, _, _, _ = execution
    started = tmp_path / "started"
    escaped = tmp_path / "escaped"
    code = (
        "from pathlib import Path; import time; "
        f"Path({str(started)!r}).touch(); time.sleep(1); Path({str(escaped)!r}).touch()"
    )
    job = claimed(execution, "QUERY_OPTIONS", {"mode": "OPTIONS", "metrics": ["orders"]})
    programmatic_child(monkeypatch, code)
    task = asyncio.create_task(executor.execute(job))
    async with asyncio.timeout(5):
        while not started.exists():
            await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not (executor.settings.temp_root / str(job["job_id"]) / str(job["attempt_id"])).exists()
    await asyncio.sleep(1.1)
    assert not escaped.exists()


@pytest.mark.parametrize("probe_passed", [True, False])
async def test_fixed_build_pins_git_source_preserves_results_and_requires_probe(
    execution, monkeypatch, tmp_path, probe_passed,
):
    executor, database, project_id, _ = execution
    repo = tmp_path / "git"
    repo.mkdir()
    (repo / PROJECT_FILE).write_text(PROJECT_TEXT, encoding="utf-8")
    git(repo, "init", "-b", "main")
    git(repo, "config", "user.email", "execution@example.invalid")
    git(repo, "config", "user.name", "Execution fixture")
    git(repo, "add", ".")
    git(repo, "commit", "-m", "fixture")
    sha = git(repo, "rev-parse", "HEAD")
    version = str(uuid4())
    executor.jobs.reserve(
        "BUILD_RUN", project_id, {
            "commitSha": sha, "projectDigest": digest(repo, sha),
            "binding": {"projectId": project_id, "remote": str(repo), "projectSubdir": ".",
                        "profileBindingId": "fixture"},
        }, toolchain_version=version, schema_name="run_fixture", profile_binding_id="fixture",
    )
    job = executor.jobs.claim(str(uuid4()), toolchain_version=version)
    original_submit = runtime_execution.JobRunner.submit

    # 真正启动子进程前检查 PostgreSQL 中的源码引用与外部执行记录。
    async def submit(runner, project, spec):
        if spec.argv[0] == "dbt":
            persisted = executor.jobs.get(job["job_id"])
            assert persisted["input_set_id"] is not None
            with database.transaction() as connection:
                sql_result = connection.exec_driver_sql(GET_ATTEMPT_SQL, (job["attempt_id"],))
                assert row_dict(sql_result)["execution_stage"] == "EXTERNAL"
            spec = dataclasses.replace(spec, argv=(sys.executable, "-c", BUILD_CODE, spec.argv[1]))
        return await original_submit(runner, project, spec)

    monkeypatch.setattr(runtime_execution.JobRunner, "submit", submit)
    programmatic_child(
        monkeypatch,
        "import json,sys; from pathlib import Path; "
        f"Path(sys.argv[2]).write_text(json.dumps({{'queryCapability': {probe_passed!r}}}))",
    )
    if not probe_passed:
        with pytest.raises(ValueError):
            await executor.execute(job)
        with database.transaction() as connection:
            sql_result = connection.exec_driver_sql(
                "SELECT count(*) AS n FROM runtime_artifact_set WHERE producer_attempt_id=%s", (job["attempt_id"],)
            )
            assert row_dict(sql_result)["n"] == 0
        return
    result = await executor.execute(job)
    persisted = executor.jobs.get(job["job_id"])
    source = executor.artifacts.metadata(persisted["input_set_id"])
    assert source["state"] == "SEALED"
    assert source["source_commit_sha"] == sha
    output = executor.artifacts.metadata(result.output_set_id)
    assert output["validation_json"]["representativeQueryPassed"] is True
    assert output["validation_json"]["allTestsPassed"] is True
    assert output["catalog_json"]["resources"][0]["name"] == "orders"
    assert output["producer_attempt_id"] == job["attempt_id"]
    assert output["state"] == "STAGING"


@pytest.mark.parametrize("succeeds", [True, False])
async def test_resource_input_and_derived_output_never_persist(execution, monkeypatch, succeeds):
    executor, database, project_id, _ = execution
    job = claimed(execution, "DBT_COMMAND", {"project": project_id, "command": "parse"})
    resources = {"private.yml": "version: 2\n# private-resource-marker\n"}
    original = runtime_execution.build_dbt_command

    def command(request, project, profiles):
        spec = original(request, project, profiles)
        return dataclasses.replace(
            spec, argv=(sys.executable, "-c", RESOURCE_CODE, "success" if succeeds else "failure"),
        )

    monkeypatch.setattr(runtime_execution, "build_dbt_command", command)
    if succeeds:
        result = await executor.execute(job, resources)
        assert result.output_set_id is None
        assert "resource execution finished" in result.payload["stdout"]
        executor.jobs.finish(job["job_id"], job["lease_token"], result.payload)
        persisted = executor.jobs.result(job["job_id"])
    else:
        with pytest.raises(ExecutionError) as error:
            await executor.execute(job, resources)
        assert error.value.payload["exit_code"] == 7
        executor.jobs.fail(job["job_id"], job["lease_token"], error.value.code, error.value.payload)
        persisted = executor.jobs.get(job["job_id"])
    assert "private-resource-marker" not in json.dumps(persisted, default=str)
    with database.transaction() as connection:
        sql_result = connection.exec_driver_sql(
            "SELECT count(*) AS n FROM runtime_artifact_set WHERE producer_attempt_id=%s", (job["attempt_id"],)
        )
        assert row_dict(sql_result)["n"] == 0


async def test_timeout_retains_bounded_diagnostics_and_unknown_stop(execution, monkeypatch):
    executor, _, project_id, _ = execution
    executor.settings = dataclasses.replace(executor.settings, command_timeout_seconds=0.2, max_output_bytes=32)
    job = claimed(execution, "DBT_COMMAND", {"project": project_id, "command": "parse"})
    original = runtime_execution.build_dbt_command

    def command(request, project, profiles):
        spec = original(request, project, profiles)
        return dataclasses.replace(spec, argv=(
            sys.executable, "-c", "import time; print('x' * 1000, flush=True); time.sleep(10)",
        ))

    monkeypatch.setattr(runtime_execution, "build_dbt_command", command)
    with pytest.raises(ExecutionError) as error:
        await executor.execute(job)
    assert error.value.code == "COMMAND_TIMEOUT"
    assert error.value.stopped is False
    assert error.value.payload["status"] == "timed_out"
    assert len(error.value.payload["stdout"].encode()) <= 32
    assert error.value.payload["output_truncated"] is True


async def test_write_child_connection_loss_does_not_confirm_external_stop(execution, monkeypatch):
    executor, _, project_id, _ = execution
    job = claimed(execution, "DBT_COMMAND", {"project": project_id, "command": "run"})
    original = runtime_execution.build_dbt_command

    def command(request, project, profiles):
        spec = original(request, project, profiles)
        return dataclasses.replace(spec, argv=(sys.executable, "-c", "import sys; sys.exit(2)"))

    monkeypatch.setattr(runtime_execution, "build_dbt_command", command)
    with pytest.raises(ExecutionError) as error:
        await executor.execute(job)
    assert error.value.stopped is False


async def test_options_invalid_input_uses_structured_error_code(execution, monkeypatch):
    executor, _, _, _ = execution
    job = claimed(execution, "QUERY_OPTIONS", {"mode": "OPTIONS", "metrics": ["missing"]})
    programmatic_child(monkeypatch,
        "import sys; from pathlib import Path; "
        "Path(sys.argv[2]).write_text('{\"errorCode\":\"INVALID_QUERY\"}'); sys.exit(2)")
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
    job.update(input_set_id=set_id, schema_name=schema)
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
