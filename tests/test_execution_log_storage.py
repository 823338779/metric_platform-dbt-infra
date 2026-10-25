"""独立执行日志表及后台批量写入的行为验证。"""

import asyncio
import threading
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from dbt.cli.main import dbtRunner
from dbt.events.types import JinjaLogInfo
from dbt_common.events.base_types import msg_from_base_event

from dbt_metricflow_service.execution.build import BuildExecutor
from dbt_metricflow_service.execution.errors import ExecutionError
from dbt_metricflow_service.execution.logs import EngineLog
from dbt_metricflow_service.execution.models import JobStatus
from dbt_metricflow_service.storage.jobs import JobStore
from tests.dbt_execution_helpers import EngineCall, invoke_job_for_test
from tests.schema_helpers import isolated_database
from tests.test_build_acceptance import CALLER, TOOLCHAIN, request, service


def entry(message):
    return EngineLog(datetime.now(UTC), "info", message, event_name="JinjaLogInfo")


def test_writer_flushes_while_running_and_drains_on_close():
    from dbt_metricflow_service.execution.log_writer import ExecutionLogWriter

    batches = []
    flushed = threading.Event()

    def write(batch):
        batches.append(batch)
        flushed.set()

    writer = ExecutionLogWriter(write)
    try:
        writer.emit(entry("before close"))
        assert flushed.wait(3), "运行期间应该定时提交，不等命令结束"
        for index in range(205):
            writer.emit(entry(str(index)))
    finally:
        writer.close()
    assert [row.message for batch in batches for row in batch] == ["before close", *map(str, range(205))]
    assert all(len(batch) <= 100 for batch in batches)


def test_writer_reports_failure_without_leaking_database_error(caplog):
    from dbt_metricflow_service.execution.log_writer import ExecutionLogWriter

    def unavailable(batch):
        raise RuntimeError("private database connection details")

    writer = ExecutionLogWriter(unavailable)
    for index in range(205):
        writer.emit(entry(str(index)))
    writer.close()
    assert "RuntimeError" in caplog.text
    assert "205" in caplog.text
    assert "private database" not in caplog.text


def test_full_queue_drops_with_warning_instead_of_blocking(monkeypatch, caplog):
    import dbt_metricflow_service.execution.log_writer as module

    monkeypatch.setattr(module, "QUEUE_SIZE", 2)
    monkeypatch.setattr(module, "BATCH_SIZE", 1)
    entered, release = threading.Event(), threading.Event()
    saved = []

    def blocked(batch):
        entered.set()
        assert release.wait(3)
        saved.extend(batch)

    writer = module.ExecutionLogWriter(blocked)
    try:
        writer.emit(entry("first"))
        assert entered.wait(3)
        writer.emit(entry("second"))
        writer.emit(entry("third"))
        writer.emit(entry("dropped"))
    finally:
        release.set()
        writer.close()
    assert [row.message for row in saved] == ["first", "second", "third"]
    assert "dropped" in caplog.text.lower()


@pytest.mark.parametrize("success", [True, False])
async def test_executor_persists_events_with_build_and_attempt_context(tmp_path, monkeypatch, success):
    with isolated_database() as db:
        db.initialize()
        app = service(SimpleNamespace(db=db))
        first = app.submit_snapshot(request(), CALLER)
        second = app.submit_snapshot(request(), CALLER)
        jobs = JobStore(db)
        executor = BuildExecutor(SimpleNamespace(max_output_bytes=32), jobs, None)

        def invoke(self, args):
            for callback in self.callbacks:
                callback(msg_from_base_event(JinjaLogInfo(msg="early " + "x" * 200)))
                callback(msg_from_base_event(JinjaLogInfo(msg="finished")))
            return SimpleNamespace(success=success, exception=None)

        monkeypatch.setattr(dbtRunner, "invoke", invoke)
        claimed = []
        for _ in (first, second):
            job = jobs.claim(str(uuid4()), toolchain_version=TOOLCHAIN)
            claimed.append(job)
            spec = EngineCall(("dbt", "parse"), tmp_path, {}, False)
            if success:
                result = await invoke_job_for_test(executor, job, spec, "PARSING")
                assert result.status == JobStatus.SUCCEEDED
            else:
                with pytest.raises(ExecutionError):
                    await invoke_job_for_test(executor, job, spec, "PARSING")
        with db.transaction() as connection:
            rows = connection.exec_driver_sql(
                "SELECT build_id,job_id,attempt_id,phase,command,message,event_name "
                "FROM runtime_execution_log ORDER BY log_id"
            ).mappings().all()
        assert len(rows) == 4
        for index, job in enumerate(claimed):
            batch = rows[index * 2:index * 2 + 2]
            assert {row["build_id"] for row in batch} == {job.request_json.build_id}
            assert {row["job_id"] for row in batch} == {job.job_id}
            assert {row["attempt_id"] for row in batch} == {job.attempt_id}
            assert {row["phase"] for row in batch} == {"PARSING"}
            assert {row["command"] for row in batch} == {"parse"}
            assert [row["message"] for row in batch] == ["early " + "x" * 200, "finished"]
            assert {row["event_name"] for row in batch} == {"JinjaLogInfo"}


def test_existing_schema_can_add_execution_log_table():
    with isolated_database() as db:
        db.initialize()
        with db.transaction() as connection:
            connection.exec_driver_sql("DROP TABLE runtime_execution_log")
        with pytest.raises(RuntimeError):
            db.check()
        upgrade = Path("src/dbt_metricflow_service/storage/add_execution_log.sql").read_text(encoding="utf-8")
        with db.transaction() as connection:
            connection.exec_driver_sql(upgrade)
        db.check()


async def test_real_parse_and_build_persist_internal_debug_and_model_events(tmp_path):
    from dbt_metricflow_service.execution.environment import build_engine_environment
    from tests.resource_helpers import make_resource_project

    project, profiles, _ = make_resource_project(tmp_path)
    with isolated_database() as db:
        db.initialize()
        service(SimpleNamespace(db=db)).submit_snapshot(request(), CALLER)
        jobs = JobStore(db)
        job = jobs.claim(str(uuid4()), toolchain_version=TOOLCHAIN)
        executor = BuildExecutor(SimpleNamespace(max_output_bytes=32), jobs, None)
        environment = build_engine_environment(project, profiles, "main", "test")
        for command, phase in (("parse", "PARSING"), ("build", "BUILDING")):
            spec = EngineCall((
                "dbt", command, "--project-dir", str(project), "--profiles-dir", str(profiles),
            ), project, environment, command == "build")
            result = await invoke_job_for_test(executor, job, spec, phase)
            assert result.status == JobStatus.SUCCEEDED
        with db.transaction() as connection:
            rows = connection.exec_driver_sql(
                "SELECT phase,command,level,event_name,node_id,invocation_id,message "
                "FROM runtime_execution_log WHERE job_id=%s ORDER BY log_id", (job.job_id,),
            ).mappings().all()
        assert {row["phase"] for row in rows} == {"PARSING", "BUILDING"}
        for row in rows:
            normalized = row["message"].replace("\\\\", "\\").replace("\\", "/")
            assert project.as_posix() not in normalized
            assert profiles.as_posix() not in normalized
        assert any(row["event_name"] == "MainReportArgs" and row["level"] == "debug" for row in rows)
        assert any(row["event_name"] == "MainReportVersion" for row in rows)
        assert any(row["event_name"] == "LogModelResult" and row["node_id"].startswith("model.") for row in rows)
        assert len({row["invocation_id"] for row in rows if row["invocation_id"]}) == 2


async def test_cancellation_waits_for_last_event_to_be_persisted(tmp_path, monkeypatch):
    with isolated_database() as db:
        db.initialize()
        service(SimpleNamespace(db=db)).submit_snapshot(request(), CALLER)
        jobs = JobStore(db)
        job = jobs.claim(str(uuid4()), toolchain_version=TOOLCHAIN)
        executor = BuildExecutor(SimpleNamespace(max_output_bytes=32), jobs, None)
        started, release = threading.Event(), threading.Event()

        def invoke(self, args):
            started.set()
            assert release.wait(5)
            for callback in self.callbacks:
                callback(msg_from_base_event(JinjaLogInfo(msg="last event after cancellation")))
            return SimpleNamespace(success=True, exception=None)

        monkeypatch.setattr(dbtRunner, "invoke", invoke)
        task = asyncio.create_task(invoke_job_for_test(executor,
            job, EngineCall(("dbt", "parse"), tmp_path, {}, False), "PARSING",
        ))
        try:
            assert await asyncio.to_thread(started.wait, 5)
            task.cancel()
            await asyncio.sleep(0)
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        with db.transaction() as connection:
            messages = connection.exec_driver_sql(
                "SELECT message FROM runtime_execution_log WHERE job_id=%s", (job.job_id,),
            ).scalars().all()
        assert messages == ["last event after cancellation"]


@pytest.mark.parametrize("failure", [None, "sql", "test", "parse", "deps"])
async def test_direct_build_uses_native_macros_and_records_failures(tmp_path, failure):
    import json

    from dbt_metricflow_service.execution.build import execute_build
    from dbt_metricflow_service.settings import Settings
    from dbt_metricflow_service.storage.artifacts import ArtifactStore
    from tests.resource_helpers import make_resource_project

    project, profiles, _ = make_resource_project(tmp_path)
    macro = project / "macros/generate_schema_name.sql"
    macro.parent.mkdir(exist_ok=True)
    macro.write_text("{% macro generate_schema_name(custom_schema_name, node) %}native_schema{% endmacro %}")
    model = project / "models/orders.sql"
    model.write_text("{{ config(alias='native_orders', pre_hook='select 1') }}\n" + model.read_text())
    if failure == "sql":
        model.write_text("select * from missing_build_table")
    elif failure == "test":
        tests = project / "tests"
        tests.mkdir(exist_ok=True)
        (tests / "must_fail.sql").write_text("select 1 as intentional_failure")
    elif failure == "parse":
        model.write_text("{{ undefined_build_macro() }}")
    elif failure == "deps":
        (project / "packages.yml").write_text("packages:\n  - package: dbt-labs/dbt_utils\n    version: 1.0.0\n")
    with isolated_database() as db:
        db.initialize()
        service(SimpleNamespace(db=db)).submit_snapshot(request(), CALLER)
        jobs = JobStore(db)
        job = jobs.claim(str(uuid4()), toolchain_version=TOOLCHAIN)
        job.profile_binding_id, job.schema_name = "test", "main"
        artifacts = ArtifactStore(db)
        executor = BuildExecutor(Settings(profiles_dir=profiles, max_output_bytes=1024), jobs, artifacts)
        if failure:
            with pytest.raises(ExecutionError) as error:
                await execute_build(executor, job, project)
            assert error.value.code == "COMMAND_FAILED"
        else:
            result = await execute_build(executor, job, project)
            with db.session() as session:
                artifacts.seal(result.output_set_id, session)
            manifest = json.loads(artifacts.read_file(result.output_set_id, "target/manifest.json"))
            node = next(node for node in manifest["nodes"].values() if node["name"] == "orders")
            assert node["schema"] == "native_schema"
            assert node["alias"] == "native_orders"
            assert not (project / "target/catalog.json").exists()
            assert not (project / "target/published_catalog.json").exists()
        with db.transaction() as connection:
            rows = connection.exec_driver_sql(
                "SELECT command,level,message FROM runtime_execution_log WHERE job_id=%s", (job.job_id,),
            ).mappings().all()
            outputs = connection.exec_driver_sql(
                "SELECT count(*) FROM runtime_artifact_set WHERE producer_attempt_id=%s", (job.attempt_id,),
            ).scalar_one()
        assert rows and {row["command"] for row in rows} == {"build"}
        if failure:
            assert outputs == 0
            assert any(row["level"] == "error" for row in rows)
            text = "\n".join(row["message"] for row in rows)
            expected = {"sql": "missing_build_table", "test": "must_fail", "parse": "undefined_build_macro",
                        "deps": "dbt deps"}
            assert expected[failure] in text
        else:
            assert outputs == 1
