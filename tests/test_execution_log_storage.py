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

from dbt_metricflow_service.execution.logs import EngineLog
from dbt_metricflow_service.execution.models import CommandSpec, JobStatus
from dbt_metricflow_service.runtime.executor import ExecutionError, RuntimeExecutor
from dbt_metricflow_service.storage.jobs import JobStore
from tests.schema_helpers import isolated_database
from tests.test_build_acceptance import CALLER, TOOLCHAIN, request, service


def entry(message):
    return EngineLog(datetime.now(UTC), "info", message, event_name="JinjaLogInfo")


def test_writer_flushes_while_running_and_drains_on_close():
    from dbt_metricflow_service.runtime.execution_logs import ExecutionLogWriter

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
    from dbt_metricflow_service.runtime.execution_logs import ExecutionLogWriter

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
    import dbt_metricflow_service.runtime.execution_logs as module

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
        executor = RuntimeExecutor(SimpleNamespace(command_timeout_seconds=5, max_output_bytes=32), jobs, None)

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
            spec = CommandSpec(("dbt", "parse"), tmp_path, {}, False)
            if success:
                result = await executor._dbt(job, spec, "PARSING")
                assert result.status == JobStatus.SUCCEEDED
            else:
                with pytest.raises(ExecutionError):
                    await executor._dbt(job, spec, "PARSING")
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
    from dbt_metricflow_service.runtime.executor import build_engine_environment
    from tests.resource_helpers import make_resource_project

    project, profiles, _ = make_resource_project(tmp_path)
    with isolated_database() as db:
        db.initialize()
        service(SimpleNamespace(db=db)).submit_snapshot(request(), CALLER)
        jobs = JobStore(db)
        job = jobs.claim(str(uuid4()), toolchain_version=TOOLCHAIN)
        executor = RuntimeExecutor(SimpleNamespace(command_timeout_seconds=60, max_output_bytes=32), jobs, None)
        environment = build_engine_environment(project, profiles, "main", "test")
        for command, phase in (("parse", "PARSING"), ("build", "BUILDING")):
            spec = CommandSpec((
                "dbt", command, "--project-dir", str(project), "--profiles-dir", str(profiles),
            ), project, environment, command == "build")
            result = await executor._dbt(job, spec, phase)
            assert result.status == JobStatus.SUCCEEDED
        with db.transaction() as connection:
            rows = connection.exec_driver_sql(
                "SELECT phase,command,level,event_name,node_id,invocation_id "
                "FROM runtime_execution_log WHERE job_id=%s ORDER BY log_id", (job.job_id,),
            ).mappings().all()
        assert {row["phase"] for row in rows} == {"PARSING", "BUILDING"}
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
        executor = RuntimeExecutor(SimpleNamespace(command_timeout_seconds=10, max_output_bytes=32), jobs, None)
        started, release = threading.Event(), threading.Event()

        def invoke(self, args):
            started.set()
            assert release.wait(5)
            for callback in self.callbacks:
                callback(msg_from_base_event(JinjaLogInfo(msg="last event after cancellation")))
            return SimpleNamespace(success=True, exception=None)

        monkeypatch.setattr(dbtRunner, "invoke", invoke)
        task = asyncio.create_task(executor._dbt(
            job, CommandSpec(("dbt", "parse"), tmp_path, {}, False), "PARSING",
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
