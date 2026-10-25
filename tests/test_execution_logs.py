"""dbt 详细日志的采集、脱敏及故障隔离。"""

import logging
import os
from datetime import UTC
from types import SimpleNamespace

import pytest
from dbt.cli.main import dbtRunner
from dbt.events.types import JinjaLogDebug, JinjaLogInfo
from dbt_common.events.base_types import msg_from_base_event

from dbt_metricflow_service.execution.dbt import run_dbt
from dbt_metricflow_service.execution.models import CommandSpec, JobStatus


async def authorize():
    pass


async def test_structured_events_are_redacted_without_tail_truncation(tmp_path, monkeypatch):
    captured = []
    secret = "dynamic-private-password"
    original = msg_from_base_event(JinjaLogDebug(
        msg="begin " + secret + "x" * 1000,
        node_info={"unique_id": "model.project." + secret},
    ))

    def invoke(self, args):
        os.environ["DBT_ENV_SECRET_DYNAMIC_LOG"] = secret
        for callback in self.callbacks:
            callback(original)
            callback(msg_from_base_event(JinjaLogInfo(msg="finished")))
        logging.getLogger("dbt.adapter").warning("python %s", secret)
        return SimpleNamespace(success=True, exception=None)

    monkeypatch.setattr(dbtRunner, "invoke", invoke)
    record = await run_dbt(
        "project", CommandSpec(("dbt", "build"), tmp_path, {}, True), 5, 32, authorize,
        log_sink=captured.append,
    )
    assert record.status == JobStatus.SUCCEEDED
    assert record.output_truncated
    assert len(captured) == 3
    first = captured[0]
    assert first.message == "begin ***" + "x" * 1000
    assert first.node_id == "model.project.***"
    assert first.level == "debug"
    assert first.event_name == "JinjaLogDebug"
    assert first.event_code == "I063"
    assert first.event_time == original.info.ts.ToDatetime(tzinfo=UTC)
    assert first.invocation_id == original.info.invocation_id
    assert first.thread_name == original.info.thread
    assert captured[1].message == "finished"
    assert captured[2].message == "python ***"
    assert captured[2].level == "warning"
    assert secret not in repr(captured)


@pytest.mark.parametrize("exception", [SystemExit(2), KeyboardInterrupt()])
async def test_returned_base_exception_cannot_escape_worker(tmp_path, monkeypatch, exception):
    monkeypatch.setattr(dbtRunner, "invoke", lambda *_: SimpleNamespace(success=False, exception=exception))
    captured = []
    record = await run_dbt("project", CommandSpec(("dbt", "build"), tmp_path, {}, True),
                           5, 1024, authorize, log_sink=captured.append)
    assert record.status == JobStatus.FAILED
    assert record.exit_code == 2
    assert record.stderr == type(exception).__name__
    assert captured[-1].event_name == "DbtInvocationError"


@pytest.mark.parametrize("success", [False, True])
async def test_log_sink_failure_does_not_change_dbt_result(tmp_path, monkeypatch, caplog, success):
    def invoke(self, args):
        for callback in self.callbacks:
            callback(msg_from_base_event(JinjaLogInfo(msg="still available in stdout")))
        return SimpleNamespace(success=success, exception=None)

    def unavailable(entry):
        raise RuntimeError("private database connection details")

    monkeypatch.setattr(dbtRunner, "invoke", invoke)
    record = await run_dbt(
        "project", CommandSpec(("dbt", "parse"), tmp_path, {}, False), 5, 1024, authorize,
        log_sink=unavailable,
    )
    assert record.status == (JobStatus.SUCCEEDED if success else JobStatus.FAILED)
    assert "still available in stdout" in record.stdout
    assert "RuntimeError" in caplog.text
    assert "private database" not in caplog.text


@pytest.mark.parametrize("raises", [False, True])
async def test_sdk_exception_without_events_is_logged_and_redacted(tmp_path, monkeypatch, raises):
    secret = "private-build-password"
    monkeypatch.setenv("DBT_ENV_SECRET_TEST_PASSWORD", secret)
    captured = []
    def invoke(self, args):
        error = RuntimeError("invalid model: " + secret)
        if raises:
            raise error
        return SimpleNamespace(success=False, exception=error)
    monkeypatch.setattr(dbtRunner, "invoke", invoke)
    record = await run_dbt("project", CommandSpec(("dbt", "build"), tmp_path, {}, True),
                           5, 1024, authorize, log_sink=captured.append)
    assert record.status == JobStatus.FAILED
    assert captured and captured[-1].level == "error"
    assert "invalid model: ***" in captured[-1].message
    assert "invalid model: ***" in record.stdout
    assert secret not in repr(captured)


async def test_execution_paths_are_removed_before_log_sink_and_diagnostics(tmp_path, monkeypatch):
    project, profiles = tmp_path / "project", tmp_path / "profiles"
    captured = []

    def invoke(self, args):
        for callback in self.callbacks:
            callback(msg_from_base_event(JinjaLogInfo(msg=f"compiled {project / 'target/compiled/orders.sql'}")))
        logging.getLogger("dbt.adapter").warning("profile %s/profiles.yml", profiles.as_posix())
        return SimpleNamespace(success=False, exception=RuntimeError(repr(str(project)) + " models/orders.sql"))

    monkeypatch.setattr(dbtRunner, "invoke", invoke)
    environment = {"DBT_PROJECT_DIR": str(project), "DBT_PROFILES_DIR": str(profiles),
                   "DBT_TARGET_PATH": str(project / "target")}
    record = await run_dbt("project", CommandSpec(("dbt", "build"), project, environment, True),
                           5, 4096, authorize, log_sink=captured.append)
    assert record.status == JobStatus.FAILED
    for text in [record.stdout, *(entry.message for entry in captured)]:
        assert str(tmp_path) not in text
        assert tmp_path.as_posix() not in text
        assert repr(str(project))[1:-1] not in text
    assert "orders.sql" in record.stdout
    assert "profiles.yml" in record.stdout
