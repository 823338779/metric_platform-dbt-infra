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
