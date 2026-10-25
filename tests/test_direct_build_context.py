"""生产构建直接调用 client，保留租约、日志和取消清理边界。"""
import asyncio
import os
import threading
from types import SimpleNamespace

import pytest
from dbt.events.types import JinjaLogInfo
from dbt_common.events.base_types import msg_from_base_event

from dbt_metricflow_service.delegates.clients.dbt_client import DbtClient
from dbt_metricflow_service.execution.embedded import ENGINE_LOCK
from dbt_metricflow_service.platform.build import execute_build
from dbt_metricflow_service.platform.manifest_capture import ManifestCapture
from dbt_metricflow_service.runtime.dbt_executor import ExecutionError, RuntimeExecutor
from dbt_metricflow_service.storage.execution_logs import ExecutionLogStore
from tests.data_helpers import leased_job
from tests.test_manifest_capture import manifest_object


@pytest.mark.parametrize("outcome", ["success", "lease_lost", "failure", "missing_manifest", "capture_failure"])
async def test_build_calls_client_and_only_persists_success(tmp_path, monkeypatch, outcome):
    (tmp_path / "dbt_project.yml").write_text("name: test\n")
    events, saved, phases = [], [], []
    def phase(*args, **kwargs):
        phases.append(kwargs)
        return outcome != "lease_lost"
    executor = RuntimeExecutor(SimpleNamespace(
        profiles_dir=tmp_path, max_output_bytes=1024, max_artifact_file_bytes=1000000,
    ), SimpleNamespace(db=None, phase=phase), object())
    job = leased_job(project_id="test", schema_name="main", profile_binding_id="test")
    manifest = manifest_object({"nodes": {}})
    def build(self, project, profiles, **kwargs):
        assert ENGINE_LOCK.locked()
        assert phases == [{"external": True}]
        assert project == profiles == tmp_path
        for callback in kwargs["callbacks"]:
            callback(msg_from_base_event(JinjaLogInfo(msg="built")))
        return SimpleNamespace(success=outcome != "failure", exception=None,
                               manifest=None if outcome == "missing_manifest" else manifest)
    def persist(self, *args, **kwargs):
        assert self.manifest is manifest
        saved.append(True)
        return "saved-artifacts"
    monkeypatch.setattr(DbtClient, "build", build)
    monkeypatch.setattr(ManifestCapture, "persist", persist)
    monkeypatch.setattr(ExecutionLogStore, "append", lambda self, *args: events.extend(args[-1]))
    if outcome == "capture_failure":
        def fail_capture(*args):
            raise ValueError("capture failed")
        monkeypatch.setattr(ManifestCapture, "__call__", fail_capture)
    before = dict(os.environ)
    if outcome == "success":
        assert (await execute_build(executor, job, tmp_path)).output_set_id == "saved-artifacts"
        assert saved == [True]
    else:
        with pytest.raises(ExecutionError) as caught:
            await execute_build(executor, job, tmp_path)
        assert caught.value.code == ("LEASE_LOST" if outcome == "lease_lost" else "COMMAND_FAILED")
        assert not saved
    assert dict(os.environ) == before
    assert not ENGINE_LOCK.locked()
    assert bool(events) == (outcome != "lease_lost")


@pytest.mark.parametrize("late_error", [False, True])
async def test_cancelled_build_flushes_final_event_without_persisting(tmp_path, monkeypatch, late_error):
    started, release = threading.Event(), threading.Event()
    events = []
    executor = RuntimeExecutor(SimpleNamespace(
        profiles_dir=tmp_path, max_output_bytes=1024, max_artifact_file_bytes=1000000,
    ), SimpleNamespace(db=None, phase=lambda *args, **kwargs: True), object())
    def build(self, *args, **kwargs):
        started.set()
        assert release.wait(5)
        for callback in kwargs["callbacks"]:
            callback(msg_from_base_event(JinjaLogInfo(msg="final event")))
        if late_error:
            raise ValueError("late failure")
        return SimpleNamespace(success=True, exception=None, manifest=manifest_object({"nodes": {}}))
    monkeypatch.setattr(DbtClient, "build", build)
    monkeypatch.setattr(ExecutionLogStore, "append", lambda self, *args: events.extend(args[-1]))
    monkeypatch.setattr(ManifestCapture, "persist", lambda *args, **kwargs: pytest.fail("cancelled build persisted"))
    before = dict(os.environ)
    task = asyncio.create_task(execute_build(executor, leased_job(), tmp_path))
    try:
        assert await asyncio.to_thread(started.wait, 5)
        task.cancel()
        await asyncio.sleep(0.02)
        assert not task.done()
        assert ENGINE_LOCK.locked()
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert events[0].message == "final event"
    if late_error:
        assert events[-1].message == "ValueError: late failure"
    assert dict(os.environ) == before
    assert not ENGINE_LOCK.locked()
