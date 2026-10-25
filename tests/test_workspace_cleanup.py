import asyncio
import threading
from types import SimpleNamespace
from uuid import uuid4

import pytest

from dbt_metricflow_service.execution.build import BuildExecutor
from dbt_metricflow_service.execution.workspace import attempt_workspace


@pytest.mark.parametrize("outcome", ["success", "failure", "cancel"])
def test_attempt_removes_entire_job_directory(tmp_path, outcome):
    job_id = str(uuid4())
    other = tmp_path / "other-job"
    other.mkdir()
    marker = other / "keep.txt"
    marker.write_text("keep")

    def execute():
        with attempt_workspace(tmp_path, job_id, str(uuid4())) as directory:
            target = directory / "project" / "target"
            target.mkdir(parents=True)
            (target / "manifest.json").write_text("{}")
            if outcome == "failure":
                raise RuntimeError("build failed")
            if outcome == "cancel":
                raise asyncio.CancelledError

    if outcome == "success":
        execute()
    else:
        error = RuntimeError if outcome == "failure" else asyncio.CancelledError
        with pytest.raises(error):
            execute()
    assert not (tmp_path / job_id).exists()
    assert marker.read_text() == "keep"


def test_cleanup_preserves_another_attempt(tmp_path):
    job_id = str(uuid4())
    with attempt_workspace(tmp_path, job_id, str(uuid4())) as first:
        with attempt_workspace(tmp_path, job_id, str(uuid4())) as second:
            (first / "keep.txt").write_text("keep")
        assert not second.exists()
        assert (first / "keep.txt").read_text() == "keep"
    assert not (tmp_path / job_id).exists()


async def test_cancel_during_source_resolution_removes_source_directory(settings, monkeypatch):
    started = threading.Event()
    release = threading.Event()
    directories = []

    def resolve(binding, sha, root):
        directory = root / "run-source"
        directory.mkdir()
        directories.append(directory)
        started.set()
        assert release.wait(5)
        (directory / "dbt_project.yml").write_text("name: test")
        return directory, "digest"

    monkeypatch.setattr("dbt_metricflow_service.platform.bindings.resolve_commit", resolve)
    monkeypatch.setattr("dbt_metricflow_service.storage.builds.BuildStore.pin_source", lambda *args: None)
    job = SimpleNamespace(
        job_id=str(uuid4()), attempt_id=str(uuid4()), input_set_id=None,
        project_id="project", profile_binding_id="profile", lease_token="lease",
        request_json=SimpleNamespace(build_id="build", execution=True, commit_sha="b" * 40,
                                     repository="repo", branch_name="main"),
    )
    jobs = SimpleNamespace(db=None, phase=lambda *args: None)
    executor = BuildExecutor(settings, jobs, None)
    task = asyncio.create_task(executor.execute(job))
    try:
        assert await asyncio.to_thread(started.wait, 5)
        task.cancel()
        await asyncio.sleep(0)
        assert directories[0].exists()
    finally:
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert not directories[0].exists()
    assert not (settings.temp_root / job.job_id).exists()
