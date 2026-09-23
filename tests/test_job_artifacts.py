from __future__ import annotations

import dataclasses
import logging
from pathlib import Path
from uuid import uuid4

import pytest
from test_jobs import command_spec

from dbt_metricflow_service.job_artifacts import (
    FINISHED_MARKER,
    create_job_directory,
    recover_finished_directories,
    remove_job_directory,
)
from dbt_metricflow_service.jobs import JobRunner
from dbt_metricflow_service.models import JobStatus

logger = logging.getLogger(__name__)


def test_create_and_remove_uuid_job_directory(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    directory = create_job_directory(root, uuid4())
    assert directory.parent == root.resolve()
    assert directory.is_dir()
    remove_job_directory(root, directory)
    assert not directory.exists()


def test_cleanup_rejects_outside_directory(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    outside = tmp_path / "keep"
    root.mkdir()
    outside.mkdir()
    (outside / "keep.txt").write_text("keep", encoding="utf-8")
    with pytest.raises(ValueError):
        remove_job_directory(root, outside)
    assert (outside / "keep.txt").read_text(encoding="utf-8") == "keep"


def test_startup_recovery_only_removes_confirmed_finished_directories(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    finished = create_job_directory(root, uuid4())
    unfinished = create_job_directory(root, uuid4())
    unknown = root / "keep"
    unknown.mkdir()
    (finished / FINISHED_MARKER).touch()
    recover_finished_directories(root)
    assert not finished.exists()
    assert unfinished.exists()
    assert unknown.exists()


@pytest.mark.parametrize("mode", ["artifact-success", "artifact-failure"])
async def test_resource_job_artifacts_are_cleaned_after_exit(tmp_path: Path, mode: str) -> None:
    root = tmp_path / "artifacts"
    runner = JobRunner(timeout_seconds=2.0, max_output_bytes=512, job_artifacts_root=root)
    spec = dataclasses.replace(command_spec(tmp_path, mode), use_job_artifacts=True)
    submitted = await runner.submit("sales", spec)
    completed = await runner.wait(submitted.id)
    expected = JobStatus.SUCCEEDED if mode == "artifact-success" else JobStatus.FAILED
    assert completed.status is expected
    assert "JOB_ARTIFACT_DIR=" in completed.stdout
    assert root.is_dir()
    assert not list(root.iterdir())


async def test_resource_job_artifacts_are_cleaned_after_timeout(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    runner = JobRunner(timeout_seconds=0.1, max_output_bytes=128, job_artifacts_root=root)
    spec = dataclasses.replace(command_spec(tmp_path, "sleep", "5"), use_job_artifacts=True)
    submitted = await runner.submit("sales", spec)
    assert (await runner.wait(submitted.id)).status is JobStatus.TIMED_OUT
    assert not list(root.iterdir())


async def test_normal_job_does_not_create_artifact_root(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    runner = JobRunner(timeout_seconds=2.0, max_output_bytes=128, job_artifacts_root=root)
    submitted = await runner.submit("sales", command_spec(tmp_path, "success"))
    assert (await runner.wait(submitted.id)).status is JobStatus.SUCCEEDED
    assert not root.exists()
