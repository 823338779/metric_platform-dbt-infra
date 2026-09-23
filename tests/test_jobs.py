from __future__ import annotations

import asyncio
import dataclasses
import logging
import os
import sys
import time
from pathlib import Path

import pytest

from dbt_metricflow_service.jobs import JobRunner, ProjectBusyError
from dbt_metricflow_service.models import CommandSpec, JobStatus

logger = logging.getLogger(__name__)

FAKE_CLI_PATH = Path(__file__).parent / "fixtures" / "fake_cli.py"
SECRET_ENVIRONMENT_KEY = "DBT_ENV_SECRET_TEST_TOKEN"
SECRET_VALUE = "never-return-this-secret"


def command_spec(
    tmp_path: Path,
    *arguments: str,
    write_operation: bool = False,
    environment: dict[str, str] | None = None,
) -> CommandSpec:
    """Build a literal Python subprocess command for one lifecycle scenario."""
    return CommandSpec(
        argv=(sys.executable, "-u", str(FAKE_CLI_PATH), *arguments),
        cwd=tmp_path,
        environment=environment or dict(os.environ),
        write_operation=write_operation,
    )


@pytest.fixture
def runner() -> JobRunner:
    """Provide limits large enough for ordinary lifecycle tests."""
    return JobRunner(timeout_seconds=2.0, max_output_bytes=128)


async def test_successful_job_captures_both_streams(
    runner: JobRunner, tmp_path: Path
) -> None:
    """A zero exit code must produce a completed immutable result."""
    submitted = await runner.submit("sales", command_spec(tmp_path, "success"))

    completed = await runner.wait(submitted.id)

    assert submitted.status is JobStatus.QUEUED
    assert completed.status is JobStatus.SUCCEEDED
    assert completed.exit_code == 0
    assert completed.stdout.strip() == "success stdout"
    assert completed.stderr.strip() == "success stderr"
    assert completed.started_at is not None
    assert completed.finished_at is not None
    assert completed.started_at.tzinfo is not None
    assert completed.finished_at.tzinfo is not None


async def test_nonzero_exit_code_marks_job_failed(
    runner: JobRunner, tmp_path: Path
) -> None:
    """A child failure must retain its exit code and stderr."""
    submitted = await runner.submit("sales", command_spec(tmp_path, "failure"))

    completed = await runner.wait(submitted.id)

    assert completed.status is JobStatus.FAILED
    assert completed.exit_code == 7
    assert completed.stderr.strip() == "failure stderr"


async def test_timeout_terminates_job(tmp_path: Path) -> None:
    """A command exceeding its wall clock limit must end as timed out."""
    runner = JobRunner(timeout_seconds=0.1, max_output_bytes=128)
    submitted = await runner.submit("sales", command_spec(tmp_path, "sleep", "5"))

    completed = await runner.wait(submitted.id)

    assert completed.status is JobStatus.TIMED_OUT
    assert completed.finished_at is not None


async def test_large_output_keeps_bounded_tail(tmp_path: Path) -> None:
    """Only the configured tail of a continuously drained stream is retained."""
    runner = JobRunner(timeout_seconds=2.0, max_output_bytes=32)
    submitted = await runner.submit("sales", command_spec(tmp_path, "large-output", "512"))

    completed = await runner.wait(submitted.id)

    assert len(completed.stdout.encode()) <= runner.max_output_bytes
    assert completed.stdout.rstrip().endswith("-tail")
    assert completed.output_truncated is True


async def test_second_write_job_for_same_project_is_rejected(
    runner: JobRunner, tmp_path: Path
) -> None:
    """A project mutation reservation must be acquired before submit returns."""
    slow_write = command_spec(tmp_path, "sleep", "0.25", write_operation=True)
    first = await runner.submit("sales", slow_write)

    with pytest.raises(ProjectBusyError):
        await runner.submit("sales", slow_write)

    assert (await runner.wait(first.id)).status is JobStatus.SUCCEEDED


async def test_write_jobs_for_different_projects_run_concurrently(
    runner: JobRunner, tmp_path: Path
) -> None:
    """Project locking must not serialize independent project mutations."""
    sales_dir = tmp_path / "sales"
    finance_dir = tmp_path / "finance"
    sales_dir.mkdir()
    finance_dir.mkdir()
    sales = await runner.submit(
        "sales", command_spec(sales_dir, "sleep", "0.25", write_operation=True)
    )
    finance = await runner.submit(
        "finance", command_spec(finance_dir, "sleep", "0.25", write_operation=True)
    )

    await asyncio.sleep(0.1)
    sales_running = await runner.get(sales.id)
    finance_running = await runner.get(finance.id)

    assert sales_running is not None and sales_running.status is JobStatus.RUNNING
    assert finance_running is not None and finance_running.status is JobStatus.RUNNING
    completed = await asyncio.gather(runner.wait(sales.id), runner.wait(finance.id))
    assert {job.status for job in completed} == {JobStatus.SUCCEEDED}


async def test_secret_environment_values_are_redacted(tmp_path: Path) -> None:
    """Secret env values emitted by a child must never enter stored output."""
    environment = dict(os.environ)
    environment[SECRET_ENVIRONMENT_KEY] = SECRET_VALUE
    runner = JobRunner(timeout_seconds=2.0, max_output_bytes=128)
    submitted = await runner.submit(
        "sales",
        command_spec(tmp_path, "echo", SECRET_VALUE, environment=environment),
    )

    completed = await runner.wait(submitted.id)

    assert SECRET_VALUE not in completed.stdout
    assert SECRET_VALUE not in completed.stderr
    assert completed.stdout.strip() == "***"
    assert completed.stderr.strip() == "***"


async def test_secret_is_redacted_before_output_tail_is_truncated(tmp_path: Path) -> None:
    """A secret longer than the retained tail must not leak its suffix."""
    environment = dict(os.environ)
    environment[SECRET_ENVIRONMENT_KEY] = SECRET_VALUE
    runner = JobRunner(timeout_seconds=2.0, max_output_bytes=10)
    submitted = await runner.submit(
        "sales",
        command_spec(tmp_path, "echo", SECRET_VALUE, environment=environment),
    )

    completed = await runner.wait(submitted.id)

    assert "SECRET" not in completed.stdout
    assert completed.stdout.strip() == "***"


async def test_secret_redaction_handles_stream_boundaries_and_overlaps(tmp_path: Path) -> None:
    """Chunk boundaries and overlapping secret values must not weaken redaction."""
    environment = dict(os.environ)
    environment[SECRET_ENVIRONMENT_KEY] = SECRET_VALUE
    environment["DBT_ENV_SECRET_TEST_SUFFIX"] = "secret"
    runner = JobRunner(timeout_seconds=2.0, max_output_bytes=128)
    submitted = await runner.submit(
        "sales",
        command_spec(
            tmp_path,
            "split-echo",
            SECRET_VALUE,
            "7",
            environment=environment,
        ),
    )

    completed = await runner.wait(submitted.id)

    assert completed.stdout == "***"


async def test_same_project_directory_alias_cannot_bypass_write_lock(
    runner: JobRunner, tmp_path: Path
) -> None:
    """The canonical cwd, rather than the request spelling, must own the lock."""
    slow_write = command_spec(tmp_path, "sleep", "0.25", write_operation=True)
    first = await runner.submit("sales", slow_write)

    with pytest.raises(ProjectBusyError):
        await runner.submit("SALES_ALIAS", slow_write)

    await runner.wait(first.id)


async def test_timeout_terminates_descendants_that_hold_output_pipes(tmp_path: Path) -> None:
    """A timed-out process tree must not keep readers and the project lock alive."""
    runner = JobRunner(timeout_seconds=0.1, max_output_bytes=128)
    started = time.monotonic()
    submitted = await runner.submit(
        "sales",
        command_spec(tmp_path, "child-holds-pipe", "2", write_operation=True),
    )

    completed = await runner.wait(submitted.id)

    assert completed.status is JobStatus.TIMED_OUT
    assert time.monotonic() - started < 1.5


async def test_close_cancels_and_terminates_active_process(tmp_path: Path) -> None:
    """Application shutdown must bound active job cleanup."""
    runner = JobRunner(timeout_seconds=30.0, max_output_bytes=128)
    submitted = await runner.submit(
        "sales",
        command_spec(tmp_path, "sleep", "5", write_operation=True),
    )
    await asyncio.sleep(0.1)
    started = time.monotonic()

    await runner.close()

    assert time.monotonic() - started < 1.5
    completed = await runner.get(submitted.id)
    assert completed is not None and completed.status is JobStatus.FAILED


async def test_completed_job_retention_is_bounded(tmp_path: Path) -> None:
    """Finished records and task objects must not grow without a fixed limit."""
    runner = JobRunner(timeout_seconds=2.0, max_output_bytes=128, max_completed_jobs=2)
    completed_ids = []
    for project in ("one", "two", "three"):
        submitted = await runner.submit(project, command_spec(tmp_path, "success"))
        completed_ids.append((await runner.wait(submitted.id)).id)

    assert await runner.get(completed_ids[0]) is None
    assert await runner.get(completed_ids[1]) is not None
    assert await runner.get(completed_ids[2]) is not None


async def test_unknown_job_returns_none(runner: JobRunner) -> None:
    """Polling an absent UUID must be distinguishable from a queued job."""
    from uuid import uuid4

    assert await runner.get(uuid4()) is None


async def test_stdin_payload_is_delivered_without_deadlocking_output(tmp_path: Path) -> None:
    payload = b"x" * (512 * 1024)
    spec = dataclasses.replace(
        command_spec(tmp_path, "stdout-before-stdin"),
        stdin_data=payload,
    )
    runner = JobRunner(timeout_seconds=3.0, max_output_bytes=128)
    submitted = await runner.submit("sales", spec)
    completed = await runner.wait(submitted.id)
    assert completed.status is JobStatus.SUCCEEDED
    assert str(len(payload)) in completed.stdout
    assert payload.decode() not in repr(completed)


async def test_child_exit_before_stdin_preserves_exit_code(tmp_path: Path) -> None:
    spec = dataclasses.replace(
        command_spec(tmp_path, "exit-before-stdin"),
        stdin_data=b"x" * (512 * 1024),
    )
    runner = JobRunner(timeout_seconds=2.0, max_output_bytes=128)
    submitted = await runner.submit("sales", spec)
    completed = await runner.wait(submitted.id)
    assert completed.status is JobStatus.FAILED
    assert completed.exit_code == 7


async def test_stdin_writer_is_inside_job_timeout(tmp_path: Path) -> None:
    spec = dataclasses.replace(
        command_spec(tmp_path, "never-read-stdin"),
        stdin_data=b"x" * (512 * 1024),
    )
    runner = JobRunner(timeout_seconds=0.1, max_output_bytes=128)
    submitted = await runner.submit("sales", spec)
    completed = await runner.wait(submitted.id)
    assert completed.status is JobStatus.TIMED_OUT
