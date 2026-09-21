from __future__ import annotations

import asyncio
import logging
import os
import sys
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
    slow_write = command_spec(tmp_path, "sleep", "0.25", write_operation=True)
    sales = await runner.submit("sales", slow_write)
    finance = await runner.submit("finance", slow_write)

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


async def test_unknown_job_returns_none(runner: JobRunner) -> None:
    """Polling an absent UUID must be distinguishable from a queued job."""
    from uuid import uuid4

    assert await runner.get(uuid4()) is None
