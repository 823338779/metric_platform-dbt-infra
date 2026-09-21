from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterable
from datetime import UTC, datetime
from uuid import UUID, uuid4

from dbt_metricflow_service.models import CommandSpec, JobRecord, JobStatus

logger = logging.getLogger(__name__)

SECRET_ENVIRONMENT_PREFIX = "DBT_ENV_SECRET_"
REDACTION_MARKER = "***"
STREAM_READ_BYTES = 65_536
TERMINATE_GRACE_SECONDS = 5.0


class ProjectBusyError(RuntimeError):
    """A mutating command is already active for the requested project."""


class JobNotFoundError(KeyError):
    """The requested job is not retained by this service process."""


class _TailBuffer:
    """Continuously retain only the bounded byte tail of one output stream."""

    def __init__(self, max_bytes: int) -> None:
        self._max_bytes = max_bytes
        self._data = bytearray()
        self.truncated = False

    def append(self, chunk: bytes) -> None:
        """Add a drained stream chunk and discard bytes outside the tail."""
        self._data.extend(chunk)
        if len(self._data) > self._max_bytes:
            del self._data[: len(self._data) - self._max_bytes]
            self.truncated = True

    def decode(self) -> str:
        """Decode retained bytes even when truncation split a code point."""
        return self._data.decode("utf-8", errors="replace")


class JobRunner:
    """Execute literal CLI commands with bounded output and project isolation."""

    def __init__(self, timeout_seconds: float, max_output_bytes: int) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if max_output_bytes <= 0:
            raise ValueError("max_output_bytes must be positive")

        # All in-memory state shares one lock so submit, polling, and reservations are atomic.
        self._timeout_seconds = timeout_seconds
        self._max_output_bytes = max_output_bytes
        self._state_lock = asyncio.Lock()
        self._jobs: dict[UUID, JobRecord] = {}
        self._tasks: dict[UUID, asyncio.Task[None]] = {}
        self._busy_projects: set[str] = set()

    @property
    def max_output_bytes(self) -> int:
        """Return the per-stream byte retention limit."""
        return self._max_output_bytes

    async def submit(self, project: str, command: CommandSpec) -> JobRecord:
        """Reserve project mutation capacity and schedule one subprocess job."""
        async with self._state_lock:
            if command.write_operation and project in self._busy_projects:
                raise ProjectBusyError(project)

            job_id = uuid4()
            record = JobRecord(
                id=job_id,
                project=project,
                status=JobStatus.QUEUED,
                submitted_at=datetime.now(UTC),
            )
            self._jobs[job_id] = record
            if command.write_operation:
                self._busy_projects.add(project)
            self._tasks[job_id] = asyncio.create_task(
                self._run(job_id, project, command),
                name=f"job-{job_id}",
            )
            return record.model_copy(deep=True)

    async def get(self, job_id: UUID) -> JobRecord | None:
        """Return a detached job snapshot when the identifier is known."""
        async with self._state_lock:
            record = self._jobs.get(job_id)
            return record.model_copy(deep=True) if record is not None else None

    async def wait(self, job_id: UUID) -> JobRecord:
        """Wait for one known job without allowing caller cancellation to stop it."""
        async with self._state_lock:
            task = self._tasks.get(job_id)
        if task is None:
            raise JobNotFoundError(str(job_id))

        await asyncio.shield(task)
        record = await self.get(job_id)
        if record is None:
            raise JobNotFoundError(str(job_id))
        return record

    async def _run(self, job_id: UUID, project: str, command: CommandSpec) -> None:
        """Own the complete process lifecycle and always release reservations."""
        stdout_buffer = _TailBuffer(self._max_output_bytes)
        stderr_buffer = _TailBuffer(self._max_output_bytes)
        status = JobStatus.FAILED
        exit_code: int | None = None
        process: asyncio.subprocess.Process | None = None
        readers: tuple[asyncio.Task[None], ...] = ()

        await self._replace_record(
            job_id,
            status=JobStatus.RUNNING,
            started_at=datetime.now(UTC),
        )
        try:
            # Arguments and environment are already constructed from validated service inputs.
            process = await asyncio.create_subprocess_exec(
                *command.argv,
                cwd=command.cwd,
                env=dict(command.environment),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            if process.stdout is None or process.stderr is None:
                raise RuntimeError("subprocess pipes were not created")
            readers = (
                asyncio.create_task(self._drain(process.stdout, stdout_buffer)),
                asyncio.create_task(self._drain(process.stderr, stderr_buffer)),
            )
            try:
                async with asyncio.timeout(self._timeout_seconds):
                    exit_code = await process.wait()
                status = JobStatus.SUCCEEDED if exit_code == 0 else JobStatus.FAILED
            except TimeoutError:
                status = JobStatus.TIMED_OUT
                await self._terminate(process)
        except OSError as error:
            stderr_buffer.append(str(error).encode("utf-8", errors="replace"))
        finally:
            if readers:
                await asyncio.gather(*readers)
            secrets = self._secret_values(command.environment.items())
            await self._replace_record(
                job_id,
                status=status,
                finished_at=datetime.now(UTC),
                exit_code=exit_code,
                stdout=self._redact(stdout_buffer.decode(), secrets),
                stderr=self._redact(stderr_buffer.decode(), secrets),
                output_truncated=stdout_buffer.truncated or stderr_buffer.truncated,
            )
            if command.write_operation:
                async with self._state_lock:
                    self._busy_projects.discard(project)

    async def _replace_record(self, job_id: UUID, **updates: object) -> None:
        """Replace one immutable record while holding the shared state lock."""
        async with self._state_lock:
            self._jobs[job_id] = self._jobs[job_id].model_copy(update=updates)

    @staticmethod
    async def _drain(stream: asyncio.StreamReader, buffer: _TailBuffer) -> None:
        """Drain one pipe to EOF so large output cannot block the child."""
        while chunk := await stream.read(STREAM_READ_BYTES):
            buffer.append(chunk)

    @staticmethod
    async def _terminate(process: asyncio.subprocess.Process) -> None:
        """Terminate a timed-out process, escalating after a fixed grace period."""
        process.terminate()
        try:
            await asyncio.wait_for(process.wait(), timeout=TERMINATE_GRACE_SECONDS)
        except TimeoutError:
            process.kill()
            await process.wait()

    @staticmethod
    def _secret_values(environment: Iterable[tuple[str, str]]) -> tuple[str, ...]:
        """Extract only non-empty dbt secret values without retaining the full environment."""
        return tuple(
            value
            for key, value in environment
            if key.startswith(SECRET_ENVIRONMENT_PREFIX) and value
        )

    @staticmethod
    def _redact(output: str, secrets: Iterable[str]) -> str:
        """Replace exact dbt secret values before output enters a job record."""
        for secret in secrets:
            output = output.replace(secret, REDACTION_MARKER)
        return output
