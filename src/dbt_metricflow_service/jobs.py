from __future__ import annotations

import asyncio
import codecs
import logging
import os
import signal
import subprocess
from collections import deque
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

from dbt_metricflow_service.models import CommandSpec, JobRecord, JobStatus

logger = logging.getLogger(__name__)

SECRET_ENVIRONMENT_PREFIX = "DBT_ENV_SECRET_"
REDACTION_MARKER = "***"
STREAM_READ_BYTES = 65_536
TERMINATE_GRACE_SECONDS = 5.0
DEFAULT_MAX_COMPLETED_JOBS = 100
FINAL_JOB_STATUSES = frozenset(
    {JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.TIMED_OUT}
)


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
        """Add a redacted stream chunk and discard bytes outside the tail."""
        self._data.extend(chunk)
        if len(self._data) > self._max_bytes:
            del self._data[: len(self._data) - self._max_bytes]
            self.truncated = True

    def decode(self) -> str:
        """Decode retained bytes even when truncation split a code point."""
        return self._data.decode("utf-8", errors="replace")


class _RedactingTailBuffer:
    """Redact exact secrets across chunks before applying the byte tail limit."""

    def __init__(self, max_bytes: int, secrets: Iterable[str]) -> None:
        self._tail = _TailBuffer(max_bytes)
        self._secrets = tuple(sorted(set(secrets), key=len, reverse=True))
        self._max_secret_length = max((len(secret) for secret in self._secrets), default=0)
        self._pending = ""
        self._decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")

    @property
    def truncated(self) -> bool:
        """Return whether redacted output exceeded the configured byte limit."""
        return self._tail.truncated

    def feed(self, chunk: bytes) -> None:
        """Decode and redact complete matches while retaining a cross-chunk suffix."""
        self._process(self._decoder.decode(chunk, final=False), final=False)

    def finish(self) -> None:
        """Flush the decoder and redact any remaining suffix at stream EOF."""
        self._process(self._decoder.decode(b"", final=True), final=True)

    def decode(self) -> str:
        """Return the retained redacted text tail."""
        return self._tail.decode()

    def _process(self, text: str, *, final: bool) -> None:
        combined = self._pending + text
        if final or self._max_secret_length == 0:
            cutoff = len(combined)
        else:
            cutoff = max(0, len(combined) - self._max_secret_length + 1)

        output: list[str] = []
        index = 0
        while index < cutoff:
            secret = next(
                (candidate for candidate in self._secrets if combined.startswith(candidate, index)),
                None,
            )
            if secret is not None:
                output.append(REDACTION_MARKER)
                index += len(secret)
            else:
                output.append(combined[index])
                index += 1
        self._pending = combined[index:]
        if output:
            self._tail.append("".join(output).encode("utf-8"))


class JobRunner:
    """Execute literal CLI commands with bounded output and project isolation."""

    def __init__(
        self,
        timeout_seconds: float,
        max_output_bytes: int,
        max_completed_jobs: int = DEFAULT_MAX_COMPLETED_JOBS,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if max_output_bytes <= 0:
            raise ValueError("max_output_bytes must be positive")
        if max_completed_jobs <= 0:
            raise ValueError("max_completed_jobs must be positive")

        # All in-memory state shares one lock so submit, polling, and reservations are atomic.
        self._timeout_seconds = timeout_seconds
        self._max_output_bytes = max_output_bytes
        self._max_completed_jobs = max_completed_jobs
        self._state_lock = asyncio.Lock()
        self._jobs: dict[UUID, JobRecord] = {}
        self._tasks: dict[UUID, asyncio.Task[None]] = {}
        self._completed_jobs: deque[UUID] = deque()
        self._busy_projects: set[str] = set()

    @property
    def max_output_bytes(self) -> int:
        """Return the per-stream byte retention limit."""
        return self._max_output_bytes

    async def submit(self, project: str, command: CommandSpec) -> JobRecord:
        """Reserve canonical project mutation capacity and schedule one subprocess job."""
        lock_key = self._project_lock_key(command.cwd)
        async with self._state_lock:
            if command.write_operation and lock_key in self._busy_projects:
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
                self._busy_projects.add(lock_key)
            self._tasks[job_id] = asyncio.create_task(
                self._run(job_id, lock_key, command),
                name=f"job-{job_id}",
            )
            return record.model_copy(deep=True)

    async def get(self, job_id: UUID) -> JobRecord | None:
        """Return a detached job snapshot when the identifier is retained."""
        async with self._state_lock:
            record = self._jobs.get(job_id)
            return record.model_copy(deep=True) if record is not None else None

    async def wait(self, job_id: UUID) -> JobRecord:
        """Wait for one retained job without caller cancellation stopping it."""
        async with self._state_lock:
            record = self._jobs.get(job_id)
            task = self._tasks.get(job_id)
            if record is None:
                raise JobNotFoundError(str(job_id))
            if task is None and record.status in FINAL_JOB_STATUSES:
                return record.model_copy(deep=True)
        if task is None:
            raise JobNotFoundError(str(job_id))

        await asyncio.shield(task)
        record = await self.get(job_id)
        if record is None:
            raise JobNotFoundError(str(job_id))
        return record

    async def close(self) -> None:
        """Cancel active jobs and bound all child-process cleanup during shutdown."""
        async with self._state_lock:
            active_tasks = tuple(self._tasks.values())
        for task in active_tasks:
            task.cancel()
        if active_tasks:
            await asyncio.gather(*active_tasks, return_exceptions=True)

        # A task cancelled before its coroutine starts cannot run its own finalizer.
        async with self._state_lock:
            now = datetime.now(UTC)
            for job_id, record in tuple(self._jobs.items()):
                if record.status not in FINAL_JOB_STATUSES:
                    self._jobs[job_id] = record.model_copy(
                        update={"status": JobStatus.FAILED, "finished_at": now}
                    )
                    self._retain_completed_locked(job_id)
            self._tasks.clear()
            self._busy_projects.clear()

    async def _run(self, job_id: UUID, lock_key: str, command: CommandSpec) -> None:
        """Own the complete process tree lifecycle and always release reservations."""
        secrets = self._secret_values(command.environment.items())
        stdout_buffer = _RedactingTailBuffer(self._max_output_bytes, secrets)
        stderr_buffer = _RedactingTailBuffer(self._max_output_bytes, secrets)
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
            # A dedicated process group lets timeout and shutdown include descendants.
            process_options: dict[str, object]
            if os.name == "nt":
                process_options = {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP}
            else:
                process_options = {"start_new_session": True}
            process = await asyncio.create_subprocess_exec(
                *command.argv,
                cwd=command.cwd,
                env=dict(command.environment),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                **process_options,
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
                await asyncio.shield(self._terminate(process, readers))
        except asyncio.CancelledError:
            if process is not None and process.returncode is None:
                await asyncio.shield(self._terminate(process, readers))
            raise
        except (OSError, RuntimeError) as error:
            stderr_buffer.feed(str(error).encode("utf-8", errors="replace"))
        finally:
            await asyncio.shield(self._finish_readers(readers))
            stdout_buffer.finish()
            stderr_buffer.finish()
            await asyncio.shield(
                self._finalize(
                    job_id,
                    lock_key,
                    command.write_operation,
                    status,
                    exit_code,
                    stdout_buffer,
                    stderr_buffer,
                )
            )

    async def _replace_record(self, job_id: UUID, **updates: object) -> None:
        """Replace one immutable record while holding the shared state lock."""
        async with self._state_lock:
            self._jobs[job_id] = self._jobs[job_id].model_copy(update=updates)

    async def _finalize(
        self,
        job_id: UUID,
        lock_key: str,
        write_operation: bool,
        status: JobStatus,
        exit_code: int | None,
        stdout_buffer: _RedactingTailBuffer,
        stderr_buffer: _RedactingTailBuffer,
    ) -> None:
        """Publish final output, release locks, and enforce history retention atomically."""
        async with self._state_lock:
            record = self._jobs.get(job_id)
            if record is not None:
                self._jobs[job_id] = record.model_copy(
                    update={
                        "status": status,
                        "finished_at": datetime.now(UTC),
                        "exit_code": exit_code,
                        "stdout": stdout_buffer.decode(),
                        "stderr": stderr_buffer.decode(),
                        "output_truncated": (
                            stdout_buffer.truncated or stderr_buffer.truncated
                        ),
                    }
                )
                self._retain_completed_locked(job_id)
            self._tasks.pop(job_id, None)
            if write_operation:
                self._busy_projects.discard(lock_key)

    def _retain_completed_locked(self, job_id: UUID) -> None:
        """Retain only the newest configured number of completed records."""
        if job_id not in self._completed_jobs:
            self._completed_jobs.append(job_id)
        while len(self._completed_jobs) > self._max_completed_jobs:
            expired_id = self._completed_jobs.popleft()
            self._jobs.pop(expired_id, None)
            self._tasks.pop(expired_id, None)

    @staticmethod
    async def _drain(stream: asyncio.StreamReader, buffer: _RedactingTailBuffer) -> None:
        """Drain one pipe to EOF so large output cannot block the child."""
        while chunk := await stream.read(STREAM_READ_BYTES):
            buffer.feed(chunk)

    @classmethod
    async def _terminate(
        cls,
        process: asyncio.subprocess.Process,
        readers: tuple[asyncio.Task[None], ...],
    ) -> None:
        """Terminate a process tree and escalate when pipes remain open."""
        await cls._signal_process_tree(process, force=False)
        process_wait = asyncio.create_task(process.wait())
        _, pending = await asyncio.wait(
            (process_wait, *readers),
            timeout=TERMINATE_GRACE_SECONDS,
            return_when=asyncio.ALL_COMPLETED,
        )
        if pending:
            await cls._signal_process_tree(process, force=True)
            await process.wait()
        await cls._finish_readers(readers)

    @staticmethod
    async def _signal_process_tree(
        process: asyncio.subprocess.Process, *, force: bool
    ) -> None:
        """Signal the isolated process group on POSIX or tree on Windows."""
        if os.name == "nt":
            arguments = ("taskkill", "/PID", str(process.pid), "/T", "/F")
            try:
                killer = await asyncio.create_subprocess_exec(
                    *arguments,
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.DEVNULL,
                )
                await killer.wait()
            except OSError:
                if process.returncode is None:
                    process.kill()
            return

        try:
            os.killpg(process.pid, signal.SIGKILL if force else signal.SIGTERM)
        except ProcessLookupError:
            if process.returncode is None:
                process.kill() if force else process.terminate()

    @staticmethod
    async def _finish_readers(readers: tuple[asyncio.Task[None], ...]) -> None:
        """Bound pipe cleanup so inherited handles cannot stall finalization."""
        if not readers:
            return
        _, pending = await asyncio.wait(readers, timeout=TERMINATE_GRACE_SECONDS)
        for task in pending:
            task.cancel()
        await asyncio.gather(*readers, return_exceptions=True)

    @staticmethod
    def _project_lock_key(project_dir: Path) -> str:
        """Canonicalize aliases and case before reserving a project mutation."""
        return os.path.normcase(str(project_dir.resolve()))

    @staticmethod
    def _secret_values(environment: Iterable[tuple[str, str]]) -> tuple[str, ...]:
        """Extract only non-empty dbt secret values without retaining the full environment."""
        return tuple(
            value
            for key, value in environment
            if key.startswith(SECRET_ENVIRONMENT_PREFIX) and value
        )
