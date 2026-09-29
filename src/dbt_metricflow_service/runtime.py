"""PostgreSQL 运行时的受理与读取边界，不依赖实例本地项目目录。"""

from __future__ import annotations

import asyncio
import hashlib
import json
import threading
import time
from importlib.metadata import version
from pathlib import Path
from uuid import uuid4

from dbt_metricflow_service.adapter_support import METRICFLOW_SUPPORTED_ADAPTERS
from dbt_metricflow_service.models import DbtJobRequest, JobRecord, JobStatus, MetricFlowJobRequest
from dbt_metricflow_service.platform_models import PlatformQueryRequest, PlatformRunRequest
from dbt_metricflow_service.platform_namespace import validate_schema_name
from dbt_metricflow_service.projects import (
    PROJECT_NAME_PATTERN,
    InvalidManifestError,
    InvalidProjectError,
    ManifestNotFoundError,
    ProjectNotFoundError,
)
from dbt_metricflow_service.settings import Settings
from dbt_metricflow_service.storage.artifacts import ArtifactStore
from dbt_metricflow_service.storage.jobs import JobStore
from dbt_metricflow_service.storage.postgres import Database

BUILD = "BUILD_RUN"
QUERY = "METRIC_QUERY"
OPTIONS = "QUERY_OPTIONS"
CLEANUP = "RUN_CLEANUP"
DBT = "DBT_COMMAND"
MF = "MF_COMMAND"
QUEUED = "QUEUED"
RUNNING = "RUNNING"
SUCCEEDED = "SUCCEEDED"
FAILED = "FAILED"
ACTIVE = "ACTIVE"
CLEANED = "CLEANED"
VOLATILE = "VOLATILE"
DURABLE = "DURABLE"
READ_ONLY = "READ_ONLY"
PREPARATION_ONLY = "PREPARATION_ONLY"
MANIFEST = "target/manifest.json"
RESOURCE_FIELD = "resources"
TOOLCHAIN_PACKAGES = ("dbt-core", "metricflow", "dbt-starrocks", "dbt-duckdb", "dbt-postgres")
SNAPSHOT_FIELDS = ("commitSha", "projectDigest", "profileBindingId", "configVersion")
SCHEMA_PREFIX = "run_"


class RuntimeUnavailable(RuntimeError):
    """受理容量、数据库或同步等待暂时不可用。"""


class AdapterUnsupported(ValueError):
    """通用 MetricFlow CLI 尚未支持请求的 adapter。"""


def current_toolchain() -> str:
    """安装版本和服务代码共同决定执行兼容标识，避免不同代码误领旧任务。"""
    digest = hashlib.sha256()
    for package in TOOLCHAIN_PACKAGES:
        digest.update(f"{package}={version(package)}\n".encode())
    for source in sorted(Path(__file__).parent.rglob("*.py")):
        digest.update(source.relative_to(Path(__file__).parent).as_posix().encode())
        digest.update(source.read_bytes().replace(b"\r\n", b"\n"))
    return digest.hexdigest()


class Runtime:
    """API 与 worker 共用的持久依赖以及实例私有的临时输入。"""

    def __init__(self, settings: Settings):
        # 数据库 schema 必须预先通过管理命令安装，启动不得修改其他实例任务。
        self.settings = settings
        self.instance_id = uuid4()
        self.toolchain = settings.toolchain_version or current_toolchain()
        self.db = Database(settings.database_url)
        self.db.check()
        self.jobs = JobStore(self.db, lease_seconds=settings.lease_seconds,
                             max_result_bytes=settings.max_result_bytes,
                             max_diagnostic_bytes=settings.max_output_bytes)
        self.artifacts = ArtifactStore(self.db, max_file_bytes=settings.max_artifact_file_bytes,
                                       max_set_bytes=settings.max_artifact_bytes)
        self.worker = None
        self._inputs: dict[str, dict[str, str]] = {}
        self._input_lock = threading.Lock()

    async def start(self):
        # 本地目录只保存当前执行，可由空目录启动。
        from dbt_metricflow_service.worker import Worker
        self.settings.temp_root.mkdir(parents=True, exist_ok=True)
        self.worker = Worker(self)
        self.worker.start()

    async def close(self):
        if self.worker is not None:
            await self.worker.close()
        self._inputs.clear()
        await asyncio.to_thread(self.db.close)

    def input_for(self, job_id: str) -> dict[str, str] | None:
        with self._input_lock:
            return self._inputs.get(job_id)

    def discard_input(self, job_id: str):
        with self._input_lock:
            self._inputs.pop(job_id, None)

    def _project(self, project_id: str) -> dict:
        if not PROJECT_NAME_PATTERN.fullmatch(project_id):
            raise InvalidProjectError(project_id)
        project = self.jobs.project(project_id)
        if project is None:
            raise ProjectNotFoundError(project_id)
        if project["config_version"] != self.settings.config_version:
            raise RuntimeUnavailable("matching project configuration is unavailable")
        return project

    async def submit_cli(self, request: DbtJobRequest | MetricFlowJobRequest) -> JobRecord:
        return await asyncio.to_thread(self._submit_cli, request)

    def _submit_cli(self, request: DbtJobRequest | MetricFlowJobRequest) -> JobRecord:
        project = self._project(request.project)
        input_set = project.get("current_output_set_id") or project.get("source_set_id")
        if input_set is None:
            raise ProjectNotFoundError(request.project)
        is_dbt = isinstance(request, DbtJobRequest)
        if not is_dbt and not request.resources:
            try:
                manifest = json.loads(self.artifacts.read_file(input_set, MANIFEST))
                adapter = manifest["metadata"]["adapter_type"]
            except (FileNotFoundError, KeyError) as error:
                raise ManifestNotFoundError(request.project) from error
            except (ValueError, TypeError) as error:
                raise InvalidManifestError(request.project) from error
            if adapter not in METRICFLOW_SUPPORTED_ADAPTERS:
                raise AdapterUnsupported("adapter is not supported by the MetricFlow CLI")
        # 在入库之前保留有界的原始输入；请求正文永远不序列化至任务记录。
        job_id = str(uuid4())
        if request.resources:
            with self._input_lock:
                if len(self._inputs) >= self.settings.worker_concurrency:
                    raise RuntimeUnavailable("volatile input capacity is exhausted")
                self._inputs[job_id] = request.resources
        try:
            row = self.jobs.reserve(
                DBT if is_dbt else MF, request.project,
                request.model_dump(mode="json", exclude={RESOURCE_FIELD}),
                job_id=job_id, input_set_id=input_set,
                input_mode=VOLATILE if request.resources else DURABLE,
                pinned_instance_id=str(self.instance_id) if request.resources else None,
                config_version=self.settings.config_version, toolchain_version=self.toolchain,
                write=is_dbt, timeout_seconds=self.settings.command_timeout_seconds,
                expected_revision=project["revision"],
            )
        except BaseException:
            self.discard_input(job_id)
            raise
        return self._cli_record(row)

    def cli_record(self, job_id: str) -> JobRecord | None:
        row = self.jobs.get(job_id)
        if row is None or row["kind"] not in (DBT, MF):
            return None
        return self._cli_record(row)

    def _cli_record(self, row: dict) -> JobRecord:
        result = self.jobs.result(row["job_id"]) or {}
        payload = result.get("payload_json") or row.get("error_detail") or {}
        status = JobStatus(row["status"].lower())
        if row.get("error_code") in ("TIMED_OUT", "COMMAND_TIMEOUT", "TASK_TIMEOUT"):
            status = JobStatus.TIMED_OUT
        return JobRecord(
            id=row["job_id"], project=row["project_id"], status=status,
            submitted_at=row["created_at"], started_at=row.get("started_at"),
            finished_at=row.get("finished_at"), exit_code=result.get("exit_code", payload.get("exit_code")),
            stdout=result.get("stdout_tail") or payload.get("stdout", ""),
            stderr=(result.get("stderr_tail") or payload.get("stderr")
                    or (row.get("error_detail") or {}).get("stderr") or row.get("error_code") or ""),
            output_truncated=result.get("output_truncated", payload.get("output_truncated", False)),
        )

    def submit_run(self, request: PlatformRunRequest) -> dict:
        project = self._project(request.project_id)
        binding = project["binding_config"]
        if binding.get("profileBindingId") != request.profile_binding_id:
            raise ValueError("profile binding does not match")
        if request.config_version != project["config_version"]:
            raise ValueError("configuration version does not match")
        from dbt_metricflow_service.platform_bindings import DIGEST_PATTERN, SHA_PATTERN
        if not SHA_PATTERN.fullmatch(request.commit_sha) or not DIGEST_PATTERN.fullmatch(request.project_digest):
            raise ValueError("invalid source revision or digest")
        job_id = uuid4()
        payload = request.model_dump(mode="json", by_alias=True)
        payload["binding"] = binding
        row = self.jobs.reserve(
            BUILD, request.project_id, payload, job_id=str(job_id),
            idempotency_scope=BUILD, idempotency_key=request.idempotency_key,
            config_version=request.config_version, toolchain_version=self.toolchain,
            schema_name=(
                validate_schema_name(binding["schemaName"]) if "schemaName" in binding else SCHEMA_PREFIX + job_id.hex
            ), profile_binding_id=request.profile_binding_id,
            timeout_seconds=self.settings.command_timeout_seconds,
            expected_revision=project["revision"],
        )
        return {"runId": row["job_id"]}

    def _run(self, run_id: str, *, ready: bool = False) -> dict:
        row = self.jobs.get(run_id)
        if row is None or row["kind"] != BUILD:
            raise KeyError(run_id)
        if ready and (row["status"] != SUCCEEDED or row["run_lifecycle"] != ACTIVE):
            raise ValueError("run is not ready")
        return row

    def get_run(self, run_id: str) -> dict | None:
        try:
            row = self._run(run_id)
        except KeyError:
            return None
        state = row["status"]
        if row["run_lifecycle"] != ACTIVE:
            state = row["run_lifecycle"]
        elif state == SUCCEEDED:
            state = "READY"
        elif state == RUNNING:
            state = row["phase"]
        payload = {"runId": row["job_id"], "state": state}
        payload.update({key: row["request_json"][key] for key in SNAPSHOT_FIELDS if key in row["request_json"]})
        result = self.jobs.result(run_id)
        if result:
            payload.update(result["payload_json"])
        if row.get("error_code"):
            payload["errorCode"] = row["error_code"]
        return payload

    def run_by_key(self, key: str):
        row = self.jobs.by_key(BUILD, key)
        return self.get_run(row["job_id"]) if row else None

    def catalog(self, run_id: str):
        row = self._run(run_id, ready=True)
        metadata = self.artifacts.metadata(row["output_set_id"])
        return metadata.get("catalog") or metadata.get("catalog_json")

    def _submit_child(self, run_id: str, kind: str, payload: dict, key: str) -> dict:
        parent = self._run(run_id, ready=True)
        # 项目当前配置可能已升级；查询固定使用父 run 的配置与工具链。
        retry_safe = parent["request_json"].get("binding", {}).get("queryRetrySafe") is True
        return self.jobs.reserve(
            kind, parent["project_id"], payload, idempotency_scope=kind, idempotency_key=key,
            parent_run_id=run_id, input_set_id=parent["output_set_id"],
            config_version=parent["config_version"], toolchain_version=parent["toolchain_version"],
            profile_binding_id=parent["profile_binding_id"], schema_name=parent["schema_name"],
            retry_policy=READ_ONLY if retry_safe else PREPARATION_ONLY,
            timeout_seconds=self.settings.command_timeout_seconds,
        )

    def submit_query(self, request: PlatformQueryRequest) -> dict:
        row = self._submit_child(str(request.run_id), QUERY,
                                 request.model_dump(mode="json", by_alias=True), request.idempotency_key)
        return {"queryId": row["job_id"]}

    def get_query(self, query_id: str):
        row = self.jobs.get(query_id)
        if row is None or row["kind"] != QUERY:
            return None
        payload = {"queryId": query_id, "state": "READY" if row["status"] == SUCCEEDED else row["status"]}
        if row["status"] == SUCCEEDED:
            result = self.jobs.result(query_id)
            payload.update(result["payload_json"])
        if row.get("error_code"):
            payload["errorCode"] = row["error_code"]
        return payload

    def query_by_key(self, key: str):
        row = self.jobs.by_key(QUERY, key)
        return self.get_query(row["job_id"]) if row else None

    def _wait(self, job_id: str) -> dict:
        deadline = time.monotonic() + self.settings.synchronous_wait_seconds
        while time.monotonic() < deadline:
            row = self.jobs.get(job_id)
            if row["status"] == SUCCEEDED:
                return self.jobs.result(job_id)["payload_json"]
            if row["status"] == FAILED:
                if row.get("error_code") == "INVALID_QUERY":
                    raise ValueError("invalid query options")
                raise RuntimeUnavailable("persistent execution failed; retry after inspection")
            time.sleep(0.2)
        raise RuntimeUnavailable("persistent task has not finished; retry the request")

    def options(self, run_id: str, metrics: tuple[str, ...]):
        if not metrics:
            raise ValueError("metrics are required")
        key = hashlib.sha256(json.dumps([run_id, metrics]).encode()).hexdigest()
        row = self._submit_child(run_id, OPTIONS, {"mode": "OPTIONS", "metrics": list(metrics)}, key)
        if row["status"] == FAILED:
            self.jobs.requeue_options(row["job_id"])
        return self._wait(row["job_id"])

    def cleanup(self, run_id: str):
        parent = self._run(run_id)
        if parent["run_lifecycle"] == CLEANED:
            return
        row = self.jobs.reserve_cleanup(run_id)
        try:
            self._wait(row["job_id"])
        except ValueError as error:
            raise RuntimeUnavailable("cleanup did not complete; retry after inspection") from error
