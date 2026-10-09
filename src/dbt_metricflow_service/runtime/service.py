"""PostgreSQL 运行时的受理与读取边界，不依赖实例本地项目目录。"""
from __future__ import annotations

import asyncio
import hashlib
import json
import time
from importlib.metadata import version
from pathlib import Path
from uuid import uuid4

from dbt_metricflow_service.settings import Settings
from dbt_metricflow_service.storage.artifacts import ArtifactStore
from dbt_metricflow_service.storage.jobs import JobStore
from dbt_metricflow_service.storage.postgres import Database

BUILD = "BUILD_RUN"
QUERY = "METRIC_QUERY"
OPTIONS = "QUERY_OPTIONS"
SUCCEEDED = "SUCCEEDED"
FAILED = "FAILED"
ACTIVE = "ACTIVE"
CLEANED = "CLEANED"
READ_ONLY = "READ_ONLY"
PREPARATION_ONLY = "PREPARATION_ONLY"
TOOLCHAIN_PACKAGES = ("dbt-core", "metricflow", "dbt-starrocks", "dbt-duckdb", "dbt-postgres")


class RuntimeUnavailable(RuntimeError):
    """受理容量、数据库或同步等待暂时不可用。"""


class AdapterUnsupported(ValueError):
    """通用 MetricFlow CLI 尚未支持请求的 adapter。"""


def current_toolchain() -> str:
    """安装版本和服务代码共同决定执行兼容标识，避免不同代码误领旧任务。"""
    digest = hashlib.sha256()
    for package in TOOLCHAIN_PACKAGES:
        digest.update(f"{package}={version(package)}\n".encode())
    # runtime 已归入子包，兼容标识仍必须覆盖整个服务源码。
    source_root = Path(__file__).resolve().parents[1]
    for source in sorted(source_root.rglob("*.py")):
        digest.update(source.relative_to(source_root).as_posix().encode())
        digest.update(source.read_bytes().replace(b"\r\n", b"\n"))
    return digest.hexdigest()


class Runtime:
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


    async def start(self):
        # 本地目录只保存当前执行，可由空目录启动。
        from dbt_metricflow_service.runtime.worker import Worker
        self.settings.temp_root.mkdir(parents=True, exist_ok=True)
        self.worker = Worker(self)
        self.worker.start()


    async def close(self):
        if self.worker is not None:
            await self.worker.close()
        await asyncio.to_thread(self.db.close)


    def _run(self, run_id: str, *, ready: bool = False) -> dict:
        row = self.jobs.get(run_id)
        if row is None or row["kind"] != BUILD:
            raise KeyError(run_id)
        if ready and (row["status"] != SUCCEEDED or row["run_lifecycle"] != ACTIVE):
            raise ValueError("run is not ready")
        return row


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
        row = self.submit_options(run_id, metrics)
        return self._wait(row["job_id"])


    def submit_options(self, run_id: str, metrics: tuple[str, ...]):
        """同步与异步入口共用受理身份；冷请求不等待 worker 完成。"""
        if not metrics:
            raise ValueError("metrics are required")
        metrics = tuple(sorted(set(metrics)))
        key = hashlib.sha256(json.dumps([run_id, metrics]).encode()).hexdigest()
        row = self._submit_child(run_id, OPTIONS, {"mode": "OPTIONS", "metrics": list(metrics)}, key)
        if row["status"] == FAILED:
            self.jobs.requeue_options(row["job_id"])
            row = self.jobs.get(row["job_id"])
        return row


    def cleanup(self, run_id: str):
        parent = self._run(run_id)
        if parent["run_lifecycle"] == CLEANED:
            return
        row = self.jobs.reserve_cleanup(run_id)
        try:
            self._wait(row["job_id"])
        except ValueError as error:
            raise RuntimeUnavailable("cleanup did not complete; retry after inspection") from error


