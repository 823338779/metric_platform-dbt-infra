from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import shutil
import subprocess
from importlib.metadata import version
from pathlib import Path
from uuid import UUID

from dbt_metricflow_service.adapters.starrocks import STARROCKS_ADAPTER
from dbt_metricflow_service.platform.bindings import ProjectBinding, resolve_revision
from dbt_metricflow_service.platform.catalog import catalog_from_artifacts
from dbt_metricflow_service.platform.metricflow import invoke_programmatic
from dbt_metricflow_service.platform.models import PlatformRunRequest
from dbt_metricflow_service.platform.namespace import prepare_versioned_project, validate_versioned_manifest
from dbt_metricflow_service.platform.store import PlatformJobStore, RunRecord, RunState

SCHEMA_PATTERN = re.compile(r"[a-z][a-z0-9_]*\Z")
ARTIFACT_NAMES = ("manifest.json", "semantic_manifest.json", "run_results.json", "catalog.json")
BUILD_TIMEOUT_SECONDS = 1800
POSTGRES_ADAPTER = "postgres"
QUERY_ADAPTERS = frozenset({POSTGRES_ADAPTER, STARROCKS_ADAPTER})

# 空发布必须有明确的空定义，不能把缺失或损坏的产物当成清空指令。
EMPTY_MANIFEST_SECTIONS = ("nodes", "sources", "semantic_models", "metrics")
EMPTY_SEMANTIC_SECTIONS = ("semantic_models", "metrics")
EMPTY_CATALOG_SECTIONS = ("nodes", "sources")


def _empty_project(target: Path) -> bool:
    manifest = _load_artifact(target, "manifest.json")
    semantic = _load_artifact(target, "semantic_manifest.json")
    catalog = _load_artifact(target, "catalog.json")
    results = _load_artifact(target, "run_results.json")
    return (
        all(manifest.get(section) == {} for section in EMPTY_MANIFEST_SECTIONS)
        and all(semantic.get(section) == [] for section in EMPTY_SEMANTIC_SECTIONS)
        and all(catalog.get(section) == {} for section in EMPTY_CATALOG_SECTIONS)
        and results.get("results") == []
    )


def validate_request_binding(request: PlatformRunRequest, binding: ProjectBinding) -> None:
    """调用方只能使用服务已登记的项目及 profile 绑定。"""

    if request.project_id != binding.project_id or request.profile_binding_id != binding.profile_binding_id:
        raise ValueError("平台项目或 profile 绑定不匹配")


def build_platform_command(project: Path, profiles: Path, target: str, schema: str) -> tuple[str, ...]:
    """构造全量 dbt build 参数，不提供 select/exclude。"""

    if not SCHEMA_PATTERN.fullmatch(schema):
        raise ValueError("运行 schema 无效")
    return (
        "dbt", "build", "--project-dir", str(project), "--profiles-dir", str(profiles),
        "--target", target, "--target-path", str(project / "target"),
    )


def _load_artifact(target: Path, name: str) -> dict[str, object]:
    value = json.loads((target / name).read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("dbt 产物格式无效")
    return value


def validate_artifacts(
    target: Path, schema: str, *, query_probe_passed: bool, table_prefix: str | None = None,
) -> dict[str, object]:
    """READY 之前核验完整测试、物理关系、原生产物和真实查询证明。"""

    empty_project = _empty_project(target)
    if not empty_project and not query_probe_passed:
        raise ValueError("MetricFlow 查询证明缺失")
    manifest = _load_artifact(target, "manifest.json")
    semantic = _load_artifact(target, "semantic_manifest.json")
    results = _load_artifact(target, "run_results.json")
    catalog = _load_artifact(target, "catalog.json")
    # 与目录端点共用版本及资源兼容性门槛，避免 READY 后目录不可读。
    catalog_from_artifacts(target)
    metadata = manifest.get("metadata")
    if not isinstance(metadata, dict) or metadata.get("adapter_type") not in QUERY_ADAPTERS:
        raise ValueError("此 adapter 的 MetricFlow 查询能力不可用")
    if not isinstance(semantic.get("semantic_models"), list) or not isinstance(semantic.get("metrics"), list):
        raise ValueError("MetricFlow 目录产物格式无效")
    if not isinstance(catalog.get("metadata"), dict):
        raise ValueError("dbt 目录产物无版本元数据")
    nodes = manifest.get("nodes")
    records = results.get("results")
    if not isinstance(nodes, dict) or not isinstance(records, list) or (not records and not empty_project):
        raise ValueError("dbt 构建结果不完整")
    if any(not isinstance(item, dict) or item.get("status") not in {"success", "pass"} for item in records):
        raise ValueError("dbt 构建或测试失败")
    completed = {item.get("unique_id") for item in records}
    expected = {
        native_id for native_id, node in nodes.items()
        if isinstance(node, dict) and node.get("resource_type") in {"model", "seed", "snapshot", "test"}
        and node.get("config", {}).get("materialized") != "ephemeral"
    }
    if not expected.issubset(completed):
        raise ValueError("dbt 构建或测试记录不完整")
    catalog_nodes = catalog.get("nodes")
    if not isinstance(catalog_nodes, dict):
        raise ValueError("dbt 物理目录缺失")
    if table_prefix is not None:
        validate_versioned_manifest(target, schema, table_prefix)
    for native_id, node in nodes.items():
        if not isinstance(node, dict) or node.get("resource_type") not in {"model", "seed", "snapshot"}:
            continue
        if node.get("config", {}).get("materialized") == "ephemeral":
            continue
        relation = node.get("relation_name")
        if not isinstance(relation, str) or node.get("schema") != schema or native_id not in catalog_nodes:
            raise ValueError("dbt 物理关系不属于当前 schema")
    checksums = {
        "manifestDigest": _hash(target / "manifest.json"),
        "semanticManifestDigest": _hash(target / "semantic_manifest.json"),
        "runResultsDigest": _hash(target / "run_results.json"),
        "catalogDigest": _hash(target / "catalog.json"),
    }
    return {
        **checksums, "allTestsPassed": True, "representativeQueryPassed": not empty_project,
        "queryCapability": not empty_project, "relationsVerified": True,
    }


def _hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class PlatformRunCoordinator:
    """为 Metric Platform 持久受理并异步执行固定 SHA 的完整构建。"""

    def __init__(
        self, store: PlatformJobStore, bindings: dict[str, ProjectBinding],
        artifacts_root: Path, profiles_dir: Path,
    ) -> None:
        self.store = store
        self.bindings = bindings
        self.artifacts_root = artifacts_root.resolve()
        self.profiles_dir = profiles_dir
        self._tasks: set[asyncio.Task[None]] = set()

    def submit(self, request: PlatformRunRequest) -> dict[str, str]:
        binding = self.bindings.get(request.project_id)
        if binding is None:
            raise ValueError("平台项目未登记")
        validate_request_binding(request, binding)
        fingerprint = hashlib.sha256(request.model_dump_json(exclude={"idempotency_key"}).encode()).hexdigest()
        run_id = self.store.reserve_run(request.idempotency_key, fingerprint)
        run_dir = self.artifacts_root / str(run_id)
        run_dir.mkdir(parents=True, exist_ok=True)
        if self.store.claim_run(run_id, run_dir):
            (run_dir / "request.json").write_text(request.model_dump_json(by_alias=True), encoding="utf-8")
            task = asyncio.create_task(self._build(run_id, request, binding, run_dir))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)
        return {"runId": str(run_id)}

    def get(self, run_id: UUID) -> dict[str, object] | None:
        record = self.store.find_run(run_id)
        return self._snapshot(record) if record else None

    def get_by_key(self, key: str) -> dict[str, object] | None:
        record = self.store.find_run_by_key(key)
        return self._snapshot(record) if record else None

    def catalog(self, run_id: UUID) -> dict[str, object]:
        record = self.store.find_run(run_id)
        if record is None or record.state is not RunState.READY or record.artifact_path is None:
            raise ValueError("运行任务尚未 READY")
        project = self.project_for_run(run_id)
        return catalog_from_artifacts(project / "target")

    def project_for_run(self, run_id: UUID) -> Path:
        record = self.store.find_run(run_id)
        if record is None or record.state is not RunState.READY or record.artifact_path is None:
            raise ValueError("运行任务尚未 READY")
        project_path = json.loads((record.artifact_path / "project-path.json").read_text(encoding="utf-8"))["path"]
        project = Path(project_path).resolve()
        if not project.is_relative_to(record.artifact_path.resolve()):
            raise ValueError("运行产物路径无效")
        return project

    def cleanup_run(self, run_id: UUID) -> None:
        """仅回收固定 run 的物理 schema 与目录，活动查询由 SQLite 原子门禁保护。"""

        record = self.store.find_run(run_id)
        if record is None:
            raise KeyError(run_id)
        if record.state is RunState.CLEANED:
            return
        directory = record.artifact_path
        expected = self.artifacts_root / str(run_id)
        if (
            directory is None or directory.is_symlink() or self.artifacts_root.is_symlink()
            or directory.absolute() != expected.absolute()
            or (directory.exists() and directory.resolve() != expected.resolve())
            or not directory.resolve().is_relative_to(self.artifacts_root.resolve())
        ):
            raise ValueError("运行产物目录不安全")
        if not self.store.claim_cleanup(run_id):
            return
        try:
            # 删除目录后的进程中断可通过持久状态恢复；schema 已在删除前清理。
            if not directory.exists() and record.error_code == "INTERRUPTED_CLEANUP":
                self.store.transition_run(run_id, RunState.CLEANED)
                return
            project_file = directory / "project-path.json"
            if record.state is RunState.READY and not project_file.is_file():
                raise ValueError("READY 运行缺少项目产物")
            if project_file.is_file():
                project = Path(json.loads(project_file.read_text(encoding="utf-8"))["path"]).resolve()
                if not project.is_relative_to(directory.resolve()) or project.is_symlink():
                    raise ValueError("运行项目目录不安全")
                namespace_file = directory / "namespace.json"
                namespace = json.loads(namespace_file.read_text(encoding="utf-8")) if namespace_file.is_file() else {}
                schema = namespace.get("schemaName", "run_" + run_id.hex)
                table_prefix = namespace.get("tablePrefix")
                input_path = directory / "cleanup-input.json"
                output_path = directory / "cleanup-output.json"
                cleanup_input = {"mode": "CLEANUP", "schema": schema}
                if table_prefix is not None:
                    cleanup_input.update({"runId": str(run_id), "tablePrefix": table_prefix})
                input_path.write_text(json.dumps(cleanup_input), encoding="utf-8")
                request = json.loads((directory / "request.json").read_text(encoding="utf-8"))
                invoke_programmatic(
                    project, self.profiles_dir, schema, request["profileBindingId"], input_path, output_path
                )
            shutil.rmtree(directory)
            self.store.transition_run(run_id, RunState.CLEANED)
        except Exception:
            self.store.transition_run(run_id, RunState.FAILED, directory, "CLEANUP_FAILED")
            raise

    def _snapshot(self, record: RunRecord) -> dict[str, object]:
        payload: dict[str, object] = {"runId": str(record.run_id), "state": record.state.value}
        if record.artifact_path:
            request_file = record.artifact_path / "request.json"
            if request_file.is_file():
                request = json.loads(request_file.read_text(encoding="utf-8"))
                for field in ("commitSha", "projectDigest", "profileBindingId", "configVersion"):
                    payload[field] = request[field]
            result_file = record.artifact_path / "validation.json"
            if result_file.is_file():
                payload.update(json.loads(result_file.read_text(encoding="utf-8")))
        if record.error_code:
            payload["errorCode"] = record.error_code
        return payload

    async def _build(
        self, run_id: UUID, request: PlatformRunRequest,
        binding: ProjectBinding, run_dir: Path,
    ) -> None:
        schema = binding.schema_name or "run_" + run_id.hex
        try:
            project = await asyncio.to_thread(
                resolve_revision, binding, request.commit_sha, request.project_digest, run_dir / "source"
            )
            (run_dir / "project-path.json").write_text(json.dumps({"path": str(project)}), encoding="utf-8")
            table_prefix = prepare_versioned_project(project, run_id, schema) if binding.schema_name else None
            (run_dir / "namespace.json").write_text(
                json.dumps({"schemaName": schema, "tablePrefix": table_prefix}), encoding="utf-8"
            )
            self.store.transition_run(run_id, RunState.BUILDING)
            command = build_platform_command(project, self.profiles_dir, binding.profile_binding_id, schema)
            environment = {**os.environ, "DBT_PLATFORM_SCHEMA": schema, "DBT_SEND_ANONYMOUS_USAGE_STATS": "false"}
            if (project / "packages.yml").exists() or (project / "dependencies.yml").exists():
                deps_command = (
                    "dbt", "deps", "--project-dir", str(project), "--profiles-dir", str(self.profiles_dir),
                    "--target", binding.profile_binding_id,
                )
                await asyncio.to_thread(self._execute, deps_command, project, environment)
            if table_prefix is not None:
                parse_command = (
                    "dbt", "parse", "--project-dir", str(project), "--profiles-dir", str(self.profiles_dir),
                    "--target", binding.profile_binding_id, "--target-path", str(project / "target"),
                )
                await asyncio.to_thread(self._execute, parse_command, project, environment)
                validate_versioned_manifest(project / "target", schema, table_prefix)
            await asyncio.to_thread(self._execute, command, project, environment)
            run_results = (project / "target" / "run_results.json").read_bytes()
            docs_command = (
                "dbt", "docs", "generate", "--project-dir", str(project),
                "--profiles-dir", str(self.profiles_dir), "--target", binding.profile_binding_id,
                "--target-path", str(project / "target"),
            )
            await asyncio.to_thread(self._execute, docs_command, project, environment)
            (project / "target" / "run_results.json").write_bytes(run_results)
            self.store.transition_run(run_id, RunState.VALIDATING)
            probe_input = run_dir / "probe-input.json"
            probe_output = run_dir / "probe-output.json"
            probe_input.write_text(json.dumps({"mode": "PROBE"}), encoding="utf-8")
            # 空项目仍校验原生产物，但没有指标可执行探针，不宣称具备查询能力。
            probe = {} if _empty_project(project / "target") else await asyncio.to_thread(
                invoke_programmatic, project, self.profiles_dir, schema,
                binding.profile_binding_id, probe_input, probe_output
            )
            validation = validate_artifacts(
                project / "target", schema, query_probe_passed=probe.get("queryCapability") is True,
                table_prefix=table_prefix,
            )
            validation.update({
                "schemaName": schema,
                "toolchainVersion": f"dbt-core {version('dbt-core')} / metricflow {version('metricflow')}",
            })
            (run_dir / "validation.json").write_text(json.dumps(validation), encoding="utf-8")
            (run_dir / "READY").write_text("ready\n", encoding="utf-8")
            self.store.transition_run(run_id, RunState.READY, run_dir)
        except Exception as error:
            self.store.transition_run(run_id, RunState.FAILED, run_dir, type(error).__name__)

    @staticmethod
    def _execute(command: tuple[str, ...], project: Path, environment: dict[str, str]) -> None:
        try:
            subprocess.run(
                command, cwd=project, env=environment, capture_output=True,
                timeout=BUILD_TIMEOUT_SECONDS, check=True,
            )
        except (OSError, subprocess.SubprocessError) as error:
            raise ValueError("dbt 固定版本构建失败") from error
