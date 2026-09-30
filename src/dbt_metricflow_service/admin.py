"""部署与迁移管理入口；只读旧库，连接信息从服务配置和环境变量加载。"""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from pathlib import Path
from urllib.parse import urlsplit
from uuid import UUID, uuid4

from psycopg2.extras import Json

from dbt_metricflow_service.job_artifacts import _is_link
from dbt_metricflow_service.platform_bindings import _prefix
from dbt_metricflow_service.platform_catalog import catalog_from_artifacts
from dbt_metricflow_service.platform_models import PlatformQueryRequest, PlatformRunRequest
from dbt_metricflow_service.platform_namespace import validate_schema_name
from dbt_metricflow_service.platform_runs import validate_artifacts
from dbt_metricflow_service.projects import PROJECT_NAME_PATTERN
from dbt_metricflow_service.runtime import current_toolchain
from dbt_metricflow_service.settings import Settings
from dbt_metricflow_service.storage.artifacts import ArtifactStore
from dbt_metricflow_service.storage.jobs import JobStore
from dbt_metricflow_service.storage.postgres import Database

# 管理命令及历史格式常量集中定义，所有请求正文均经公开模型验证。
MIGRATE = "migrate"
IMPORT_PROJECT = "import-project"
REGISTER_BINDINGS = "register-bindings"
IMPORT_LEGACY = "import-legacy"
RECONCILE = "reconcile-attempt"
GC = "gc"
UTF8 = "utf-8"
BUILD = "BUILD_RUN"
QUERY = "METRIC_QUERY"
READY = "READY"
FAILED = "FAILED"
CLEANED = "CLEANED"
SUCCEEDED = "SUCCEEDED"
EXECUTION = "EXECUTION"
ACTIVE = "ACTIVE"
REQUEST_FILE = "request.json"
VALIDATION_FILE = "validation.json"
PROJECT_PATH_FILE = "project-path.json"
RESULT_FILE = "result.json"
TARGET = "target"
MANIFEST = "manifest.json"
ARTIFACT_DIGESTS = {MANIFEST: "manifestDigest", "semantic_manifest.json": "semanticManifestDigest",
                    "run_results.json": "runResultsDigest", "catalog.json": "catalogDigest"}
SCHEMA_PREFIX = "run_"
IMPORT_DIGEST = "legacyImportDigest"
LEGACY_TABLES = (("platform_runs", BUILD), ("platform_queries", QUERY))
BINDING_KEYS = frozenset({"projectId", "remote", "projectSubdir", "profileBindingId"})
BINDING_OPTIONAL_KEYS = frozenset({"configVersion", "queryRetrySafe", "schemaName"})
SQL_INSERT_PROJECT = """INSERT INTO runtime_project(project_id,config_version)
VALUES (%s,%s) ON CONFLICT(project_id) DO NOTHING"""
SQL_UPDATE_PROJECT = """UPDATE runtime_project SET source_set_id=%s,current_output_set_id=%s,
config_version=%s,revision=revision+1 WHERE project_id=%s"""
SQL_LOCK_IMPORT = "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))"
SQL_EXISTING = "SELECT * FROM runtime_job WHERE job_id=%s"
SQL_INSERT_JOB = """INSERT INTO runtime_job
 (job_id,kind,project_id,parent_run_id,idempotency_scope,idempotency_key,request_fingerprint,request_json,
  input_set_id,output_set_id,config_version,toolchain_version,schema_name,profile_binding_id,
  status,run_lifecycle,deadline_at,finished_at,started_at,attempt_no,error_code,error_detail)
 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
 clock_timestamp(),clock_timestamp(),clock_timestamp(),1,%s,%s)"""
SQL_INSERT_ATTEMPT = """INSERT INTO runtime_attempt
 (attempt_id,job_id,attempt_no,worker_id,lease_token,lease_expires_at,state,stop_confirmed_at,finished_at)
 VALUES (%s,%s,1,%s,%s,clock_timestamp(),%s,clock_timestamp(),clock_timestamp())"""
SQL_ATTACH_ATTEMPT = "UPDATE runtime_job SET current_attempt_id=%s WHERE job_id=%s"
SQL_UNCONFIRMED_IMPORT = """UPDATE runtime_attempt SET execution_stage='EXTERNAL',
 state='EXPIRED_UNCONFIRMED',stop_confirmed_at=NULL WHERE attempt_id=%s"""
SQL_INSERT_RESULT = "INSERT INTO runtime_job_result(job_id,attempt_id,payload_json) VALUES (%s,%s,%s)"
SQL_ATTEMPT = "SELECT lease_token,state FROM runtime_attempt WHERE attempt_id=%s"
SQL_GC = """SELECT set_id FROM runtime_artifact_set
 WHERE created_at < clock_timestamp() - %s * interval '1 hour' ORDER BY created_at LIMIT %s"""
SQLITE_BEGIN = "BEGIN"
SQLITE_TABLES = "SELECT name FROM sqlite_master WHERE type='table'"
SQLITE_ROWS = "SELECT * FROM {} ORDER BY id"
SQLITE_READ_ONLY = "?mode=ro"


def _artifacts(db: Database, settings: Settings) -> ArtifactStore:
    return ArtifactStore(db, max_file_bytes=settings.max_artifact_file_bytes, max_set_bytes=settings.max_artifact_bytes)


def _project_id(value: str) -> str:
    if not isinstance(value, str) or not PROJECT_NAME_PATTERN.fullmatch(value):
        raise ValueError("project identifier is invalid")
    return value


def _json(path: Path, maximum: int) -> dict | list:
    # 控制文件不会作为产物保存，读取前仍校验链接与长度。
    if any(_is_link(item) for item in (path, *path.parents)):
        raise ValueError("legacy input cannot contain linked paths")
    with path.open("rb") as stream:
        content = stream.read(maximum + 1)
    if len(content) > maximum:
        raise ValueError("legacy JSON file size limit exceeded")
    value = json.loads(content)
    if not isinstance(value, (dict, list)):
        raise ValueError("legacy JSON structure is invalid")
    return value


def import_project(db: Database, settings: Settings, project_id: str, directory: Path) -> dict:
    """导入当前源码及已有解析产物；最终事务一次替换同源的两个项目指针。"""
    project_id = _project_id(project_id)
    artifacts = _artifacts(db, settings)
    with db.transaction() as cursor:
        cursor.execute(SQL_INSERT_PROJECT, (project_id, settings.config_version))
    metadata = {"config_version": settings.config_version,
                "toolchain_version": settings.toolchain_version or current_toolchain()}
    source_id = artifacts.capture(project_id, directory, metadata=metadata)
    output_id = None
    try:
        if (directory / TARGET / MANIFEST).is_file():
            output_id = artifacts.capture(project_id, directory, kind=EXECUTION,
                                          metadata={**metadata, "source_set_id": source_id})
        with db.transaction() as cursor:
            cursor.execute(SQL_UPDATE_PROJECT, (source_id, output_id, settings.config_version, project_id))
    except BaseException:
        if output_id:
            artifacts.delete_unreferenced(output_id)
        artifacts.delete_unreferenced(source_id)
        raise
    return {"projectId": project_id, "sourceSetId": source_id, "outputSetId": output_id}


def register_bindings(db: Database, settings: Settings, path: Path) -> list[str]:
    """把已验证的受控 Git/profile 绑定整体登记；不保存 URL 凭据。"""
    records = _json(path, settings.max_artifact_file_bytes)
    if not isinstance(records, list):
        raise ValueError("project bindings must be a list")
    seen: set[str] = set()
    for record in records:
        if (not isinstance(record, dict) or not BINDING_KEYS <= set(record)
                or set(record) - BINDING_KEYS - BINDING_OPTIONAL_KEYS):
            raise ValueError("project binding fields are invalid")
        if type(record.get("queryRetrySafe", False)) is not bool:
            raise ValueError("queryRetrySafe must be boolean")
        if not isinstance(record.get("configVersion", settings.config_version), str):
            raise ValueError("configVersion must be a string")
        project_id = _project_id(record["projectId"])
        if project_id in seen:
            raise ValueError("project binding is duplicated")
        seen.add(project_id)
        _prefix(record["projectSubdir"])
        remote = record["remote"]
        if not isinstance(remote, str) or not remote or any(char in remote for char in ("\n", "\r")):
            raise ValueError("Git remote is invalid")
        parsed = urlsplit(remote)
        if parsed.password or parsed.query or parsed.fragment or parsed.scheme in {"http", "https"} and parsed.username:
            raise ValueError("Git remote must not contain credentials or query parameters")
        if not isinstance(record["profileBindingId"], str) or not record["profileBindingId"]:
            raise ValueError("profile binding identifier is required")
        if "schemaName" in record:
            validate_schema_name(record["schemaName"])
    jobs = JobStore(db)
    for record in records:
        jobs.register_project(record["projectId"], record, record.get("configVersion", settings.config_version))
    return [record["projectId"] for record in records]


def _legacy_row(db: Database, settings: Settings, row: dict, kind: str) -> str:
    # 迁移只接受已排空的终态；不重放旧任务，也不替未确认外部执行伪造完成。
    identifier = str(UUID(row["id"]))
    state = row["state"]
    if state not in {READY, FAILED, CLEANED} or kind == QUERY and state == CLEANED:
        raise ValueError("legacy tasks must be drained before import")
    if not row.get("artifact_path"):
        raise ValueError("legacy task request directory is missing")
    directory = Path(row["artifact_path"])
    raw_request = _json(directory / REQUEST_FILE, settings.max_artifact_file_bytes)
    model = PlatformRunRequest if kind == BUILD else PlatformQueryRequest
    parsed = model.model_validate(raw_request)
    request = parsed.model_dump(by_alias=True, mode="json")
    if request["idempotencyKey"] != row["idempotency_key"]:
        raise ValueError("legacy request idempotency key mismatch")
    jobs, artifacts = JobStore(db), _artifacts(db, settings)
    output_id = None
    source_id = None
    parent = None
    validation: dict = {}
    result: dict = {}
    if kind == BUILD:
        project_id = _project_id(request["projectId"])
        config_version = request["configVersion"]
        schema = SCHEMA_PREFIX + UUID(identifier).hex
        profile = request["profileBindingId"]
        with db.transaction() as cursor:
            cursor.execute(SQL_INSERT_PROJECT, (project_id, config_version))
        if state == READY:
            location = _json(directory / PROJECT_PATH_FILE, settings.max_artifact_file_bytes)
            project = Path(location["path"])
            if _is_link(project) or not project.resolve().is_relative_to(directory.resolve()):
                raise ValueError("legacy project path is outside the run directory")
            validation = _json(directory / VALIDATION_FILE, settings.max_artifact_file_bytes)
            if validation.get("schemaName") != schema:
                raise ValueError("legacy schema name mismatch")
            # 原校验器读取 JSON 前先限制每份文件大小，并拒绝链接带来的越界读取。
            for artifact_name in ARTIFACT_DIGESTS:
                _json(project / TARGET / artifact_name, settings.max_artifact_file_bytes)
            checked = validate_artifacts(project / TARGET, schema,
                                         query_probe_passed=validation.get("representativeQueryPassed") is True)
            if any(validation.get(key) != value for key, value in checked.items()):
                raise ValueError("legacy artifact digest or validation evidence mismatch")
            metadata = {"validation_json": validation, "catalog_json": catalog_from_artifacts(project / TARGET),
                        "source_commit_sha": request["commitSha"], "project_digest": request["projectDigest"],
                        "config_version": config_version,
                        "toolchain_version": settings.toolchain_version or current_toolchain()}
            output_id = artifacts.capture(project_id, project, kind=EXECUTION, metadata=metadata)
        elif state == FAILED and (directory / PROJECT_PATH_FILE).is_file():
            # 失败构建也可能创建了 schema；保存源码供人工核实结束后的清理使用。
            location = _json(directory / PROJECT_PATH_FILE, settings.max_artifact_file_bytes)
            project = Path(location["path"])
            if _is_link(project) or not project.resolve().is_relative_to(directory.resolve()):
                raise ValueError("legacy project path is outside the run directory")
            source_id = artifacts.capture(project_id, project, metadata={"config_version": config_version})
    else:
        parent_id = str(parsed.run_id)
        if row.get("parent_run_id") not in (None, parent_id):
            raise ValueError("legacy query parent mismatch")
        parent = jobs.get(parent_id)
        if parent is None or parent["kind"] != BUILD:
            raise ValueError("legacy query parent must be imported first")
        project_id, config_version = parent["project_id"], parent["config_version"]
        schema, profile = parent["schema_name"], parent["profile_binding_id"]
        if state == READY:
            if not parent["output_set_id"]:
                raise ValueError("legacy successful query requires published run artifacts")
            result = _json(directory / RESULT_FILE, settings.max_result_bytes)
            if not isinstance(result, dict):
                raise ValueError("legacy query result must be an object")
    # 摘要覆盖可观察内容及原生字节，排除只能在旧机器使用的绝对路径。
    snapshot = {"request": request, "state": state, "errorCode": row.get("error_code"),
                "fingerprint": row["fingerprint"], "validation": validation, "result": result,
                "artifactDigest": artifacts.metadata(output_id or source_id)["content_digest"]
                if output_id or source_id else None}
    fingerprint = hashlib.sha256(json.dumps(snapshot, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    attached = False
    try:
        with db.transaction() as cursor:
            cursor.execute(SQL_LOCK_IMPORT, (identifier,))
            cursor.execute(SQL_EXISTING, (identifier,))
            existing = cursor.fetchone()
            if existing:
                if (existing["error_detail"] or {}).get(IMPORT_DIGEST) != fingerprint:
                    raise ValueError("legacy import content is different from the existing record")
                return identifier
            succeeded = state in {READY, CLEANED}
            status = SUCCEEDED if succeeded else FAILED
            lifecycle = (CLEANED if state == CLEANED else ACTIVE) if kind == BUILD else None
            cursor.execute(SQL_INSERT_JOB, (
                identifier, kind, project_id, parent["job_id"] if parent else None, kind,
                row["idempotency_key"], row["fingerprint"], Json(request),
                parent["output_set_id"] if parent else source_id, output_id, config_version,
                settings.toolchain_version or current_toolchain(), schema, profile, status, lifecycle,
                row.get("error_code"), Json({IMPORT_DIGEST: fingerprint}),
            ))
            attempt_id = str(uuid4())
            cursor.execute(SQL_INSERT_ATTEMPT, (attempt_id, identifier, str(uuid4()), str(uuid4()), status))
            if state == FAILED:
                # SQLite 没有可靠的外部停止证据，迁移失败记录默认保留清理保护。
                cursor.execute(SQL_UNCONFIRMED_IMPORT, (attempt_id,))
            cursor.execute(SQL_ATTACH_ATTEMPT, (attempt_id, identifier))
            if state == READY:
                payload = validation if kind == BUILD else result
                cursor.execute(SQL_INSERT_RESULT, (identifier, attempt_id, Json(payload)))
        attached = True
        return identifier
    finally:
        if output_id and not attached:
            artifacts.delete_unreferenced(output_id)
        if source_id and not attached:
            artifacts.delete_unreferenced(source_id)


def import_legacy(db: Database, settings: Settings, sqlite_path: Path) -> list[str]:
    """逐条幂等导入终态任务；已成功导入的记录可在后续重试中复用。"""
    uri = sqlite_path.resolve(strict=True).as_uri() + SQLITE_READ_ONLY
    with sqlite3.connect(uri, uri=True) as connection:
        connection.row_factory = sqlite3.Row
        # 一致的只读事务固定旧库快照，不调用旧仓储的启动恢复逻辑。
        connection.execute(SQLITE_BEGIN)
        tables = {item[0] for item in connection.execute(SQLITE_TABLES)}
        records = [(kind, dict(row)) for table, kind in LEGACY_TABLES if table in tables
                   for row in connection.execute(SQLITE_ROWS.format(table))]
        return [_legacy_row(db, settings, row, kind) for kind, row in records]


def reconcile_attempt(db: Database, attempt_id: str, *, confirm_external_stopped: bool) -> bool:
    """运维核实外部执行结束后释放保护；不提供自动判定或 HTTP 入口。"""
    if not confirm_external_stopped:
        raise ValueError("explicit external execution stop confirmation is required")
    with db.transaction() as cursor:
        cursor.execute(SQL_ATTEMPT, (str(UUID(attempt_id)),))
        attempt = cursor.fetchone()
    if not attempt or attempt["state"] != "EXPIRED_UNCONFIRMED":
        raise ValueError("attempt is not awaiting external stop confirmation")
    return JobStore(db).confirm_stopped(attempt_id, attempt["lease_token"])


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="PostgreSQL 运行时管理及旧记录只读迁移")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser(MIGRATE)
    project = commands.add_parser(IMPORT_PROJECT)
    project.add_argument("project_id")
    project.add_argument("directory", type=Path)
    bindings = commands.add_parser(REGISTER_BINDINGS)
    bindings.add_argument("path", type=Path)
    legacy = commands.add_parser(IMPORT_LEGACY)
    legacy.add_argument("sqlite_path", type=Path)
    reconcile = commands.add_parser(RECONCILE)
    reconcile.add_argument("attempt_id")
    reconcile.add_argument("--confirm-external-stopped", action="store_true", required=True)
    gc = commands.add_parser(GC)
    gc.add_argument("--older-than-hours", type=int, default=24)
    gc.add_argument("--limit", type=int, default=100)
    args = parser.parse_args(argv)
    # 管理操作与服务启动读取同一配置，避免迁移和运行连接到不同数据库。
    settings = Settings.from_file()
    if not settings.database_url:
        parser.error("SERVICE_DATABASE_URL is required")
    db = Database(settings.database_url)
    try:
        if args.command == MIGRATE:
            db.migrate()
            result = {"migrated": True}
        else:
            db.check()
            if args.command == IMPORT_PROJECT:
                result = import_project(db, settings, args.project_id, args.directory)
            elif args.command == REGISTER_BINDINGS:
                result = register_bindings(db, settings, args.path)
            elif args.command == IMPORT_LEGACY:
                result = import_legacy(db, settings, args.sqlite_path)
            elif args.command == RECONCILE:
                result = {"confirmed": reconcile_attempt(db, args.attempt_id,
                                                        confirm_external_stopped=args.confirm_external_stopped)}
            else:
                if args.older_than_hours < 0 or not 1 <= args.limit <= 1000:
                    raise ValueError("GC age and limit are invalid")
                with db.transaction() as cursor:
                    cursor.execute(SQL_GC, (args.older_than_hours, args.limit))
                    identifiers = [row["set_id"] for row in cursor.fetchall()]
                store = _artifacts(db, settings)
                result = {"deleted": sum(store.delete_unreferenced(identifier) for identifier in identifiers)}
        print(json.dumps(result, ensure_ascii=False))
    finally:
        db.close()


if __name__ == "__main__":
    main()
