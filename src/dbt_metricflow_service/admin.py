"""部署与迁移管理入口；只读旧库，连接信息从服务配置和环境变量加载。"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from uuid import UUID

from dbt_metricflow_service.execution.artifacts import _is_link
from dbt_metricflow_service.platform.bindings import PROJECT_NAME_PATTERN
from dbt_metricflow_service.runtime.service import current_toolchain
from dbt_metricflow_service.settings import Settings
from dbt_metricflow_service.storage.artifacts import ArtifactStore
from dbt_metricflow_service.storage.jobs import JobStore
from dbt_metricflow_service.storage.postgres import Database
from dbt_metricflow_service.storage.rows import row_dict

# 管理命令及历史格式常量集中定义，所有请求正文均经公开模型验证。
MIGRATE = "migrate"
REGISTER_BINDINGS = "register-bindings"
RECONCILE = "reconcile-attempt"
GC = "gc"
BUILD = "build"
MIGRATE_HISTORY = "migrate-history"
IMPORT_PUBLICATION = "import-publication"
BINDING_KEYS = frozenset({"repository", "executionBinding", "configVersion", "config"})
IDENTITY_KEYS = ("repository", "executionBinding", "configVersion")
SQL_ATTEMPT = "SELECT lease_token,state FROM runtime_attempt WHERE attempt_id=%s"
SQL_GC = """SELECT set_id FROM runtime_artifact_set
 WHERE created_at < clock_timestamp() - %s * interval '1 hour' ORDER BY created_at LIMIT %s"""


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


def register_bindings(db: Database, settings: Settings, path: Path) -> list[str]:
    """登记仓库允许使用的不可变执行配置，无业务项目注册。"""
    from dbt_metricflow_service.models.builds import ConfigSnapshot
    from dbt_metricflow_service.storage.builds import BuildStore

    records = _json(path, settings.max_artifact_file_bytes)
    if not isinstance(records, list):
        raise ValueError("bindings must be a list")
    parsed = []
    for record in records:
        if not isinstance(record, dict) or set(record) != BINDING_KEYS:
            raise ValueError("binding fields are invalid")
        config = ConfigSnapshot.model_validate(record["config"])
        if any(not isinstance(record[key], str) or not record[key] for key in IDENTITY_KEYS):
            raise ValueError("binding identity is required")
        parsed.append((record, config))
    # 输入整体校验后再登记，每个版本不可被后续命令静默覆盖。
    store = BuildStore(db)
    for record, config in parsed:
        store.register_binding(record["repository"], record["executionBinding"], record["configVersion"],
                               config.model_dump(mode="json", by_alias=True, exclude_none=True))
    return [record["executionBinding"] for record, _ in parsed]


def reconcile_attempt(db: Database, attempt_id: str, *, confirm_external_stopped: bool) -> bool:
    """运维核实外部执行结束后释放保护；不提供自动判定或 HTTP 入口。"""
    if not confirm_external_stopped:
        raise ValueError("explicit external execution stop confirmation is required")
    with db.transaction() as connection:
        sql_result = connection.exec_driver_sql(SQL_ATTEMPT, (str(UUID(attempt_id)),))
        attempt = row_dict(sql_result)
    if not attempt or attempt["state"] != "EXPIRED_UNCONFIRMED":
        raise ValueError("attempt is not awaiting external stop confirmation")
    return JobStore(db).confirm_stopped(attempt_id, attempt["lease_token"])


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="PostgreSQL 运行时管理及旧记录只读迁移")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser(MIGRATE)
    build = commands.add_parser(BUILD)
    build.add_argument("--file", type=Path, required=True)
    history = commands.add_parser(MIGRATE_HISTORY)
    history.add_argument("--bindings", type=Path, required=True)
    history.add_argument("--dry-run", action="store_true")
    publication_import = commands.add_parser(IMPORT_PUBLICATION)
    publication_import.add_argument("--file", type=Path, required=True)
    publication_import.add_argument("--dry-run", action="store_true")
    bindings = commands.add_parser(REGISTER_BINDINGS)
    bindings.add_argument("path", type=Path)
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
            if args.command == IMPORT_PUBLICATION:
                from dbt_metricflow_service.storage.publication_import import import_publication

                result = import_publication(db, _artifacts(db, settings),
                                            _json(args.file, settings.max_artifact_file_bytes), dry_run=args.dry_run)
            elif args.command == MIGRATE_HISTORY:
                from dbt_metricflow_service.storage.migration import migrate_history

                with db.transaction() as connection:
                    result = migrate_history(connection, _json(args.bindings, settings.max_artifact_file_bytes),
                                             dry_run=args.dry_run)
            elif args.command == BUILD:
                from dbt_metricflow_service.application.builds import BuildService
                from dbt_metricflow_service.models.builds import BuildRequest
                from dbt_metricflow_service.storage.builds import BuildStore

                service = BuildService(BuildStore(db), settings.toolchain_version or current_toolchain(),
                                       settings.command_timeout_seconds)
                request = BuildRequest.model_validate(_json(args.file, settings.max_artifact_file_bytes))
                result = service.submit(request, "admin").model_dump(mode="json", by_alias=True)
            elif args.command == REGISTER_BINDINGS:
                result = register_bindings(db, settings, args.path)
            elif args.command == RECONCILE:
                result = {"confirmed": reconcile_attempt(db, args.attempt_id,
                                                        confirm_external_stopped=args.confirm_external_stopped)}
            else:
                if args.older_than_hours < 0 or not 1 <= args.limit <= 1000:
                    raise ValueError("GC age and limit are invalid")
                with db.transaction() as connection:
                    sql_result = connection.exec_driver_sql(SQL_GC, (args.older_than_hours, args.limit))
                    identifiers = [row["set_id"] for row in sql_result.mappings()]
                store = _artifacts(db, settings)
                result = {"deleted": sum(store.delete_unreferenced(identifier) for identifier in identifiers)}
        print(json.dumps(result, ensure_ascii=False))
    finally:
        db.close()


if __name__ == "__main__":
    main()
