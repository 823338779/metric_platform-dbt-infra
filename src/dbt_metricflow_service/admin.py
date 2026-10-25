"""数据库初始化与运行时管理入口；连接信息从服务配置和环境变量加载。"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Final
from uuid import UUID

from dbt_metricflow_service import constants as strings
from dbt_metricflow_service.bootstrap import current_toolchain
from dbt_metricflow_service.execution.artifacts import _is_link
from dbt_metricflow_service.models.configuration import ConfigurationModel
from dbt_metricflow_service.models.payloads import JsonObject, JsonValue
from dbt_metricflow_service.platform.bindings import PROJECT_NAME_PATTERN
from dbt_metricflow_service.settings import Settings
from dbt_metricflow_service.storage.artifacts import ArtifactStore
from dbt_metricflow_service.storage.jobs import JobStore
from dbt_metricflow_service.storage.postgres import Database
from dbt_metricflow_service.storage.rows import row_record

# 管理命令及输入格式常量集中定义，所有请求正文均经公开模型验证。
INVALID_PROJECT_IDENTIFIER: Final[str] = "project identifier is invalid"
LINKED_INPUT_FORBIDDEN: Final[str] = "input cannot contain linked paths"
JSON_INPUT_TOO_LARGE: Final[str] = "JSON file size limit exceeded"
INVALID_JSON_STRUCTURE: Final[str] = "JSON structure is invalid"
EXTERNAL_STOP_CONFIRMATION_REQUIRED: Final[str] = "explicit external execution stop confirmation is required"
ATTEMPT_NOT_AWAITING_CONFIRMATION: Final[str] = "attempt is not awaiting external stop confirmation"
ADMIN_DESCRIPTION: Final[str] = "PostgreSQL 数据库初始化与运行时管理"
DATABASE_URL_REQUIRED: Final[str] = "SERVICE_DATABASE_URL is required"
INVALID_GC_OPTIONS: Final[str] = "GC age and limit are invalid"


INIT_DB = "init-db"
RECONCILE = strings.ADMIN_RECONCILE_ATTEMPT_COMMAND
GC = "gc"
BUILD = strings.ADMIN_BUILD_COMMAND
SQL_ATTEMPT = "SELECT lease_token,state FROM runtime_attempt WHERE attempt_id=%s"
SQL_GC = """SELECT set_id FROM runtime_artifact_set
 WHERE created_at < clock_timestamp() - %s * interval '1 hour' ORDER BY created_at LIMIT %s"""


class AttemptState(ConfigurationModel):
    lease_token: str
    state: str


class InitializationResult(ConfigurationModel):
    initialized: bool


class ReconciliationResult(ConfigurationModel):
    confirmed: bool


class GarbageCollectionResult(ConfigurationModel):
    deleted: int


def _artifacts(db: Database, settings: Settings) -> ArtifactStore:
    return ArtifactStore(db, max_file_bytes=settings.max_artifact_file_bytes, max_set_bytes=settings.max_artifact_bytes)


def _project_id(value: str) -> str:
    if not isinstance(value, str) or not PROJECT_NAME_PATTERN.fullmatch(value):
        raise ValueError(INVALID_PROJECT_IDENTIFIER)
    return value


def _json(path: Path, maximum: int) -> JsonObject | list[JsonValue]:
    # 控制文件不会作为产物保存，读取前仍校验链接与长度。
    if any(_is_link(item) for item in (path, *path.parents)):
        raise ValueError(LINKED_INPUT_FORBIDDEN)
    with path.open(strings.BINARY_READ_MODE) as stream:
        content = stream.read(maximum + 1)
    if len(content) > maximum:
        raise ValueError(JSON_INPUT_TOO_LARGE)
    value = json.loads(content)
    if not isinstance(value, (dict, list)):
        raise ValueError(INVALID_JSON_STRUCTURE)
    return value


def reconcile_attempt(db: Database, attempt_id: str, *, confirm_external_stopped: bool) -> bool:
    """运维核实外部执行结束后释放保护；不提供自动判定或 HTTP 入口。"""
    if not confirm_external_stopped:
        raise ValueError(EXTERNAL_STOP_CONFIRMATION_REQUIRED)
    with db.transaction() as connection:
        sql_result = connection.exec_driver_sql(SQL_ATTEMPT, (str(UUID(attempt_id)),))
        attempt = row_record(sql_result, AttemptState)
    if not attempt or attempt.state != strings.EXPIRED_UNCONFIRMED:
        raise ValueError(ATTEMPT_NOT_AWAITING_CONFIRMATION)
    return JobStore(db).confirm_stopped(attempt_id, attempt.lease_token)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=ADMIN_DESCRIPTION)
    commands = parser.add_subparsers(dest=strings.ADMIN_COMMAND_DESTINATION, required=True)
    commands.add_parser(INIT_DB)
    build = commands.add_parser(BUILD)
    build.add_argument(strings.ADMIN_FILE_OPTION, type=Path, required=True)
    reconcile = commands.add_parser(RECONCILE)
    reconcile.add_argument(strings.ADMIN_ATTEMPT_ID_ARGUMENT)
    reconcile.add_argument(
        strings.ADMIN_CONFIRM_EXTERNAL_STOPPED_OPTION, action=strings.ADMIN_STORE_TRUE_ACTION, required=True
    )
    gc = commands.add_parser(GC)
    gc.add_argument(strings.ADMIN_OLDER_THAN_HOURS_OPTION, type=int, default=24)
    gc.add_argument(strings.ADMIN_LIMIT_OPTION, type=int, default=100)
    args = parser.parse_args(argv)
    # 管理操作与服务启动读取同一配置，避免初始化和运行连接到不同数据库。
    settings = Settings.from_file()
    if not settings.database_url:
        parser.error(DATABASE_URL_REQUIRED)
    db = Database(settings.database_url)
    try:
        if args.command == INIT_DB:
            db.initialize()
            result = InitializationResult(initialized=True)
        else:
            db.check()
            if args.command == BUILD:
                from dbt_metricflow_service.application.builds import BuildService
                from dbt_metricflow_service.models.execution import SnapshotBuildRequest
                from dbt_metricflow_service.platform.execution_profile import validate_execution_target
                from dbt_metricflow_service.storage.builds import BuildStore

                service = BuildService(BuildStore(db), settings.toolchain_version or current_toolchain(),
                                       settings.config_version)
                request = SnapshotBuildRequest.model_validate(_json(args.file, settings.max_artifact_file_bytes))
                validate_execution_target(request.execution, settings)
                result = service.submit_snapshot(request, strings.ADMIN_IDENTITY)
            elif args.command == RECONCILE:
                result = ReconciliationResult(confirmed=reconcile_attempt(
                    db, args.attempt_id, confirm_external_stopped=args.confirm_external_stopped,
                ))
            else:
                if args.older_than_hours < 0 or not 1 <= args.limit <= 1000:
                    raise ValueError(INVALID_GC_OPTIONS)
                with db.transaction() as connection:
                    sql_result = connection.exec_driver_sql(SQL_GC, (args.older_than_hours, args.limit))
                    identifiers = list(sql_result.scalars())
                store = _artifacts(db, settings)
                result = GarbageCollectionResult(
                    deleted=sum(store.delete_unreferenced(identifier) for identifier in identifiers),
                )
        print(result.model_dump_json(by_alias=True))
    finally:
        db.close()


if __name__ == strings.MODULE_ENTRYPOINT:
    main()
