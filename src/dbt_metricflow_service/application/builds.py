"""构建受理与历史读取；不认识外部项目或 Agent 生命周期。"""

from __future__ import annotations

from uuid import UUID

from dbt_metricflow_service.models.builds import BuildRequest, ChangePage
from dbt_metricflow_service.storage.builds import BuildStore
from dbt_metricflow_service.storage.records import StoredBuild

from ..models.builds import BuildStatus, BuildView, ErrorView, LogPage, Page
from ..storage.changes import ChangeStore
from ..storage.deployments import DeploymentStore, attempt_view
from ..storage.jobs import StoreConflict
from ..storage.paging import page
from .errors import ServiceError

JSON_MODE = "json"
NOT_SUCCEEDED = "BUILD_NOT_SUCCEEDED"
REMOVED = "PHYSICAL_OBJECTS_REMOVED"
MISSING = "ARTIFACT_UNAVAILABLE"
ENVIRONMENT_MISSING = "EXECUTION_ENVIRONMENT_UNAVAILABLE"


class BuildService:
    def __init__(self, store: BuildStore, toolchain: str, timeout: int, config_version: str | None=None) -> None:
        # 具体 Store 和固定执行参数足够，不传入整个 Runtime。
        self.store = store
        self.toolchain = toolchain
        self.timeout = timeout
        # 实例只报告其可执行的版本；存量配置快照本身仍然保留。
        self.config_version = config_version

    def submit(self, request: BuildRequest, caller: str) -> BuildView:
        try:
            row = self.store.accept(request, caller, self.toolchain, self.timeout)
        except StoreConflict as error:
            raise ServiceError("IDEMPOTENCY_CONFLICT", str(error), 409) from error
        except ValueError as error:
            raise ServiceError("INVALID_BINDING", str(error)) from error
        return self.view(row)

    def get(self, build_id: UUID | str) -> BuildView:
        row = self.store.get(str(build_id))
        if row is None:
            raise ServiceError("BUILD_NOT_FOUND", "build does not exist", 404)
        return self.view(row)

    def pin_source(self, build_id: UUID | str, commit_sha: str, fencing_token: UUID | str) -> BuildView:
        return self.view(self.store.pin_source(str(build_id), commit_sha, fencing_token))

    def cancel(self, build_id: UUID | str, caller: str) -> tuple[BuildView, int]:
        # caller 已由入口验证；取消没有新的外部任务身份。
        self.get(build_id)
        try:
            status = self.store.cancel(str(build_id))
        except StoreConflict as error:
            raise ServiceError("BUILD_TERMINAL", str(error), 409, build_id=str(build_id)) from error
        return self.get(build_id), status

    def list(
        self, repository: str, branch_name: str | None = None, cursor: str | None = None, limit: int = 50
    ) -> Page[BuildView]:
        rows = [row for row in self.store.list(repository) if branch_name is None or row["branch_name"] == branch_name]
        try:
            rows, next_cursor = page(
                rows,
                scope=[repository, branch_name],
                cursor=cursor,
                limit=limit,
                reverse=True,
                identity=lambda row: str(row["created_at"]) + row["build_id"],
            )
        except ValueError as error:
            raise ServiceError("INVALID_CURSOR", str(error)) from error
        return Page[BuildView](items=[self.view(row) for row in rows], next_cursor=next_cursor)

    def changes(self, cursor: str | None=None, limit: int=50) -> ChangePage:
        try:
            return ChangeStore(self.store.db).read(cursor, limit)
        except ValueError as error:
            raise ServiceError("INVALID_CURSOR", str(error)) from error

    def logs(self, build_id: UUID | str, cursor: str | None=None) -> LogPage:
        self.get(build_id)
        try:
            rows, next_cursor = self.store.logs(str(build_id), cursor)
        except ValueError as error:
            raise ServiceError("INVALID_CURSOR", str(error)) from error
        # 持久阶段和错误码不含 CLI、凭据或本地路径；游标可继续轮询。
        items = [
            " | ".join(
                str(value)
                for value in (row["summary"]["phase"], row["summary"]["build_status"], row["summary"].get("error_code"))
                if value
            )
            for row in rows
        ]
        return LogPage(items=items, next_cursor=next_cursor, truncated=False)

    def view(self, row: StoredBuild) -> BuildView:
        # 目录能力与物理执行能力独立；后续目录读取仍校验封存字节。
        succeeded = row["build_status"] == BuildStatus.SUCCEEDED
        catalog = succeeded and bool(row["output_set_id"])
        reason = None
        if not succeeded:
            reason = NOT_SUCCEEDED
        elif not catalog:
            reason = MISSING
        elif row.get("run_lifecycle") in {"CLEANED", "CLEANING"}:
            reason = REMOVED
        elif (
            row["toolchain_version"] != self.toolchain
            or self.config_version is not None
            and row["config_version"] != self.config_version
        ):
            reason = ENVIRONMENT_MISSING
        fields = {key: row[key] for key in BuildView.model_fields if key in row}
        initial = DeploymentStore(self.store.db).initial(row["build_id"])
        fields.update(
            catalog_available=catalog,
            query_available=reason is None,
            query_unavailable_reason=reason,
            initial_deployment=(attempt_view(initial).model_dump(mode=JSON_MODE, by_alias=True) if initial else None),
            error=ErrorView.model_validate(
                {
                    "code": row["error_code"],
                    "message": "engine execution did not complete",
                    "phase": row["phase"],
                    "build_id": row["build_id"],
                }
            )
            if row["error_code"]
            else None,
        )
        return BuildView(**fields)
