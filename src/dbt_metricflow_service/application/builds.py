"""构建受理与历史读取；不认识外部项目或 Agent 生命周期。"""

from __future__ import annotations

from datetime import UTC
from functools import partial
from typing import Final
from uuid import UUID

from dbt_metricflow_service import constants as strings
from dbt_metricflow_service.platform.execution_profile import ExecutionConfigurationError, validate_execution_target
from dbt_metricflow_service.settings import Settings
from dbt_metricflow_service.storage.builds import BuildStore
from dbt_metricflow_service.storage.records import StoredBuild, StoredDeploymentAttempt

from ..models.builds import BuildRequest, BuildStatus, BuildView, ExecutionLogPage, ExecutionLogView
from ..models.contracts import Page
from ..models.service import ErrorView
from ..storage.deployments import DeploymentStore
from ..storage.execution_logs import ExecutionLogStore
from ..storage.jobs import StoreConflict
from .deployments import attempt_view
from .errors import ServiceError

INCOMPLETE_EXECUTION_MESSAGE: Final = "engine execution did not complete"
MISSING_BUILD_MESSAGE: Final = strings.BUILD_NOT_FOUND_MESSAGE


class BuildService:
    def __init__(self, store: BuildStore, toolchain: str, settings: Settings) -> None:
        # 具体 Store 和固定执行参数足够，不传入整个 Runtime。
        self.store = store
        self.toolchain = toolchain
        # 实例只报告其可执行的版本；存量配置快照本身仍然保留。
        self.settings = settings

    def submit(self, request: BuildRequest, caller: str) -> BuildView:
        try:
            row = self.store.accept(
                request, caller, self.toolchain, self.settings.config_version,
                validate_new=partial(validate_execution_target, settings=self.settings),
            )
        except ExecutionConfigurationError as error:
            raise ServiceError(error.code, "execution target is not verified") from error
        except StoreConflict as error:
            code = str(error) if str(error).startswith(strings.EXECUTION_) else strings.IDEMPOTENCY_CONFLICT
            raise ServiceError(code, str(error), 409) from error
        return self.get(row.build_id)

    def get(self, build_id: UUID | str) -> BuildView:
        row = self.load(build_id)
        initial = DeploymentStore(self.store.db).initial(row.build_id)
        return self.view(row, initial)


    def list_by_environment(
        self, environment_id: UUID, repository: str | None = None, branch_name: str | None = None,
        status: BuildStatus | None = None, cursor: str | None = None, limit: int = 50,
    ) -> Page[BuildView]:
        try:
            rows, following = self.store.list(repository, branch_name, cursor, limit,
                                               environment_id=str(environment_id), status=status)
        except ValueError as error:
            raise ServiceError(strings.INVALID_CURSOR, str(error)) from error
        initials = DeploymentStore(self.store.db).initials([row.build_id for row in rows])
        return Page[BuildView](items=[self.view(row, initials.get(row.build_id))
                                              for row in rows], next_cursor=following)

    def load(self, build_id: UUID | str) -> StoredBuild:
        row = self.store.get(str(build_id))
        if row is None:
            raise ServiceError(strings.BUILD_NOT_FOUND, MISSING_BUILD_MESSAGE, 404)
        return row

    def execution_logs(self, build_id: UUID | str, cursor: str | None = None, limit: int = 100) -> ExecutionLogPage:
        build = self.load(build_id)
        try:
            rows, next_cursor, has_more = ExecutionLogStore(self.store.db).list(build.build_id, cursor, limit)
        except ValueError as error:
            raise ServiceError(strings.INVALID_CURSOR, str(error)) from error
        return ExecutionLogPage(items=[ExecutionLogView(
            log_id=str(row.log_id), job_id=row.job_id, attempt_id=row.attempt_id,
            phase=row.phase, command=row.command, event_time=row.event_time.astimezone(UTC), level=row.level,
            event_name=row.event_name, event_code=row.event_code, invocation_id=row.invocation_id,
            node_id=row.node_id, thread_name=row.thread_name, message=row.message,
            created_at=row.created_at.astimezone(UTC),
        ) for row in rows], next_cursor=next_cursor, has_more=has_more)

    def view(
        self, row: StoredBuild, initial: StoredDeploymentAttempt | None,
    ) -> BuildView:
        # 响应组装不隐含数据库读取。
        catalog = row.build_status == BuildStatus.SUCCEEDED and bool(row.output_set_id)
        return BuildView(
            build_id=row.build_id,
            repository=row.repository,
            branch_name=row.branch_name,
            requested_commit_sha=row.requested_commit_sha,
            commit_sha=row.commit_sha,
            toolchain_version=row.toolchain_version,
            build_status=row.build_status,
            phase=row.phase,
            created_at=row.created_at,
            updated_at=row.updated_at,
            finished_at=row.finished_at,
            catalog_available=catalog,
            initial_deployment=attempt_view(initial) if initial else None,
            error=ErrorView(
                code=row.error_code,
                message=INCOMPLETE_EXECUTION_MESSAGE,
                phase=row.phase,
                build_id=row.build_id,
            )
            if row.error_code
            else None,
            execution=row.config_snapshot,
        )
