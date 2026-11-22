"""构建受理和历史查询。已受理的构建直接进入执行队列。"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import Final
from uuid import UUID, uuid4

from sqlalchemy import select, tuple_

from dbt_metricflow_service import constants as strings
from dbt_metricflow_service.models.builds import BuildRequest
from dbt_metricflow_service.models.execution import ExecutionSnapshot
from dbt_metricflow_service.storage.postgres import Database
from dbt_metricflow_service.storage.records import StoredBuild

from .entities import Build
from .paging import HISTORY_CURSOR_VERSION, read_cursor, write_cursor
from .rows import entity_record

CURSOR_TIMESTAMP_OFFSET_MESSAGE: Final = "cursor timestamp requires an offset"
INVALID_CURSOR_MESSAGE: Final = strings.INVALID_CURSOR_MESSAGE

JSON_MODE = strings.JSON_MODE
PROJECT_PREFIX = strings.ENGINE_IDEMPOTENCY_PREFIX


class BuildStore:
    def __init__(self, db: Database) -> None:
        # 只接收具体持久依赖，不拥有 Runtime 或 worker。
        self.db = db

    def get(self, build_id: UUID | str) -> StoredBuild | None:
        with self.db.session() as session:
            return entity_record(session.get(Build, str(build_id)), StoredBuild)

    def accept(
        self, request: BuildRequest, caller: str, toolchain: str,
        *, validate_new: Callable[[ExecutionSnapshot], None],
    ) -> StoredBuild:
        from .deployments import DeploymentStore

        key = str(request.idempotency_key)
        # 已排队的构建记录和可选的部署意图共用这一事务，任一步失败都会一起回滚。
        with self.db.fact_session() as session:
            # fact_session 已串行化受理事务。相同 UUID 始终指向首次受理的构建，重试直接返回。
            prior = session.scalar(select(Build).where(Build.idempotency_key == key))
            if prior:
                return entity_record(prior, StoredBuild)
            snapshot = request.execution
            validate_new(snapshot)
            body = request.model_dump(mode=JSON_MODE, by_alias=True)
            environment = str(snapshot.environment_id)

            # 构建记录本身就是队列项，初始状态由数据库设为 QUEUED。
            build_id = str(uuid4())
            build = Build(
                build_id=build_id,
                project_id=PROJECT_PREFIX + build_id,
                repository=request.repository,
                branch_name=request.branch_name,
                environment=environment,
                environment_id=environment,
                toolchain_version=toolchain,
                caller=caller,
                idempotency_key=key,
                request_json=body,
                commit_sha=request.commit_sha,
            )
            session.add(build)
            session.flush()
            row = entity_record(build, StoredBuild)
            # ON_SUCCESS 在受理时就登记部署顺序。实际切换由后台在构建成功并核对来源后完成。
            if request.deployment_policy == strings.ON_SUCCESS:
                DeploymentStore(self.db).accept_in_transaction(session, row)
            return row

    def list(
        self, repository: str | None, branch_name: str | None = None, cursor: str | None = None, limit: int = 50,
        *, environment_id: str, status: str | None = None,
    ) -> tuple[list[StoredBuild], str | None]:
        scope = [environment_id, repository, branch_name, status]
        after = read_cursor(scope, cursor, limit, version=HISTORY_CURSOR_VERSION)
        statement = select(Build).where(Build.environment_id == environment_id)
        if repository is not None:
            statement = statement.where(Build.repository == repository)
        if status is not None:
            statement = statement.where(Build.build_status == status)
        if branch_name is not None:
            statement = statement.where(Build.branch_name == branch_name)
        if after is not None:
            try:
                created_at, build_id = datetime.fromisoformat(after[:-36]), str(UUID(after[-36:]))
                if created_at.tzinfo is None:
                    raise ValueError(CURSOR_TIMESTAMP_OFFSET_MESSAGE)
            except ValueError as error:
                raise ValueError(INVALID_CURSOR_MESSAGE) from error
            statement = statement.where(tuple_(Build.created_at, Build.build_id) < tuple_(created_at, build_id))
        with self.db.session() as session:
            statement = statement.order_by(Build.created_at.desc(), Build.build_id.desc()).limit(limit + 1)
            rows = []
            for build in session.scalars(statement):
                rows.append(entity_record(build, StoredBuild))
        selected = rows[:limit]
        next_cursor = (
            write_cursor(
                scope, str(selected[-1].created_at) + selected[-1].build_id, version=HISTORY_CURSOR_VERSION,
            )
            if len(rows) > limit else None
        )
        return selected, next_cursor
