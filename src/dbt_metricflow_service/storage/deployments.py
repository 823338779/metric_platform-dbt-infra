"""部署自然键与接收顺序；活动指针不属于构建终态。"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Final
from uuid import UUID

from sqlalchemy import func, select, tuple_, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session
from sqlalchemy.sql.elements import ColumnElement

from dbt_metricflow_service import constants as strings
from dbt_metricflow_service.models.deployments import DeploymentAttemptView, DeploymentKey, DeploymentRequest
from dbt_metricflow_service.storage.postgres import Database
from dbt_metricflow_service.storage.records import StoredBuild, StoredDeploymentAttempt, StoredDeploymentTarget

from .entities import Build, DeploymentAttempt, DeploymentTarget, RuntimeJob
from .jobs import StoreConflict
from .paging import HISTORY_CURSOR_VERSION, read_cursor, write_cursor
from .rows import entity_record

DEPLOYMENT_INPUT_CONFLICT_MESSAGE: Final = "deployment key already binds another input"
DEPLOYMENT_VERSION_CONFLICT_MESSAGE: Final = "deployment target version changed"
INVALID_CURSOR_IDENTITY_MESSAGE: Final = "invalid cursor identity"
INVALID_CURSOR_MESSAGE: Final = strings.INVALID_CURSOR_MESSAGE

BUILD_OPERATION = strings.BUILD
DEPLOYMENT_OPERATION = strings.DEPLOYMENT
BUILD_KEY_PREFIX = strings.BUILD_IDEMPOTENCY_PREFIX
SUCCEEDED = strings.SUCCEEDED
PENDING = strings.PENDING
WAITING_FOR_BUILD = strings.WAITING_FOR_BUILD
CHECKING = strings.CHECKING
DEPLOYED = strings.DEPLOYED
STALE = strings.STALE
NEWER_INTENT = strings.NEWER_INTENT
TARGET_OR_BUILD_CHANGED = strings.TARGET_OR_BUILD_CHANGED
MATCHED = strings.MATCHED
MISSING = strings.MISSING
TERMINAL = frozenset({DEPLOYED, STALE, strings.FAILED, strings.CANCELLED})
PENDING_STATUSES = (WAITING_FOR_BUILD, PENDING, CHECKING)


def _natural(
    entity: type[DeploymentTarget] | type[DeploymentAttempt], key: tuple[str, str, str]
) -> tuple[ColumnElement[bool], ...]:
    # 目标和部署尝试使用相同的自然键，组合条件不改变锁定次序。
    return entity.repository == key[0], entity.environment == key[1], entity.branch_name == key[2]


def _stale(session: Session, key: tuple[str, str, str]) -> None:
    session.execute(
        update(DeploymentAttempt)
        .where(*_natural(DeploymentAttempt, key), DeploymentAttempt.deployment_status.in_(PENDING_STATUSES))
        .values(deployment_status=STALE, reason=NEWER_INTENT, version=DeploymentAttempt.version + 1)
    )


class DeploymentStore:
    def __init__(self, db: Database) -> None:
        # 与构建队列共用连接池；不独立执行外部 Git 操作。
        self.db = db

    def accept_in_transaction(
        self,
        session: Session,
        build: StoredBuild,
        caller: str,
        key: str,
        fingerprint: str,
        branch: str,
        operation: str = DEPLOYMENT_OPERATION,
    ) -> StoredDeploymentAttempt:
        natural_key = (build.repository, build.environment, branch)
        session.execute(
            pg_insert(DeploymentTarget)
            .values(repository=natural_key[0], environment=natural_key[1], branch_name=branch)
            .on_conflict_do_nothing()
        )
        target = session.get(DeploymentTarget, natural_key, with_for_update=True, populate_existing=True)
        _stale(session, natural_key)
        target.desired_generation += 1
        target.version += 1
        session.flush()
        attempt = DeploymentAttempt(
            repository=natural_key[0],
            environment=natural_key[1],
            branch_name=branch,
            generation=target.desired_generation,
            build_id=build.build_id,
            caller=caller,
            idempotency_key=key,
            request_digest=fingerprint,
            deployment_status=PENDING if build.build_status == SUCCEEDED else WAITING_FOR_BUILD,
            operation=operation,
        )
        session.add(attempt)
        session.flush()
        return entity_record(attempt, StoredDeploymentAttempt)

    def initial(self, build_id: UUID | str) -> StoredDeploymentAttempt | None:
        return self.initials([str(build_id)]).get(str(build_id))

    def initials(self, build_ids: list[str]) -> dict[str, StoredDeploymentAttempt]:
        if not build_ids:
            return {}
        with self.db.session() as session:
            rows = session.scalars(select(DeploymentAttempt).where(
                DeploymentAttempt.build_id.in_(build_ids),
                DeploymentAttempt.operation == BUILD_OPERATION,
                DeploymentAttempt.idempotency_key == func.concat(BUILD_KEY_PREFIX, DeploymentAttempt.build_id),
            ))
            return {row.build_id: entity_record(row, StoredDeploymentAttempt) for row in rows}

    def current(self, key: DeploymentKey) -> StoredDeploymentTarget | None:
        with self.db.session() as session:
            row = session.get(DeploymentTarget, natural(key))
            return entity_record(row, StoredDeploymentTarget)

    def attempt(self, key: DeploymentKey, generation: int) -> StoredDeploymentAttempt | None:
        with self.db.session() as session:
            row = session.get(DeploymentAttempt, (*natural(key), generation))
            return entity_record(row, StoredDeploymentAttempt)

    def by_key(self, repository: str, caller: str, key: str) -> StoredDeploymentAttempt | None:
        with self.db.session() as session:
            row = session.scalar(
                select(DeploymentAttempt).where(
                    DeploymentAttempt.repository == repository,
                    DeploymentAttempt.caller == caller,
                    DeploymentAttempt.operation == DEPLOYMENT_OPERATION,
                    DeploymentAttempt.idempotency_key == key,
                )
            )
            return entity_record(row, StoredDeploymentAttempt)

    def submit(
        self, build: StoredBuild, caller: str, request: DeploymentRequest, fingerprint: str
    ) -> StoredDeploymentAttempt:
        with self.db.fact_session() as session:
            # 使用同一 advisory lock 键序列化部署幂等受理，不依赖构建层的 SQL 常量。
            scope = build.repository + caller + request.idempotency_key
            session.execute(select(func.pg_advisory_xact_lock(func.hashtextextended(scope, 0))))
            prior = session.scalar(
                select(DeploymentAttempt).where(
                    DeploymentAttempt.repository == build.repository,
                    DeploymentAttempt.caller == caller,
                    DeploymentAttempt.operation == DEPLOYMENT_OPERATION,
                    DeploymentAttempt.idempotency_key == request.idempotency_key,
                )
            )
            if prior:
                if prior.request_digest != fingerprint:
                    raise StoreConflict(DEPLOYMENT_INPUT_CONFLICT_MESSAGE)
                return entity_record(prior, StoredDeploymentAttempt)
            key = (build.repository, build.environment, request.branch_name)
            session.execute(
                pg_insert(DeploymentTarget)
                .values(repository=key[0], environment=key[1], branch_name=key[2])
                .on_conflict_do_nothing()
            )
            current = session.get(DeploymentTarget, key, with_for_update=True)
            if current.version != request.expected_target_version:
                raise StoreConflict(DEPLOYMENT_VERSION_CONFLICT_MESSAGE)
            return self.accept_in_transaction(
                session, build, caller, request.idempotency_key, fingerprint, request.branch_name
            )

    def observe(
        self,
        key: DeploymentKey,
        head: str | None,
        state: str,
        *,
        expected_version: int,
        expected_active_build_id: str | None,
    ) -> StoredDeploymentTarget | None:
        with self.db.fact_session() as session:
            target = session.get(DeploymentTarget, natural(key), with_for_update=True)
            if not target or target.version != expected_version or target.active_build_id != expected_active_build_id:
                return None
            target.observed_head_sha = head
            target.head_observed_at = func.clock_timestamp()
            target.source_state = state
            if state == MISSING:
                target.version += int(target.active_build_id is not None)
                target.active_build_id = None
            session.flush()
            if state == MISSING:
                _stale(session, natural(key))
            return entity_record(target, StoredDeploymentTarget)

    def settle(
        self,
        key: DeploymentKey,
        generation: int,
        status: str,
        reason: str | None = None,
        *,
        expected_version: int | None = None,
        head: str | None = None,
    ) -> StoredDeploymentAttempt | None:
        with self.db.fact_session() as session:
            target = session.get(DeploymentTarget, natural(key), with_for_update=True)
            attempt = session.get(DeploymentAttempt, (*natural(key), generation))
            if not attempt or attempt.deployment_status in TERMINAL:
                return entity_record(attempt, StoredDeploymentAttempt)
            if target.desired_generation != generation:
                status, reason = STALE, NEWER_INTENT
            if status == DEPLOYED:
                build = session.scalar(
                    select(Build)
                    .join(RuntimeJob, Build.run_id == RuntimeJob.job_id)
                    .where(Build.build_id == attempt.build_id)
                    .with_for_update(of=(RuntimeJob, Build))
                )
                # 锁定构建与执行记录；head 只比较事务前已完成的观察值。
                if (
                    target.version != expected_version
                    or build.build_status != SUCCEEDED
                    or not build.output_set_id
                    or build.commit_sha != head
                ):
                    status, reason = STALE, TARGET_OR_BUILD_CHANGED
                else:
                    target.active_build_id = build.build_id
                    target.version += 1
                    target.source_state = MATCHED
                    session.flush()
            # 已持目标行锁，串行化此目标下的所有结算；刷新后读取数据库时间。
            attempt.deployment_status = status
            attempt.reason = reason
            attempt.version += 1
            if status == DEPLOYED:
                attempt.deployed_at = func.clock_timestamp()
            session.flush()
            return entity_record(attempt, StoredDeploymentAttempt)

    def pending(self) -> list[StoredDeploymentAttempt]:
        with self.db.session() as session:
            # 候选选择与更新时间保持单语句，多个协调器不会认领同一批锁定行。
            candidates = (
                select(
                    DeploymentAttempt.repository,
                    DeploymentAttempt.environment,
                    DeploymentAttempt.branch_name,
                    DeploymentAttempt.generation,
                )
                .where(DeploymentAttempt.deployment_status.in_(PENDING_STATUSES))
                .order_by(DeploymentAttempt.last_checked_at.nulls_first(), DeploymentAttempt.created_at)
                .limit(100)
                .with_for_update(skip_locked=True)
                .cte()
            )
            statement = (
                update(DeploymentAttempt)
                .where(
                    tuple_(
                        DeploymentAttempt.repository,
                        DeploymentAttempt.environment,
                        DeploymentAttempt.branch_name,
                        DeploymentAttempt.generation,
                    )
                    == tuple_(
                        candidates.c.repository,
                        candidates.c.environment,
                        candidates.c.branch_name,
                        candidates.c.generation,
                    )
                )
                .values(last_checked_at=func.clock_timestamp())
                .returning(DeploymentAttempt)
            )
            rows = session.scalars(statement, execution_options={strings.SQLALCHEMY_SYNCHRONIZE_SESSION_OPTION: False})
            return [entity_record(row, StoredDeploymentAttempt) for row in rows]

    def list(
        self, repository: str, environment: str | None = None, branch_name: str | None = None,
        cursor: str | None = None, limit: int = 50,
    ) -> tuple[list[StoredDeploymentAttempt], str | None]:
        scope = [repository, environment, branch_name]
        after = read_cursor(scope, cursor, limit, version=HISTORY_CURSOR_VERSION)
        columns = (
            DeploymentAttempt.created_at,
            DeploymentAttempt.repository.collate(strings.POSTGRES_C_COLLATION),
            DeploymentAttempt.environment.collate(strings.POSTGRES_C_COLLATION),
            DeploymentAttempt.branch_name.collate(strings.POSTGRES_C_COLLATION),
            DeploymentAttempt.generation,
        )
        statement = select(DeploymentAttempt).where(DeploymentAttempt.repository == repository)
        if environment is not None:
            statement = statement.where(DeploymentAttempt.environment == environment)
        if branch_name is not None:
            statement = statement.where(DeploymentAttempt.branch_name == branch_name)
        if after is not None:
            try:
                timestamp, separator, identity = after.partition("[")
                created_at = datetime.fromisoformat(timestamp)
                key = json.loads(separator + identity)
                if (created_at.tzinfo is None or not isinstance(key, list) or len(key) != 4
                        or not all(isinstance(part, str) for part in key[:3]) or type(key[3]) is not int):
                    raise ValueError(INVALID_CURSOR_IDENTITY_MESSAGE)
            except (ValueError, TypeError) as error:
                raise ValueError(INVALID_CURSOR_MESSAGE) from error
            statement = statement.where(tuple_(*columns) < tuple_(created_at, *key))
        with self.db.session() as session:
            rows = session.scalars(statement.order_by(*(column.desc() for column in columns)).limit(limit + 1))
            records = [entity_record(row, StoredDeploymentAttempt) for row in rows]
        selected = records[:limit]
        next_cursor = None
        if len(records) > limit:
            last = selected[-1]
            identity = json.dumps([last.repository, last.environment, last.branch_name, last.generation])
            next_cursor = write_cursor(scope, str(last.created_at) + identity, version=HISTORY_CURSOR_VERSION)
        return selected, next_cursor


def natural(key: DeploymentKey) -> tuple[str, str, str]:
    return key.repository, key.environment, key.branch_name


def attempt_view(row: StoredDeploymentAttempt) -> DeploymentAttemptView:
    # 显式白名单防止内部幂等来源和请求摘要泄漏到 API。
    from ..models.deployments import DeploymentAttemptView
    return DeploymentAttemptView(
        repository=row.repository, environment=row.environment, branch_name=row.branch_name,
        generation=row.generation, build_id=UUID(row.build_id), deployment_status=row.deployment_status,
        reason=row.reason, created_at=row.created_at, deployed_at=row.deployed_at,
    )
