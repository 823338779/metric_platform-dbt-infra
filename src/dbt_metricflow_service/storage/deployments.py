"""部署自然键与接收顺序；活动指针不属于构建终态。"""

from __future__ import annotations

from typing import cast
from uuid import UUID

from sqlalchemy import func, select, tuple_, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session
from sqlalchemy.sql.elements import ColumnElement

from dbt_metricflow_service.models.deployments import DeploymentAttemptView, DeploymentKey, DeploymentRequest
from dbt_metricflow_service.storage.postgres import Database
from dbt_metricflow_service.storage.records import StoredBuild, StoredDeploymentAttempt, StoredDeploymentTarget

from .entities import Build, DeploymentAttempt, DeploymentTarget, RuntimeJob
from .jobs import StoreConflict
from .rows import entity_dict

BUILD_OPERATION = "BUILD"
DEPLOYMENT_OPERATION = "DEPLOYMENT"
BUILD_KEY_PREFIX = "build:"
SUCCEEDED = "SUCCEEDED"
PENDING = "PENDING"
WAITING_FOR_BUILD = "WAITING_FOR_BUILD"
CHECKING = "CHECKING"
DEPLOYED = "DEPLOYED"
STALE = "STALE"
NEWER_INTENT = "NEWER_INTENT"
TARGET_OR_BUILD_CHANGED = "TARGET_OR_BUILD_CHANGED"
MATCHED = "MATCHED"
MISSING = "MISSING"
ACTIVE = "ACTIVE"
TERMINAL = frozenset({DEPLOYED, STALE, "FAILED", "CANCELLED"})
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
        natural_key = (build["repository"], build["environment"], branch)
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
            build_id=build["build_id"],
            caller=caller,
            idempotency_key=key,
            request_digest=fingerprint,
            deployment_status=PENDING if build["build_status"] == SUCCEEDED else WAITING_FOR_BUILD,
            operation=operation,
        )
        session.add(attempt)
        session.flush()
        return cast(StoredDeploymentAttempt, entity_dict(attempt))

    def initial(self, build_id: UUID | str) -> StoredDeploymentAttempt | None:
        with self.db.session() as session:
            row = session.scalar(
                select(DeploymentAttempt).where(
                    DeploymentAttempt.build_id == str(build_id),
                    DeploymentAttempt.operation == BUILD_OPERATION,
                    DeploymentAttempt.idempotency_key == BUILD_KEY_PREFIX + str(build_id),
                )
            )
            return cast(StoredDeploymentAttempt | None, entity_dict(row))

    def current(self, key: DeploymentKey) -> StoredDeploymentTarget | None:
        with self.db.session() as session:
            row = session.get(DeploymentTarget, natural(key))
            return cast(StoredDeploymentTarget | None, entity_dict(row))

    def attempt(self, key: DeploymentKey, generation: int) -> StoredDeploymentAttempt | None:
        with self.db.session() as session:
            row = session.get(DeploymentAttempt, (*natural(key), generation))
            return cast(StoredDeploymentAttempt | None, entity_dict(row))

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
            return cast(StoredDeploymentAttempt | None, entity_dict(row))

    def submit(
        self, build: StoredBuild, caller: str, request: DeploymentRequest, fingerprint: str
    ) -> StoredDeploymentAttempt:
        with self.db.fact_session() as session:
            # 使用同一 advisory lock 键序列化部署幂等受理，不依赖构建层的 SQL 常量。
            scope = build["repository"] + caller + request.idempotency_key
            session.execute(select(func.pg_advisory_xact_lock(func.hashtextextended(scope, 0))))
            prior = session.scalar(
                select(DeploymentAttempt).where(
                    DeploymentAttempt.repository == build["repository"],
                    DeploymentAttempt.caller == caller,
                    DeploymentAttempt.operation == DEPLOYMENT_OPERATION,
                    DeploymentAttempt.idempotency_key == request.idempotency_key,
                )
            )
            if prior:
                if prior.request_digest != fingerprint:
                    raise StoreConflict("deployment key already binds another input")
                return cast(StoredDeploymentAttempt, entity_dict(prior))
            key = (build["repository"], build["environment"], request.branch_name)
            session.execute(
                pg_insert(DeploymentTarget)
                .values(repository=key[0], environment=key[1], branch_name=key[2])
                .on_conflict_do_nothing()
            )
            current = session.get(DeploymentTarget, key, with_for_update=True)
            if current.version != request.expected_target_version:
                raise StoreConflict("deployment target version changed")
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
            return cast(StoredDeploymentTarget | None, entity_dict(target))

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
                return cast(StoredDeploymentAttempt | None, entity_dict(attempt))
            if target.desired_generation != generation:
                status, reason = STALE, NEWER_INTENT
            if status == DEPLOYED:
                build, lifecycle = session.execute(
                    select(Build, RuntimeJob.run_lifecycle)
                    .join(RuntimeJob, Build.run_id == RuntimeJob.job_id)
                    .where(Build.build_id == attempt.build_id)
                    .with_for_update(of=(RuntimeJob, Build))
                ).one()
                # 与人工清理锁同一执行记录；head 只比较事务前已完成的观察值。
                if (
                    target.version != expected_version
                    or build.build_status != SUCCEEDED
                    or lifecycle != ACTIVE
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
            return cast(StoredDeploymentAttempt, entity_dict(attempt))

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
            rows = session.scalars(statement, execution_options={"synchronize_session": False})
            return cast(list[StoredDeploymentAttempt], [entity_dict(row) for row in rows])

    def list(self, repository: str) -> list[StoredDeploymentAttempt]:
        with self.db.session() as session:
            rows = session.scalars(
                select(DeploymentAttempt)
                .where(DeploymentAttempt.repository == repository)
                .order_by(DeploymentAttempt.created_at.desc(), DeploymentAttempt.generation.desc())
            )
            return cast(list[StoredDeploymentAttempt], [entity_dict(row) for row in rows])


def natural(key: DeploymentKey) -> tuple[str, str, str]:
    return key.repository, key.environment, key.branch_name


def attempt_view(row: StoredDeploymentAttempt) -> DeploymentAttemptView:
    # 显式白名单防止内部幂等来源和请求摘要泄漏到 API。
    from ..models.deployments import DeploymentAttemptView

    return DeploymentAttemptView(**{key: row[key] for key in DeploymentAttemptView.model_fields})
