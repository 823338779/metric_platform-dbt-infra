"""构建产物的部署用例；只核对来源、可用性和意图顺序。"""

from __future__ import annotations

from collections.abc import Callable
from typing import Final, cast

from dbt_metricflow_service import constants as strings
from dbt_metricflow_service.application.builds import BuildService
from dbt_metricflow_service.application.catalog import CatalogService
from dbt_metricflow_service.models.deployments import DeploymentRequest
from dbt_metricflow_service.storage.deployments import DeploymentStore
from dbt_metricflow_service.storage.records import StoredBuild, StoredDeploymentAttempt, StoredDeploymentTarget

from ..models.builds import Page
from ..models.deployments import (
    DeploymentAttemptView,
    DeploymentKey,
    DeploymentTargetView,
)
from ..storage.builds import digest
from ..storage.deployments import TERMINAL, attempt_view
from ..storage.jobs import StoreConflict
from .errors import ServiceError

DEPLOYMENT_INPUT_CONFLICT_MESSAGE: Final = "deployment input differs"
DEPLOYMENT_SOURCE_CONFLICT_MESSAGE: Final = "deployment branch differs from build source"
MISSING_BUILD_MESSAGE: Final = strings.BUILD_NOT_FOUND_MESSAGE
MISSING_DEPLOYMENT_INTENT_MESSAGE: Final = "deployment intent does not exist"
REQUIRED_SUCCESSFUL_BUILD_MESSAGE: Final = "a successful build with source evidence is required"

JSON_MODE = strings.JSON_MODE


class DeploymentService:
    def __init__(
        self,
        store: DeploymentStore,
        head_reader: Callable[[str, str], str | None],
        ancestry_reader: Callable[[str, str, str], bool] | None = None,
        *,
        builds: BuildService,
        catalogs: CatalogService,
    ) -> None:
        # 只读 Git 函数是来源证明，不涉及调用方的分支管理/合并流程。
        self.store = store
        self.builds = builds.store
        self.build_service = builds
        self.catalogs = catalogs
        self.head_reader = head_reader
        self.ancestry_reader = ancestry_reader

    def submit(self, request: DeploymentRequest, caller: str) -> DeploymentAttemptView:
        build = self.builds.get(str(request.build_id))
        if not build:
            raise ServiceError(strings.BUILD_NOT_FOUND, MISSING_BUILD_MESSAGE, 404)
        fingerprint = digest(request.model_dump(mode=JSON_MODE, by_alias=True))
        prior = self.store.by_key(build.repository, caller, request.idempotency_key)
        if prior:
            if prior.request_digest != fingerprint:
                raise ServiceError(strings.IDEMPOTENCY_CONFLICT, DEPLOYMENT_INPUT_CONFLICT_MESSAGE, 409)
            return attempt_view(prior)
        if build.build_status != strings.SUCCEEDED:
            raise ServiceError(strings.BUILD_NOT_READY, REQUIRED_SUCCESSFUL_BUILD_MESSAGE, 409)
        if build.branch_name and request.branch_name != build.branch_name:
            raise ServiceError(strings.SOURCE_MISMATCH, DEPLOYMENT_SOURCE_CONFLICT_MESSAGE)
        try:
            return attempt_view(self.store.submit(build, caller, request, fingerprint))
        except StoreConflict as error:
            raise ServiceError(strings.TARGET_CONFLICT, str(error), 409) from error

    def current(self, key: DeploymentKey) -> DeploymentTargetView:
        model = DeploymentTargetView
        target = self.store.current(key)
        if target is None:
            return model(**key.model_dump())
        active = self.builds.get(target.active_build_id) if target.active_build_id else None
        try:
            head = self.head_reader(key.repository, key.branch_name)
            state = strings.MISSING if head is None else strings.UNDEPLOYED
            if active and head:
                state = strings.MATCHED if head == active.commit_sha else strings.UNKNOWN
                if state == strings.UNKNOWN and self.ancestry_reader:
                    state = (
                        strings.AHEAD
                        if self.ancestry_reader(key.repository, cast(str, active.commit_sha), head)
                        else strings.DIVERGED
                    )
        except (ValueError, OSError):
            head, state = None, strings.UNKNOWN
        observed = self.store.observe(
            key, head, state, expected_version=target.version, expected_active_build_id=target.active_build_id
        )
        # 远端读取期间可能切换部署；旧观察不写入新版本，返回最新的完整已知快照。
        # 已受理目标没有删除路径；并发观察最多改变其版本和指针。
        target = cast(StoredDeploymentTarget, observed or self.store.current(key))
        active = self.builds.get(target.active_build_id) if target.active_build_id else None
        latest = self.store.attempt(key, target.desired_generation)
        return model(
            repository=target.repository, environment=target.environment, branch_name=target.branch_name,
            version=target.version, desired_generation=target.desired_generation,
            active_build_id=target.active_build_id,
            active_commit_sha=active.commit_sha if active and target.active_build_id else None,
            latest_attempt=attempt_view(latest) if latest else None,
            observed_head_sha=target.observed_head_sha, head_observed_at=target.head_observed_at,
            source_state=target.source_state,
        )

    def reconcile(self, key: DeploymentKey, generation: int) -> DeploymentAttemptView:
        attempt = self.store.attempt(key, generation)
        if not attempt:
            raise ServiceError(strings.DEPLOYMENT_NOT_FOUND, MISSING_DEPLOYMENT_INTENT_MESSAGE, 404)
        if attempt.deployment_status in TERMINAL:
            return attempt_view(attempt)
        # 部署记录的外键保证引用构建存在；构建历史不会被物理清理删除。
        build = cast(StoredBuild, self.builds.get(attempt.build_id))
        status = build.build_status
        if status in {strings.FAILED, strings.CANCELLED}:
            return attempt_view(
                cast(StoredDeploymentAttempt, self.store.settle(key, generation, status, strings.BUILD_ + status))
            )
        if status != strings.SUCCEEDED:
            return attempt_view(attempt)
        # 物理清理或历史工具链不可用不能通过独立部署入口绕过。
        reason = self.build_service.query_unavailable_reason(build)
        if reason:
            return attempt_view(
                cast(
                    StoredDeploymentAttempt, self.store.settle(key, generation, strings.FAILED, reason)
                )
            )
        try:
            self.catalogs.read_build(build)
        except ServiceError as error:
            return attempt_view(
                cast(StoredDeploymentAttempt, self.store.settle(key, generation, strings.FAILED, error.error.code))
            )
        current = self.current(key)
        if current.source_state == strings.UNKNOWN:
            return attempt_view(attempt)
        if current.observed_head_sha != build.commit_sha:
            return attempt_view(
                cast(
                    StoredDeploymentAttempt,
                    self.store.settle(key, generation, strings.STALE, strings.SOURCE_HEAD_CHANGED),
                )
            )
        return attempt_view(
            cast(StoredDeploymentAttempt, self.store.settle(
                key, generation, strings.DEPLOYED, expected_version=current.version, head=current.observed_head_sha
            ))
        )

    def reconcile_pending(self) -> None:
        # 调度由 runtime 调用；每次有界扫描，重启后仍读取同一持久意图。
        for attempt in self.store.pending():
            model = DeploymentKey
            key = model(repository=attempt.repository, environment=attempt.environment, branch_name=attempt.branch_name)
            self.reconcile(key, attempt.generation)

    def list(
        self,
        repository: str,
        environment: str | None = None,
        branch_name: str | None = None,
        cursor: str | None = None,
        limit: int = 50,
    ) -> Page[DeploymentAttemptView]:
        try:
            rows, next_cursor = self.store.list(repository, environment, branch_name, cursor, limit)
        except ValueError as error:
            raise ServiceError(strings.INVALID_CURSOR, str(error)) from error
        return Page[DeploymentAttemptView](items=[attempt_view(row) for row in rows], next_cursor=next_cursor)
