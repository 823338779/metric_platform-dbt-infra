"""构建产物的部署用例；只核对来源、可用性和意图顺序。"""

from __future__ import annotations

from collections.abc import Callable
from typing import cast

from dbt_metricflow_service.application.builds import BuildService
from dbt_metricflow_service.application.catalog import CatalogService
from dbt_metricflow_service.models.builds import Environment
from dbt_metricflow_service.models.deployments import DeploymentRequest
from dbt_metricflow_service.storage.deployments import DeploymentStore
from dbt_metricflow_service.storage.records import StoredBuild, StoredDeploymentAttempt, StoredDeploymentTarget

from ..models.builds import Page
from ..models.deployments import DeploymentAttemptView, DeploymentKey, DeploymentTargetView
from ..storage.builds import digest
from ..storage.deployments import TERMINAL, attempt_view
from ..storage.jobs import StoreConflict
from ..storage.paging import page
from .errors import ServiceError

JSON_MODE = "json"


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
            raise ServiceError("BUILD_NOT_FOUND", "build does not exist", 404)
        fingerprint = digest(request.model_dump(mode=JSON_MODE, by_alias=True))
        prior = self.store.by_key(build["repository"], caller, request.idempotency_key)
        if prior:
            if prior["request_digest"] != fingerprint:
                raise ServiceError("IDEMPOTENCY_CONFLICT", "deployment input differs", 409)
            return attempt_view(prior)
        if build["build_status"] != "SUCCEEDED":
            raise ServiceError("BUILD_NOT_READY", "a successful build with source evidence is required", 409)
        if build["branch_name"] and request.branch_name != build["branch_name"]:
            raise ServiceError("SOURCE_MISMATCH", "deployment branch differs from build source")
        if (build["environment"] == "PRODUCTION") != (request.branch_name == "main"):
            raise ServiceError("INVALID_TARGET", "branch and environment do not match")
        try:
            return attempt_view(self.store.submit(build, caller, request, fingerprint))
        except StoreConflict as error:
            raise ServiceError("TARGET_CONFLICT", str(error), 409) from error

    def current(self, key: DeploymentKey) -> DeploymentTargetView:
        target = self.store.current(key)
        if target is None:
            return DeploymentTargetView(**key.model_dump())
        active = self.builds.get(target["active_build_id"]) if target["active_build_id"] else None
        try:
            head = self.head_reader(key.repository, key.branch_name)
            state = "MISSING" if head is None else "UNDEPLOYED"
            if active and head:
                state = "MATCHED" if head == active["commit_sha"] else "UNKNOWN"
                if state == "UNKNOWN" and self.ancestry_reader:
                    state = (
                        "AHEAD"
                        if self.ancestry_reader(key.repository, cast(str, active["commit_sha"]), head)
                        else "DIVERGED"
                    )
        except (ValueError, OSError):
            head, state = None, "UNKNOWN"
        observed = self.store.observe(
            key, head, state, expected_version=target["version"], expected_active_build_id=target["active_build_id"]
        )
        # 远端读取期间可能切换部署；旧观察不写入新版本，返回最新的完整已知快照。
        # 已受理目标没有删除路径；并发观察最多改变其版本和指针。
        target = cast(StoredDeploymentTarget, observed or self.store.current(key))
        active = self.builds.get(target["active_build_id"]) if target["active_build_id"] else None
        fields = {name: target[name] for name in DeploymentTargetView.model_fields if name in target}
        fields["active_commit_sha"] = active["commit_sha"] if active and target["active_build_id"] else None
        latest = self.store.attempt(key, target["desired_generation"])
        fields["latest_attempt"] = attempt_view(latest) if latest else None
        return DeploymentTargetView(**fields)

    def reconcile(self, key: DeploymentKey, generation: int) -> DeploymentAttemptView:
        attempt = self.store.attempt(key, generation)
        if not attempt:
            raise ServiceError("DEPLOYMENT_NOT_FOUND", "deployment intent does not exist", 404)
        if attempt["deployment_status"] in TERMINAL:
            return attempt_view(attempt)
        # 部署记录的外键保证引用构建存在；构建历史不会被物理清理删除。
        build = cast(StoredBuild, self.builds.get(attempt["build_id"]))
        status = build["build_status"]
        if status in {"FAILED", "CANCELLED"}:
            return attempt_view(
                cast(StoredDeploymentAttempt, self.store.settle(key, generation, status, "BUILD_" + status))
            )
        if status != "SUCCEEDED":
            return attempt_view(attempt)
        # 物理清理或历史工具链不可用不能通过独立部署入口绕过。
        view = self.build_service.get(build["build_id"])
        if not view.query_available:
            return attempt_view(
                cast(
                    StoredDeploymentAttempt, self.store.settle(key, generation, "FAILED", view.query_unavailable_reason)
                )
            )
        try:
            self.catalogs.read(build["build_id"])
        except ServiceError as error:
            return attempt_view(
                cast(StoredDeploymentAttempt, self.store.settle(key, generation, "FAILED", error.error.code))
            )
        current = self.current(key)
        if current.source_state == "UNKNOWN":
            return attempt_view(attempt)
        if current.observed_head_sha != build["commit_sha"]:
            return attempt_view(
                cast(StoredDeploymentAttempt, self.store.settle(key, generation, "STALE", "SOURCE_HEAD_CHANGED"))
            )
        # 正式目标不能向现有版本的祖先或分叉切换；无法证明则等待核实。
        if current.active_commit_sha and current.active_commit_sha != build["commit_sha"]:
            if self.ancestry_reader is None:
                return attempt_view(attempt)
            try:
                forward = self.ancestry_reader(
                    key.repository, current.active_commit_sha, cast(str, build["commit_sha"])
                )
            except (ValueError, OSError):
                return attempt_view(attempt)
            if not forward and key.environment == "PRODUCTION":
                return attempt_view(
                    cast(StoredDeploymentAttempt, self.store.settle(key, generation, "STALE", "NON_FORWARD_SOURCE"))
                )
        return attempt_view(
            cast(StoredDeploymentAttempt, self.store.settle(
                key, generation, "DEPLOYED", expected_version=current.version, head=current.observed_head_sha
            ))
        )

    def reconcile_pending(self) -> None:
        # 调度由 runtime 调用；每次有界扫描，重启后仍读取同一持久意图。
        for attempt in self.store.pending():
            key = DeploymentKey(**{name: attempt[name] for name in DeploymentKey.model_fields})
            self.reconcile(key, attempt["generation"])

    def list(
        self,
        repository: str,
        environment: Environment | None = None,
        branch_name: str | None = None,
        cursor: str | None = None,
        limit: int = 50,
    ) -> Page[DeploymentAttemptView]:
        rows = [
            row
            for row in self.store.list(repository)
            if (environment is None or row["environment"] == environment)
            and (branch_name is None or row["branch_name"] == branch_name)
        ]
        try:
            rows, next_cursor = page(
                rows,
                scope=[repository, environment, branch_name],
                cursor=cursor,
                limit=limit,
                reverse=True,
                identity=lambda row: str(row["created_at"]) + json_identity(row),
            )
        except ValueError as error:
            raise ServiceError("INVALID_CURSOR", str(error)) from error
        return Page[DeploymentAttemptView](items=[attempt_view(row) for row in rows], next_cursor=next_cursor)


def json_identity(row: StoredDeploymentAttempt) -> str:
    import json

    return json.dumps([row["repository"], row["environment"], row["branch_name"], row["generation"]])
