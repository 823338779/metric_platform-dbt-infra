"""固定构建的同步选项与查询；执行记录保留版本保护、幂等和结果分页。"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import cast
from uuid import UUID, uuid4

from dbt_metricflow_service.application.builds import BuildService
from dbt_metricflow_service.application.catalog import CatalogService
from dbt_metricflow_service.models.payloads import AvailableDimensions, JsonObject, QueryOption
from dbt_metricflow_service.models.queries import AvailableDimensionsRequest, QueryRequest
from dbt_metricflow_service.storage.jobs import JobStore
from dbt_metricflow_service.storage.records import StoredBuild, StoredJob

from ..models.builds import BuildStatus, ErrorView
from ..models.queries import AvailableDimensionsView, QueryView, ResultPage
from ..platform.options import map_options
from ..platform.query_time import canonical_query, normalize_query_time, query_time_metadata
from ..platform.results import result_page
from ..storage.builds import digest
from ..storage.jobs import CleanupBlocked, StoreConflict
from .errors import ServiceError
from .query_plan import compile_query

OPTIONS = "QUERY_OPTIONS"
QUERY = "METRIC_QUERY"
JSON_MODE = "json"
OPTIONS_CALLER = "query-options"
NO_RETRY = "NEVER"
TIMEOUT_CODES = frozenset({"COMMAND_TIMEOUT", "TASK_TIMEOUT"})
INVALID_QUERY = "INVALID_QUERY"


class QueryService:
    def __init__(
        self, builds: BuildService, catalogs: CatalogService, jobs: JobStore, execute: Callable[[str], None],
    ) -> None:
        # 执行回调在当前请求内完成；持久记录仅用于租约保护及结果重读。
        self.builds = builds
        self.catalogs = catalogs
        self.jobs = jobs
        # 指定记录必须完成后返回，不能交给后台队列后立即返回。
        self.execute = execute

    def _scope(self, build: StoredBuild, caller: str, kind: str) -> str:
        return json.dumps([build["repository"], caller, kind], separators=(",", ":"))

    def _available(self, build: StoredBuild) -> None:
        code = self.builds.query_unavailable_reason(build)
        if code:
            status = 410 if code == "PHYSICAL_OBJECTS_REMOVED" else 409 if code == "BUILD_NOT_SUCCEEDED" else 503
            raise ServiceError(code, "build is not available for new engine tasks", status, build_id=build["build_id"])

    def _prior(self, build: StoredBuild, body: JsonObject, caller: str, kind: str) -> tuple[StoredJob | None, str]:
        fingerprint = digest({"buildId": build["build_id"], **body})
        row = self.jobs.by_key(self._scope(build, caller, kind), body["idempotencyKey"])
        if row and row["request_json"].get("publicDigest") != fingerprint:
            raise ServiceError("IDEMPOTENCY_CONFLICT", "task key already binds another request", 409)
        return row, fingerprint

    def _reserve(self, build: StoredBuild, body: JsonObject, caller: str, kind: str, payload: JsonObject) -> StoredJob:
        # JobStore 同时锁父构建并核对 lifecycle，引用取得与清理互斥。
        # 成功且可查询的构建具有持久父任务，查询受理不会删除这条历史记录。
        parent = cast(StoredJob, self.jobs.get(cast(str, build["run_id"])))
        try:
            return self.jobs.reserve(
                kind,
                parent["project_id"],
                payload,
                idempotency_scope=self._scope(build, caller, kind),
                idempotency_key=body["idempotencyKey"],
                parent_run_id=build["run_id"],
                input_set_id=build["output_set_id"],
                config_version=build["config_version"],
                toolchain_version=build["toolchain_version"],
                profile_binding_id=parent["profile_binding_id"],
                schema_name=parent["schema_name"],
                timeout_seconds=self.builds.timeout,
                retry_policy=NO_RETRY,
                max_attempts=1,
            )
        except StoreConflict as error:
            raise ServiceError("IDEMPOTENCY_CONFLICT", "task key already binds another request", 409) from error
        except CleanupBlocked as error:
            raise ServiceError("PHYSICAL_OBJECTS_REMOVED", "build is being cleaned", 410) from error

    def available_dimensions(
        self, build_id: UUID | str, request: AvailableDimensionsRequest, caller: str,
    ) -> AvailableDimensionsView:
        build = self.builds.load(build_id)
        self._available(build)
        catalog = self.catalogs.read_build(build)
        selected = sorted(set(request.metric_resource_ids))
        result, _ = self._options(build, selected, catalog, caller)
        return AvailableDimensionsView.model_validate(result)

    def _complete(self, row: StoredJob, build: StoredBuild) -> None:
        # 重复幂等请求等待同一执行完成，HTTP 不暴露排队或运行中状态。
        # 终态允许跨工具链重读；未完成记录不能在不兼容实例空等至超时。
        if row["status"] not in {BuildStatus.SUCCEEDED, BuildStatus.FAILED}:
            self._available(build)
        try:
            self.execute(row["job_id"])
        except TimeoutError as error:
            raise ServiceError("COMMAND_TIMEOUT", "query execution timed out", 504,
                               build_id=build["build_id"]) from error
        completed = self.jobs.get(row["job_id"])
        if completed is None:
            raise ServiceError("TASK_NOT_FOUND", "query execution record is unavailable", 404)
        if completed["status"] != BuildStatus.SUCCEEDED:
            code = completed["error_code"] or "EXECUTION_OUTCOME_UNKNOWN"
            status = 422 if code == INVALID_QUERY else 504 if code in TIMEOUT_CODES else 503
            raise ServiceError(code, "query execution did not complete", status,
                               phase=completed["phase"], build_id=build["build_id"])

    @staticmethod
    def _metrics(catalog: JsonObject, selected: list[str]) -> dict[str, JsonObject]:
        indexed = {item["resourceId"]: item for item in catalog["resources"]}
        if any(
            key not in indexed or indexed[key]["kind"] != "METRIC" or "QUERY" not in indexed[key]["capabilities"]
            for key in selected
        ):
            raise ServiceError("INVALID_METRIC_RESOURCE", "selected metric does not belong to this build")
        return indexed

    def _task(self, task_id: UUID | str, kind: str) -> tuple[StoredJob, StoredBuild]:
        row = self.jobs.get(str(task_id))
        if not row or row["kind"] != kind or not row["request_json"].get("buildId"):
            raise ServiceError("TASK_NOT_FOUND", "engine task does not exist", 404)
        build = self.builds.load(row["request_json"]["buildId"])
        if row["parent_run_id"] != build["run_id"]:
            raise ServiceError("TASK_NOT_FOUND", "task does not belong to this build", 404)
        return row, build

    @staticmethod
    def _state(row: StoredJob) -> BuildStatus:
        if row["error_code"] == "CANCELLED":
            return BuildStatus.CANCELLED
        if row["error_code"] == "EXECUTION_OUTCOME_UNKNOWN" or (row["error_detail"] or {}).get(
            "externalOutcomeUnknown"
        ):
            return BuildStatus.OUTCOME_UNKNOWN
        return BuildStatus(row["status"])

    @staticmethod
    def _error(row: StoredJob, build: StoredBuild) -> ErrorView | None:
        return (
            ErrorView.model_validate(
                {
                    "code": row["error_code"],
                    "message": "engine task failed",
                    "phase": row["phase"],
                    "build_id": build["build_id"],
                }
            )
            if row["error_code"]
            else None
        )

    def _options(
        self, build: StoredBuild, selected: list[str], catalog: JsonObject, caller: str = OPTIONS_CALLER,
    ) -> tuple[AvailableDimensions, dict[str, str]]:
        # 复用同一构建和指标集合的完整结果；首次查询也能当场计算，不要求预提交。
        indexed = self._metrics(catalog, selected)
        rows = self.jobs.options_for(cast(str, build["run_id"]))
        for row in rows:
            if row["request_json"].get("publicRequest", {}).get("metricResourceIds") == selected:
                result = self.jobs.result(row["job_id"])
                if result:
                    return map_options(build["build_id"], selected, catalog, result["payload_json"])
        body = {"metricResourceIds": selected, "idempotencyKey": uuid4().hex}
        payload = {
            "buildId": build["build_id"], "publicRequest": body, "mode": "OPTIONS",
            "metrics": [indexed[key]["name"] for key in selected],
        }
        row = self._reserve(build, body, caller, OPTIONS, payload)
        self._complete(row, build)
        result = self.jobs.result(row["job_id"])
        if not result:
            raise ServiceError("RESULT_REMOVED", "options result was removed", 410)
        return map_options(build["build_id"], selected, catalog, result["payload_json"])

    def submit(self, build_id: UUID | str, request: QueryRequest, caller: str) -> ResultPage:
        build = self.builds.load(build_id)
        timezone = build["config_snapshot"]["businessTimezone"]
        request = normalize_query_time(request, timezone)
        body = canonical_query(request)
        prior, fingerprint = self._prior(build, body, caller, QUERY)
        if prior:
            self._complete(prior, build)
            return self.results(prior["job_id"])
        self._available(build)
        catalog = self.catalogs.read_build(build)
        indexed = self._metrics(catalog, body["metricResourceIds"])
        options: list[QueryOption] = []
        mapping: dict[str, str] = {}
        if request.group_by or request.filters or request.dimension_option_id:
            available, mapping = self._options(build, body["metricResourceIds"], catalog)
            options = available["dimensions"]
        engine = compile_query(request, build["run_id"], indexed, options, mapping)
        payload = {
            "buildId": str(build_id),
            "publicRequest": body,
            "publicDigest": fingerprint,
            "engineRequest": engine.model_dump(mode=JSON_MODE, by_alias=True),
            **query_time_metadata(body, timezone),
        }
        row = self._reserve(build, body, caller, QUERY, payload)
        self._complete(row, build)
        return self.results(row["job_id"])

    def status(self, query_id: UUID | str) -> QueryView:
        row, build = self._task(query_id, QUERY)
        body = row["request_json"]
        # 状态路径仅读任务元数据，不加载结果正文。
        return QueryView.model_validate(
            {
                "query_id": row["job_id"],
                "build_id": build["build_id"],
                "state": self._state(row),
                "mode": body["publicRequest"]["mode"],
                "commit_sha": build["commit_sha"],
                "business_timezone": body["businessTimezone"],
                "normalized_time_range": body["normalizedTimeRange"],
                "result_available": row["status"] == "SUCCEEDED" and self.jobs.result_exists(row["job_id"]),
                "error": self._error(row, build),
            }
        )

    def results(self, query_id: UUID | str, offset: int=0, limit: int=100) -> ResultPage:
        if offset < 0 or not 1 <= limit <= 200:
            raise ServiceError("INVALID_PAGE", "offset must be nonnegative and limit between 1 and 200")
        view = self.status(query_id)
        if view.state != BuildStatus.SUCCEEDED:
            raise ServiceError("RESULT_NOT_READY", "query has not succeeded", 409)
        result = self.jobs.result(str(query_id))
        if result is None:
            raise ServiceError("RESULT_REMOVED", "query result was removed", 410)
        try:
            return ResultPage.model_validate(
                result_page(result["payload_json"], view.model_dump(mode=JSON_MODE, by_alias=True), offset, limit)
            )
        except ValueError as error:
            raise ServiceError("RESULT_TOO_LARGE", "one row or result metadata exceeds the page limit") from error
