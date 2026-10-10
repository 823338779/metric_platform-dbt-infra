"""固定构建的异步选项与查询；不重新解析当前部署或执行外部进程。"""

import json

from ..models.builds import BuildStatus, ErrorView
from ..models.queries import OptionsTaskView, QueryMode, QueryView, ResultPage
from ..platform.models import PlatformQueryRequest
from ..platform.options import map_options
from ..platform.query_time import DEFAULT_TIMEZONE, canonical_query, normalize_query_time, query_time_metadata
from ..platform.results import result_page
from ..storage.builds import digest
from ..storage.jobs import CleanupBlocked, StoreConflict
from .errors import ServiceError

OPTIONS = "QUERY_OPTIONS"
QUERY = "METRIC_QUERY"
JSON_MODE = "json"
MAX_PAGE_BYTES = 8 * 1024 * 1024


class QueryService:
    def __init__(self, builds, catalogs, jobs):
        # 注入已有具体能力；任务执行由持久队列和 worker 承担。
        self.builds = builds
        self.catalogs = catalogs
        self.jobs = jobs

    def _scope(self, build, caller, kind):
        return json.dumps([build["repository"], caller, kind], separators=(",", ":"))

    def _build(self, build_id):
        row = self.builds.store.get(str(build_id))
        if not row:
            raise ServiceError("BUILD_NOT_FOUND", "build does not exist", 404)
        return row

    def _available(self, build_id):
        view = self.builds.get(build_id)
        if not view.query_available:
            code = view.query_unavailable_reason
            status = 410 if code == "PHYSICAL_OBJECTS_REMOVED" else 409 if code == "BUILD_NOT_SUCCEEDED" else 503
            raise ServiceError(code, "build is not available for new engine tasks", status, build_id=str(build_id))

    def _prior(self, build, body, caller, kind):
        fingerprint = digest({"buildId": build["build_id"], **body})
        row = self.jobs.by_key(self._scope(build, caller, kind), body["idempotencyKey"])
        if row and row["request_json"].get("publicDigest") != fingerprint:
            raise ServiceError("IDEMPOTENCY_CONFLICT", "task key already binds another request", 409)
        return row, fingerprint

    def _reserve(self, build, body, caller, kind, payload):
        # JobStore 同时锁父构建并核对 lifecycle，引用取得与清理互斥。
        parent = self.jobs.get(build["run_id"])
        retry = "READ_ONLY" if build["config_snapshot"].get("queryRetrySafe") else "PREPARATION_ONLY"
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
                retry_policy=retry,
            )
        except StoreConflict as error:
            raise ServiceError("IDEMPOTENCY_CONFLICT", "task key already binds another request", 409) from error
        except CleanupBlocked as error:
            raise ServiceError("PHYSICAL_OBJECTS_REMOVED", "build is being cleaned", 410) from error

    def submit_options(self, build_id, request, caller):
        build = self._build(build_id)
        body = request.model_dump(mode=JSON_MODE, by_alias=True)
        body["metricResourceIds"] = sorted(set(request.metric_resource_ids))
        prior, fingerprint = self._prior(build, body, caller, OPTIONS)
        if prior:
            return self.get_options(prior["job_id"])
        self._available(build_id)
        _, catalog = self.catalogs.read(build_id)
        indexed = self._metrics(catalog, body["metricResourceIds"])
        payload = {
            "buildId": str(build_id),
            "publicRequest": body,
            "publicDigest": fingerprint,
            "mode": "OPTIONS",
            "metrics": [indexed[key]["name"] for key in body["metricResourceIds"]],
        }
        row = self._reserve(build, body, caller, OPTIONS, payload)
        return self.get_options(row["job_id"])

    @staticmethod
    def _metrics(catalog, selected):
        indexed = {item["resourceId"]: item for item in catalog["resources"]}
        if any(
            key not in indexed or indexed[key]["kind"] != "METRIC" or "QUERY" not in indexed[key]["capabilities"]
            for key in selected
        ):
            raise ServiceError("INVALID_METRIC_RESOURCE", "selected metric does not belong to this build")
        return indexed

    def _task(self, task_id, kind):
        row = self.jobs.get(str(task_id))
        if not row or row["kind"] != kind or not row["request_json"].get("buildId"):
            raise ServiceError("TASK_NOT_FOUND", "engine task does not exist", 404)
        build = self._build(row["request_json"]["buildId"])
        if row["parent_run_id"] != build["run_id"]:
            raise ServiceError("TASK_NOT_FOUND", "task does not belong to this build", 404)
        return row, build

    @staticmethod
    def _state(row):
        if row["error_code"] == "CANCELLED":
            return BuildStatus.CANCELLED
        if row["error_code"] == "EXECUTION_OUTCOME_UNKNOWN" or (row["error_detail"] or {}).get(
            "externalOutcomeUnknown"
        ):
            return BuildStatus.OUTCOME_UNKNOWN
        return BuildStatus(row["status"])

    @staticmethod
    def _error(row, build):
        return (
            ErrorView(
                code=row["error_code"], message="engine task failed", phase=row["phase"], build_id=build["build_id"]
            )
            if row["error_code"]
            else None
        )

    def get_options(self, options_task_id):
        row, build = self._task(options_task_id, OPTIONS)
        metrics = row["request_json"]["publicRequest"]["metricResourceIds"]
        options = None
        if row["status"] == "SUCCEEDED":
            _, catalog = self.catalogs.read(build["build_id"])
            result = self.jobs.result(row["job_id"])
            if not result:
                raise ServiceError("RESULT_REMOVED", "options result was removed", 410)
            options = map_options(build["build_id"], metrics, catalog, result["payload_json"])[0]["options"]
        return OptionsTaskView(
            options_task_id=row["job_id"],
            build_id=build["build_id"],
            state=self._state(row),
            metric_resource_ids=metrics,
            options=options,
            error=self._error(row, build),
        )

    def _options(self, build, selected, catalog):
        # 查询消费已完成的选项，不在应用层同步等待或重新运行引擎。
        rows = self.jobs.options_for(build["run_id"])
        for row in rows:
            if row["request_json"].get("publicRequest", {}).get("metricResourceIds") == selected:
                result = self.jobs.result(row["job_id"])
                if result:
                    return map_options(build["build_id"], selected, catalog, result["payload_json"])
        raise ServiceError("OPTIONS_NOT_READY", "obtain query options for this build and metric set first", 409)

    def submit(self, build_id, request, caller):
        build = self._build(build_id)
        timezone = build["config_snapshot"].get("businessTimezone", DEFAULT_TIMEZONE)
        request = normalize_query_time(request, timezone)
        body = canonical_query(request, timezone)
        prior, fingerprint = self._prior(build, body, caller, QUERY)
        if prior:
            return self.status(prior["job_id"])
        self._available(build_id)
        _, catalog = self.catalogs.read(build_id)
        indexed = self._metrics(catalog, body["metricResourceIds"])
        if request.start_time and request.end_time and request.start_time > request.end_time:
            raise ServiceError("INVALID_TIME_RANGE", "startTime must not exceed endTime")
        if request.mode == QueryMode.PREVIEW:
            resource = indexed.get(request.dataset_resource_id)
            if not resource or "PREVIEW" not in resource["capabilities"]:
                raise ServiceError("INVALID_PREVIEW_RESOURCE", "resource cannot be previewed")
        options, mapping = {"options": []}, {}
        if request.group_by or request.filters or request.dimension_option_id:
            options, mapping = self._options(build, body["metricResourceIds"], catalog)
        option_index = {item["optionId"]: item for item in options["options"]}
        groups, filters = [], []
        for selected in request.group_by:
            option = option_index.get(selected.option_id)
            if not option or selected.grain and selected.grain not in option["granularities"]:
                raise ServiceError("INVALID_DIMENSION_OPTION", "option or grain belongs to another selection")
            groups.append(mapping[selected.option_id])
        for selected in request.filters:
            option = option_index.get(selected.option_id)
            if not option or selected.operator not in option["operators"]:
                raise ServiceError("INVALID_FILTER_OPTION", "filter does not belong to this selection")
            filters.append(
                {"field": mapping[selected.option_id], "operator": selected.operator, "value": selected.value}
            )
        if request.dimension_option_id and request.dimension_option_id not in mapping:
            raise ServiceError("INVALID_DIMENSION_OPTION", "dimension does not belong to this selection")
        order_fields = {key: indexed[key]["name"] for key in body["metricResourceIds"]}
        order_fields.update({selection.option_id: mapping[selection.option_id] for selection in request.group_by})
        if any(order.field_id not in order_fields for order in request.order_by):
            raise ServiceError("INVALID_ORDER_FIELD", "order field was not selected")
        engine = PlatformQueryRequest(
            run_id=build["run_id"],
            idempotency_key=request.idempotency_key,
            mode=request.mode,
            metrics=[indexed[key]["name"] for key in body["metricResourceIds"]],
            group_by=groups,
            filters=filters,
            start_time=request.start_time,
            end_time=request.end_time,
            limit=request.limit,
            dataset_resource_id=request.dataset_resource_id,
            dimension=mapping.get(request.dimension_option_id),
            order_by=[
                ("-" if order.direction == "DESC" else "") + order_fields[order.field_id] for order in request.order_by
            ],
        )
        payload = {
            "buildId": str(build_id),
            "publicRequest": body,
            "publicDigest": fingerprint,
            "engineRequest": engine.model_dump(mode=JSON_MODE, by_alias=True),
            **query_time_metadata(request, timezone),
        }
        return self.status(self._reserve(build, body, caller, QUERY, payload)["job_id"])

    def status(self, query_id):
        row, build = self._task(query_id, QUERY)
        body = row["request_json"]
        # 状态路径仅读任务元数据，不加载结果正文。
        return QueryView(
            query_id=row["job_id"],
            build_id=build["build_id"],
            state=self._state(row),
            mode=body["publicRequest"]["mode"],
            commit_sha=build["commit_sha"],
            business_timezone=body.get(
                "businessTimezone", build["config_snapshot"].get("businessTimezone", DEFAULT_TIMEZONE)
            ),
            normalized_time_range=body["normalizedTimeRange"],
            result_available=row["status"] == "SUCCEEDED" and self.jobs.result_exists(row["job_id"]),
            error=self._error(row, build),
        )

    def results(self, query_id, offset=0, limit=100):
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
