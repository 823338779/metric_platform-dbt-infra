"""服务拥有发布输入与查询映射。"""
import hashlib
import json

from ..platform.models import PlatformQueryRequest, QueryMode
from ..storage.jobs import StoreConflict
from ..storage.queries import InactiveRelease, QueryStore
from .catalog_service import CatalogService
from .errors import PublicationError
from .models import PublishedQueryRequest, QueryOptionsRequest, ResourceKind
from .query_time import DEFAULT_TIMEZONE, canonical_query, normalize_query_time, query_time_metadata
from .service import PublicationService, ReleaseGone, invalid_selection, query_receipt

UTF8 = "utf-8"
PRODUCTION = "PRODUCTION"
QUERY_KIND = "METRIC_QUERY"
QUERY_SCOPE = "PUBLISHED_QUERY:"
JSON_MODE = "json"
DESCENDING = "DESC"
GRAIN_LABELS = {"second": "秒", "minute": "分钟", "hour": "小时", "day": "日", "week": "周",
                "month": "月", "quarter": "季度", "year": "年"}
PATH_SEPARATOR = "__"
DISPLAY_SEPARATOR = " → "


class QueryService:
    def __init__(self, runtime):
        self.runtime = runtime
        self.publications = PublicationService(runtime)
        self.catalogs = CatalogService(runtime)
        self.store = QueryStore(runtime.jobs)

    def _options(self, project_id: str, request: QueryOptionsRequest) -> tuple[dict, dict, dict]:
        release, catalog = self.catalogs._catalog(project_id, str(request.release_id))
        indexed = {item["resourceId"]: item for item in catalog["resources"]}
        selected = sorted(set(request.metric_resource_ids))
        if any(key not in indexed or indexed[key]["kind"] != ResourceKind.METRIC
               or "QUERY" not in indexed[key]["capabilities"] for key in selected):
            raise invalid_selection("invalid_metric_resource", "metricResourceIds", "指标资源不可查询。",
                                    "reload_catalog")
        native = self.runtime.options(release["run_id"], tuple(indexed[key]["name"] for key in selected))
        return release, *self._map_options(request, catalog, native)


    @staticmethod
    def _map_options(request: QueryOptionsRequest, catalog: dict, native: dict) -> tuple[dict, dict]:
        indexed = {item["resourceId"]: item for item in catalog["resources"]}
        selected = sorted(set(request.metric_resource_ids))
        output, mapping = [], {}
        # 原生路径只保存在服务映射内；每个 join 路径保持独立选项身份。
        for entry in [*native["dimensions"], *native["timeDimensions"]]:
            token = entry["token"]
            canonical = json.dumps([str(request.release_id), selected, token], separators=(",", ":"))
            option_id = hashlib.sha256(canonical.encode(UTF8)).hexdigest()
            if option_id in mapping:
                continue
            grain = entry.get("granularity")
            candidates = [item for item in indexed.values() if item["kind"] == ResourceKind.DIMENSION
                          and item["name"] == entry.get("name")]
            resource_id = candidates[0]["resourceId"] if len(candidates) == 1 and not grain else None
            label = candidates[0]["displayName"] if resource_id else entry.get("name", token)
            if grain:
                label = "指标时间（" + GRAIN_LABELS.get(grain, grain) + "）"
            elif PATH_SEPARATOR in token:
                # 可见路径用于区分多种 join 选项；执行仍只接收不可伪造的选项映射。
                label += "（" + DISPLAY_SEPARATOR.join(token.split(PATH_SEPARATOR)[:-1]) + "）"
            output.append({"optionId": option_id, "resourceId": resource_id,
                           "displayName": label,
                           "dimensionType": "time" if grain else entry.get("type", "unknown"),
                           "valueType": "datetime" if grain else entry.get("valueType", "unknown"),
                           "granularities": [grain] if grain else [],
                           "operators": [] if grain else native["allowedFilters"]})
            mapping[option_id] = token
        response = {"releaseId": str(request.release_id), "metricResourceIds": selected, "options": output}
        return response, mapping


    def query_options(self, project_id: str, request: QueryOptionsRequest) -> dict:
        return self._options(project_id, request)[1]


    def submit_options(self, project_id: str, request: QueryOptionsRequest) -> dict:
        release, catalog = self.catalogs._catalog(project_id, str(request.release_id))
        indexed = {item["resourceId"]: item for item in catalog["resources"]}
        selected = sorted(set(request.metric_resource_ids))
        if any(key not in indexed or indexed[key]["kind"] != ResourceKind.METRIC
               or "QUERY" not in indexed[key]["capabilities"] for key in selected):
            raise invalid_selection("invalid_metric_resource", "metricResourceIds",
                                    "指标资源不可查询。", "reload_catalog")
        row = self.runtime.submit_options(release["run_id"], tuple(indexed[key]["name"] for key in selected))
        return {"optionsJobId": row["job_id"], "releaseId": str(request.release_id),
                "state": "READY" if row["status"] == "SUCCEEDED" else row["status"]}


    def get_options(self, project_id: str, options_job_id: str) -> dict:
        row = self.runtime.jobs.get(options_job_id)
        if (not row or row["kind"] != "QUERY_OPTIONS" or row["project_id"] != project_id
                or row["branch_id"] != self.publications._branch(project_id)["branch_id"]):
            raise KeyError(options_job_id)
        release = self.store.release_for_run(project_id, row["parent_run_id"])
        if not release:
            raise KeyError(options_job_id)
        _, catalog = self.catalogs._catalog(project_id, release["release_id"])
        response = {"optionsJobId": options_job_id, "releaseId": release["release_id"],
                    "state": "READY" if row["status"] == "SUCCEEDED" else row["status"]}
        if row["status"] == "SUCCEEDED":
            selected = [item["resourceId"] for item in catalog["resources"]
                        if item["kind"] == ResourceKind.METRIC and item["name"] in row["request_json"]["metrics"]]
            request = QueryOptionsRequest(releaseId=release["release_id"], metricResourceIds=selected)
            native = self.runtime.jobs.result(options_job_id)["payload_json"]
            response.update(self._map_options(request, catalog, native)[0])
        if row["error_code"]:
            from .results import task_error

            response.update(errorCode=row["error_code"], error=task_error(row["error_code"]))
        return response


    def submit_query(self, project_id: str, request: PublishedQueryRequest, identity_scope: str) -> dict:
        # 先恢复同键已受理结果，确保发布切换后重试不会错误创建新查询。
        # 模式专属参数必须显式拒绝，不能接受后在引擎层静默忽略。
        if (request.mode != QueryMode.PREVIEW and request.dataset_resource_id
                or request.mode != QueryMode.DIMENSION_VALUES and request.dimension_option_id):
            raise invalid_selection("invalid_mode_fields", "mode", "查询模式与资源参数不匹配。")
        branch = self.publications._branch(project_id)
        identity = [project_id, identity_scope]
        if branch["mode"] != PRODUCTION:
            identity.append(branch["branch_id"])
        scope = QUERY_SCOPE + json.dumps(identity, separators=(",", ":"))

        def recover(prior):
            snapshot = prior["request_json"]
            timezone = snapshot.get("businessTimezone", DEFAULT_TIMEZONE)
            original = PublishedQueryRequest.model_validate(snapshot["publicationRequest"])
            if canonical_query(original, timezone) != canonical_query(request, timezone):
                raise StoreConflict("查询幂等键已用于不同输入")
            return query_receipt(prior, project_id, request.release_id)

        prior = self.runtime.jobs.by_key(scope, request.idempotency_key)
        if prior:
            return recover(prior)
        try:
            release, catalog = self.catalogs._catalog(project_id, str(request.release_id))
            options, mapping = {"options": []}, {}
            if request.mode != QueryMode.PREVIEW:
                _, options, mapping = self._options(project_id, QueryOptionsRequest(
                    release_id=request.release_id, metric_resource_ids=request.metric_resource_ids))
        except ReleaseGone:
            # 首次查键与版本读取之间可能发生同键受理和发布切换，再恢复一次已提交结果。
            prior = self.runtime.jobs.by_key(scope, request.idempotency_key)
            if prior:
                return recover(prior)
            raise
        timezone = release["request_json"].get("businessTimezone", DEFAULT_TIMEZONE)
        request = normalize_query_time(request, timezone)
        public = canonical_query(request, timezone)
        indexed = {item["resourceId"]: item for item in catalog["resources"]}
        if request.mode == QueryMode.PREVIEW:
            resource = indexed.get(request.dataset_resource_id)
            if (not resource or "PREVIEW" not in resource["capabilities"] or request.metric_resource_ids
                    or request.group_by or request.filters or request.order_by or request.dimension_option_id
                    or request.start_time or request.end_time):
                raise invalid_selection("invalid_preview_fields", "datasetResourceId",
                                        "资源不可预览或包含非法预览参数。")
        option_index = {item["optionId"]: item for item in options["options"]}
        groups = []
        for position, selection in enumerate(request.group_by):
            option = option_index.get(selection.option_id)
            if not option or selection.grain and selection.grain not in option["granularities"]:
                raise invalid_selection("invalid_dimension_option", f"groupBy[{position}].optionId",
                                        "维度选项或粒度无效。", "reload_query_options")
            groups.append(mapping[selection.option_id])
        filters = []
        for position, selection in enumerate(request.filters):
            option = option_index.get(selection.option_id)
            if not option or selection.operator not in option["operators"]:
                raise invalid_selection("invalid_filter_option", f"filters[{position}]",
                                        "筛选选项或操作符无效。", "reload_query_options")
            filters.append({"field": mapping[selection.option_id], "operator": selection.operator,
                            "value": selection.value})
        if request.mode == QueryMode.DIMENSION_VALUES and request.dimension_option_id not in mapping:
            raise invalid_selection("invalid_dimension_option", "dimensionOptionId",
                                    "维度值查询缺少合法选项。", "reload_query_options")
        order_fields = {key: indexed[key]["name"] for key in public["metricResourceIds"]}
        order_fields.update({item.option_id: mapping[item.option_id] for item in request.group_by})
        if any(item.field_id not in order_fields for item in request.order_by):
            raise invalid_selection("invalid_order_field", "orderBy", "排序字段未被选择。")
        engine = PlatformQueryRequest(
            run_id=release["run_id"], idempotency_key=request.idempotency_key, mode=request.mode,
            metrics=[indexed[key]["name"] for key in public["metricResourceIds"]], group_by=groups, filters=filters,
            start_time=request.start_time, end_time=request.end_time, limit=request.limit,
            order_by=[("-" if item.direction == DESCENDING else "") + order_fields[item.field_id]
                      for item in request.order_by], dataset_resource_id=request.dataset_resource_id,
            dimension=mapping.get(request.dimension_option_id),
        )
        if engine.start_time and engine.end_time and engine.start_time > engine.end_time:
            raise invalid_selection("invalid_time_range", "endTime", "结束时间不能早于开始时间。")
        try:
            row = self.store.reserve(project_id, release, scope, request.idempotency_key,
                {"publicationRequest": public, "businessTimezone": timezone,
                 "engineRequest": engine.model_dump(mode=JSON_MODE, by_alias=True)},
                self.runtime.settings.command_timeout_seconds)
        except InactiveRelease as error:
            raise ReleaseGone("发布版本已替代") from error
        return recover(row)

    def get_query(self, project_id: str, query_id: str) -> dict:
        row, metadata = self._query_context(project_id, query_id)
        return {**self.runtime.get_query(row["job_id"]), **metadata}


    def _query_context(self, project_id: str, query_id: str) -> tuple[dict, dict]:
        """只读任务与发布身份，状态轮询绝不加载 runtime_job_result.payload_json。"""
        target_id, alias = self.store.query_id(project_id, query_id)
        row = self.runtime.jobs.get(target_id)
        if (not row or row["project_id"] != project_id or row["kind"] != QUERY_KIND):
            raise KeyError(query_id)
        # 已受理查询只按其固定 run 读取，不重新检查当前活动指针。
        release = self.store.release_for_run(project_id, row["parent_run_id"])
        if not release or not alias and not row["request_json"].get("publicationRequest"):
            raise KeyError(query_id)
        public = row["request_json"].get("publicationRequest") or {}
        engine = row["request_json"].get("engineRequest", row["request_json"])
        time_metadata = (query_time_metadata(PublishedQueryRequest.model_validate(public),
                         row["request_json"].get("businessTimezone", DEFAULT_TIMEZONE)) if public else {})
        return row, {**time_metadata, "queryId": query_id, "projectId": project_id,
                "releaseId": public.get("releaseId", release["release_id"]), "mode": engine.get("mode"),
                "targetCommitSha": release["request_json"].get("commitSha")}


    def query_status(self, project_id: str, query_id: str) -> dict:
        from .results import task_error

        row, metadata = self._query_context(project_id, query_id)
        available = row["status"] == "SUCCEEDED"
        value = {**metadata, "state": "READY" if available else row["status"], "resultAvailable": available}
        if row["error_code"]:
            value.update(errorCode=row["error_code"], error=task_error(row["error_code"]))
        return value


    def query_result_page(self, project_id: str, query_id: str, offset=0, limit=100) -> dict:
        from .results import result_page

        row, metadata = self._query_context(project_id, query_id)
        if row["status"] == "FAILED":
            return self.query_status(project_id, query_id)
        if row["status"] != "SUCCEEDED":
            raise PublicationError("result_not_ready", "result_not_ready", None,
                                   "查询尚未完成，请继续轮询。", True, "poll_query", 409)
        payload = self.runtime.jobs.result(row["job_id"])["payload_json"]
        return result_page(payload, metadata, offset, limit)


