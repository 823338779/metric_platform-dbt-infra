"""服务拥有发布输入与查询映射。"""

import hashlib
import json
from uuid import uuid4

from ..platform.bindings import ProjectBinding, observe_revision
from ..platform.models import PlatformQueryRequest, QueryMode
from ..platform.namespace import validate_schema_name
from ..storage.branches import SQL_BRANCH_LOCK, SQL_PARENT_LOCK, BranchStore
from ..storage.jobs import SQL_SELECT_FROM_RUNTIME_JOB_2, SQL_SELECT_FROM_RUNTIME_JOB_4, StoreConflict
from ..storage.publications import (
    CATALOG_PATH,
    SQL_ATTACH_RUN,
    SQL_BRANCH_BY_KEY,
    PublicationStore,
)
from .errors import INVALID_SELECTION, PublicationError
from .models import CATALOG_SCHEMA_VERSION, PublishedQueryRequest, QueryOptionsRequest, ResourceKind
from .query_time import DEFAULT_TIMEZONE, canonical_query, normalize_query_time, query_time_metadata

BUILD = "BUILD_RUN"
SCOPE = "PUBLICATION"
SCHEMA_PREFIX = "run_"
SQL_PROJECTS = "SELECT project_id FROM runtime_project ORDER BY project_id"
SQL_RELEASES = "SELECT * FROM runtime_release WHERE project_id=%s AND branch_id=%s ORDER BY sequence DESC"
UTF8 = "utf-8"
PUBLISHED = "PUBLISHED"
ACTIVE_BRANCH = "ACTIVE"
PRODUCTION = "PRODUCTION"
CATALOG_SEARCH_FIELDS = ("name", "displayName", "description")
QUERY_KIND = "METRIC_QUERY"
QUERY_SCOPE = "PUBLISHED_QUERY:"
JSON_MODE = "json"
DESCENDING = "DESC"
GRAIN_LABELS = {"second": "秒", "minute": "分钟", "hour": "小时", "day": "日", "week": "周",
                "month": "月", "quarter": "季度", "year": "年"}
PATH_SEPARATOR = "__"
DISPLAY_SEPARATOR = " → "
SQL_QUERY_ALIAS = """SELECT target_id FROM runtime_legacy_identity
 WHERE project_id=%s AND kind='QUERY' AND legacy_id=%s"""
SQL_QUERY_RELEASE = "SELECT * FROM runtime_release WHERE project_id=%s AND run_id=%s AND state='PUBLISHED'"
PROTOCOL_VERSION = "agent-dbt-v1"
AGENT_CAPABILITIES = ["draft-validation-v1", "query-options-async-v1", "query-results-page-v1", "query-time-v1",
                      "branch-development-v1", "draft-validation-v2"]
VALIDATION_CHECKS = ("allTestsPassed", "representativeQueryPassed", "relationsVerified")
CHECK_PASSED = "PASSED"
CHECK_FAILED = "FAILED"
SQL_SCAN_OWNED = """SELECT 1 FROM runtime_branch WHERE project_id=%s AND branch_id=%s
 AND scan_token=%s AND scan_expires_at>clock_timestamp() AND status='ACTIVE'"""


def invalid_selection(reason, field, message, recovery="fix_query_selection"):
    return PublicationError(INVALID_SELECTION, reason, field, message, False, recovery)


class InvalidPublishedArtifact(RuntimeError):
    """封存目录发生存储或协议异常，不能归因于用户查询参数。"""


class ReleaseGone(ValueError):
    """历史记录仍然存在，但不能用于新的目录操作或查询。"""


def release_descriptor(row: dict) -> dict:
    return {"projectId": row["project_id"], "releaseId": row["release_id"], "runId": row["run_id"],
            "artifactSetId": row["artifact_set_id"], "publicationSequence": row["sequence"],
            "publishedAt": row["published_at"], "sourceSha": row["request_json"].get("commitSha"),
            "buildMode": row["build_mode"], "catalogSchemaVersion": CATALOG_SCHEMA_VERSION,
            "catalogDigest": row["catalog_digest"], "state": row["state"], "errorCode": row["error_code"],
            "createdAt": row["created_at"],
            "businessTimezone": row["request_json"].get("businessTimezone", DEFAULT_TIMEZONE),
            "projectSubdir": row["request_json"].get("projectSubdir", ".")}


def query_receipt(row: dict, project_id: str, release_id) -> dict:
    # 重试恢复身份后仍由查询读取端获取结果，受理响应保持可轮询状态。
    return {"queryId": row["job_id"], "projectId": project_id,
            "releaseId": str(release_id), "state": "QUEUED"}


class PublicationService:
    def __init__(self, runtime, *, branch_id: str | None = None):
        # 复用运行时连接池及工具链，管理入口不启动另一个 worker。
        self.runtime = runtime
        self.store = PublicationStore(runtime.db)
        # 每个请求固定分支，未指定时始终选择 main；不能修改此值切换在途请求。
        self.branch_id = branch_id

    def _branch(self, project_id):
        # UUID 还必须属于当前项目，显式 main 与旧无分支入口使用相同身份。
        branches = BranchStore(self.runtime.db)
        return branches.get(project_id, self.branch_id) if self.branch_id else branches.production(project_id)

    def _release(self, project_id, release_id):
        # 封存目录原始字节不改变，归属由外层发布记录验证。
        release = self.store.get_release(project_id, release_id)
        if release["branch_id"] != self._branch(project_id)["branch_id"]:
            raise KeyError(release_id)
        return release

    def submit(self, project_id: str, idempotency_key: str, *, branch_id: str | None = None,
               _observed: tuple[str, str] | None = None, _scan_token: str | None = None,
               _expected_sequence: int | None = None) -> dict:
        # 既有幂等请求返回原版本，不因远端 main 已推进而创建另一候选。
        branches = BranchStore(self.runtime.db)
        branch = branches.get(project_id, branch_id) if branch_id else self._branch(project_id)
        with self.runtime.db.transaction() as cursor:
            cursor.execute(SQL_BRANCH_BY_KEY, (project_id, branch["branch_id"], idempotency_key))
            existing = cursor.fetchone()
            if existing:
                return existing
        project = self.runtime.jobs.project(project_id)
        if not project:
            raise KeyError(project_id)
        binding = {**branch["binding_config"], "projectId": project_id}
        configured = ProjectBinding(project_id, binding["remote"], binding["projectSubdir"],
                                    binding["profileBindingId"], binding.get("schemaName"))
        sha, digest = (_observed if _observed is not None else
                       observe_revision(configured, self.runtime.settings.temp_root, git_ref=branch["git_ref"]))
        request = {"projectId": project_id, "commitSha": sha, "projectDigest": digest,
                   "profileBindingId": configured.profile_binding_id, "configVersion": branch["config_version"],
                   "toolchainVersion": self.runtime.toolchain,
                   "businessTimezone": binding.get("businessTimezone", DEFAULT_TIMEZONE),
                   "projectSubdir": configured.project_subdir, "gitRef": branch["git_ref"]}
        # 候选和可领取任务同事务出现，杜绝 worker 先完成再关联发布的竞态。
        with self.runtime.db.transaction() as cursor:
            # Git I/O 前的序号必须仍有效；扫描和显式入口共用同一个受理 CAS。
            cursor.execute(SQL_PARENT_LOCK, (project_id,))
            cursor.execute(SQL_BRANCH_LOCK, (project_id, branch["branch_id"], branch["branch_id"]))
            current = cursor.fetchone()
            expected = branch["publication_sequence"] if _expected_sequence is None else _expected_sequence
            if (current["publication_sequence"] != expected
                    or current["config_version"] != branch["config_version"]
                    or current["binding_config"] != branch["binding_config"]):
                # 并发同键可恢复原受理；其他旧观测必须重试并重新读取 Git。
                cursor.execute(SQL_BRANCH_BY_KEY, (project_id, branch["branch_id"], idempotency_key))
                prior = cursor.fetchone()
                if prior:
                    return prior
                raise StoreConflict("分支在源码观察期间已变化，请重新受理")
            release = self.store.create_candidate(project_id, request, idempotency_key,
                                                  branch_id=branch["branch_id"], _cursor=cursor)
            # 扫描在外部 Git I/O 期间失去租约时，回滚候选及序号，不能迟到受理。
            if _scan_token is not None:
                cursor.execute(SQL_SCAN_OWNED, (project_id, branch["branch_id"], _scan_token))
                if not cursor.fetchone():
                    raise StoreConflict("分支扫描租约已失效")
            if release["run_id"]:
                return release
            run_id = uuid4()
            schema = (
                validate_schema_name(configured.schema_name) if configured.schema_name else SCHEMA_PREFIX + run_id.hex
            )
            job = self.runtime.jobs.reserve(
                BUILD, project_id, {**request, "binding": binding, "releaseId": release["release_id"]},
                job_id=str(run_id), idempotency_scope=SCOPE + project_id + branch["branch_id"],
                idempotency_key=idempotency_key,
                config_version=branch["config_version"], toolchain_version=self.runtime.toolchain,
                schema_name=schema, profile_binding_id=configured.profile_binding_id,
                timeout_seconds=self.runtime.settings.command_timeout_seconds,
                expected_revision=project["revision"], _cursor=cursor,
            )
            cursor.execute(SQL_ATTACH_RUN, (job["job_id"], release["release_id"]))
            return {**release, "run_id": job["job_id"]}

    def projects(self) -> list[dict]:
        with self.runtime.db.transaction() as cursor:
            cursor.execute(SQL_PROJECTS)
            projects = cursor.fetchall()
        return [self.publication(row["project_id"]) for row in projects]

    def _descriptor(self, row):
        # 新分支契约携带固定上下文；旧生产协议保持原响应形状。
        result = release_descriptor(row)
        if self.branch_id:
            branch = self._branch(row["project_id"])
            result.update(branchId=branch["branch_id"], gitRef=branch["git_ref"])
        return result

    def publication(self, project_id: str) -> dict:
        branch = self._branch(project_id)
        result = self.store.get_publication(project_id, branch_id=branch["branch_id"])
        binding = branch["binding_config"]
        result.update(protocolVersion=PROTOCOL_VERSION, capabilities=AGENT_CAPABILITIES,
                      projectSubdir=binding.get("projectSubdir", "."),
                      businessTimezone=binding.get("businessTimezone", DEFAULT_TIMEZONE))
        if self.branch_id:
            result.update(branchId=branch["branch_id"], gitRef=branch["git_ref"])
            # Agent 必须将校验证据与当前配置比较，不能把旧发布的配置当成当前配置。
            result.update(configVersion=branch["config_version"],
                          toolchainVersion=getattr(self.runtime, "toolchain", None))
        if result["activePublication"]:
            result["activePublication"] = self._descriptor(result["activePublication"])
        return result

    def releases(self, project_id: str) -> list[dict]:
        branch = self._branch(project_id)
        with self.runtime.db.transaction() as cursor:
            cursor.execute(SQL_RELEASES, (project_id, branch["branch_id"]))
            rows = cursor.fetchall()
        return [self._descriptor(row) for row in rows]

    def release(self, project_id: str, release_id: str) -> dict:
        row = self._release(project_id, release_id)
        # 只展示封存的布尔证明和固定文案，不回显数据库、SQL 或 CLI 异常正文。
        checks = []
        if row["artifact_set_id"]:
            evidence = self.runtime.artifacts.metadata(row["artifact_set_id"])["validation_json"]
            checks = [{"name": name, "status": CHECK_PASSED if evidence.get(name) is True else CHECK_FAILED,
                       "message": None} for name in VALIDATION_CHECKS]
        elif row["run_id"]:
            job = self.runtime.jobs.get(row["run_id"])
            summary = (job.get("error_detail") or {}).get("validationSummary") if job else None
            if summary:
                return {**self._descriptor(row), "validationSummary": summary}
        return {**self._descriptor(row), "validationSummary": {
            "phase": row["state"], "checks": checks, "truncated": False}}

    def _active_release(self, project_id: str, release_id: str) -> dict:
        release = self._release(project_id, release_id)
        branch = self._branch(project_id)
        publication = self.store.get_publication(project_id, branch_id=branch["branch_id"])["activePublication"]
        if release["state"] != PUBLISHED:
            raise KeyError(release_id)
        if (branch["status"] != ACTIVE_BRANCH or not publication
                or publication["release_id"] != release["release_id"]):
            raise ReleaseGone("发布版本已替代")
        return release

    def _catalog(self, project_id: str, release_id: str) -> tuple[dict, dict]:
        release = self._active_release(project_id, release_id)
        # 文件读取校验摘要；不在请求过程中重新解析原生 manifest 或建立投影。
        try:
            raw = self.runtime.artifacts.read_file(release["artifact_set_id"], CATALOG_PATH)
            if hashlib.sha256(raw).hexdigest() != release["catalog_digest"]:
                raise ValueError("发布目录摘要不匹配")
            return release, json.loads(raw)
        except (ValueError, KeyError) as error:
            raise InvalidPublishedArtifact("无法读取已发布目录") from error

    def catalog(self, project_id: str, release_id: str, q: str = "", kind: str | None = None,
                page: int = 1, size: int = 50) -> dict:
        _, catalog = self._catalog(project_id, release_id)
        needle = q.casefold()
        resources = [item for item in catalog["resources"] if (kind is None or item["kind"] == kind)
                     and (not needle or any(needle in (item.get(field) or "").casefold()
                                            for field in CATALOG_SEARCH_FIELDS))]
        resources.sort(key=lambda item: item["resourceId"])
        return {"releaseId": str(release_id), "page": page, "size": size, "total": len(resources),
                "resources": resources[(page - 1) * size:page * size]}

    def resource(self, project_id: str, release_id: str, resource_id: str, view: str | None = None) -> dict:
        release, catalog = self._catalog(project_id, release_id)
        resource = next((item for item in catalog["resources"] if item["resourceId"] == resource_id), None)
        if resource is None:
            raise KeyError(resource_id)
        envelope = {"releaseId": str(release_id), "resourceId": resource_id}
        if view == "lineage":
            return {**envelope, "dependencies": [edge for edge in catalog["relations"]
                                                 if resource_id in (edge["upstreamResourceId"],
                                                                    edge["downstreamResourceId"])]}
        if view == "native-details":
            return {**envelope, "nativeDetails": resource["nativeDetails"]}
        if view == "source":
            path = resource.get("sourcePath")
            if not path:
                raise KeyError(resource_id)
            run = self.runtime.jobs.get(release["run_id"])
            content = self.runtime.artifacts.read_file(run["input_set_id"], path).decode(UTF8)
            return {**envelope, "path": path, "content": content}
        return {**envelope, **resource}

    def _options(self, project_id: str, request: QueryOptionsRequest) -> tuple[dict, dict, dict]:
        release, catalog = self._catalog(project_id, str(request.release_id))
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
        release, catalog = self._catalog(project_id, str(request.release_id))
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
                or row["branch_id"] != self._branch(project_id)["branch_id"]):
            raise KeyError(options_job_id)
        with self.runtime.db.transaction() as cursor:
            cursor.execute(SQL_QUERY_RELEASE, (project_id, row["parent_run_id"]))
            release = cursor.fetchone()
        if not release:
            raise KeyError(options_job_id)
        _, catalog = self._catalog(project_id, release["release_id"])
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
        branch = self._branch(project_id)
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
            release, catalog = self._catalog(project_id, str(request.release_id))
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
        # 与发布共用项目行锁；parent 先锁保持现有 JobStore 与清理的锁顺序。
        with self.runtime.db.transaction() as cursor:
            cursor.execute(SQL_SELECT_FROM_RUNTIME_JOB_4, (release["run_id"],))
            parent = cursor.fetchone()
            cursor.execute(SQL_PARENT_LOCK, (project_id,))
            cursor.execute(SQL_BRANCH_LOCK, (project_id, branch["branch_id"], branch["branch_id"]))
            project = cursor.fetchone()
            # 等锁期间另一请求可能已受理同一幂等键；先恢复它再判断版本。
            cursor.execute(SQL_SELECT_FROM_RUNTIME_JOB_2, (scope, request.idempotency_key))
            prior = cursor.fetchone()
            if prior:
                return recover(prior)
            if project["status"] != ACTIVE_BRANCH or project["active_release_id"] != release["release_id"]:
                raise ReleaseGone("发布版本已替代")
            row = self.runtime.jobs.reserve(
                QUERY_KIND, project_id, {"publicationRequest": public, "businessTimezone": timezone,
                                         "engineRequest": engine.model_dump(mode=JSON_MODE, by_alias=True)},
                idempotency_scope=scope, idempotency_key=request.idempotency_key,
                parent_run_id=parent["job_id"], input_set_id=parent["output_set_id"],
                config_version=parent["config_version"], toolchain_version=parent["toolchain_version"],
                profile_binding_id=parent["profile_binding_id"], schema_name=parent["schema_name"],
                timeout_seconds=self.runtime.settings.command_timeout_seconds, _cursor=cursor,
            )
        return query_receipt(row, project_id, request.release_id)

    def get_query(self, project_id: str, query_id: str) -> dict:
        row, metadata = self._query_context(project_id, query_id)
        return {**self.runtime.get_query(row["job_id"]), **metadata}

    def _query_context(self, project_id: str, query_id: str) -> tuple[dict, dict]:
        """只读任务与发布身份，状态轮询绝不加载 runtime_job_result.payload_json。"""
        with self.runtime.db.transaction() as cursor:
            cursor.execute(SQL_QUERY_ALIAS, (project_id, query_id))
            alias = cursor.fetchone()
        target_id = alias["target_id"] if alias else query_id
        row = self.runtime.jobs.get(target_id)
        if (not row or row["project_id"] != project_id or row["kind"] != QUERY_KIND
                or row["branch_id"] != self._branch(project_id)["branch_id"]):
            raise KeyError(query_id)
        # 已受理查询只按其固定 run 读取，不重新检查当前活动指针。
        with self.runtime.db.transaction() as cursor:
            cursor.execute(SQL_QUERY_RELEASE, (project_id, row["parent_run_id"]))
            release = cursor.fetchone()
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
