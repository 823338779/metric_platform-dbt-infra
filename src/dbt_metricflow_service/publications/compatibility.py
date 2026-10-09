"""兼容接口只在 dbt-service 内做名称转资源身份。"""

from ..platform.models import QueryMode
from .models import PublishedQueryRequest, QueryOptionsRequest, ResourceKind

COMMON_FIELDS = frozenset({"releaseId", "limit"})
QUERY_FIELDS = COMMON_FIELDS | frozenset({"metrics", "groupBy", "filters", "startTime", "endTime", "orderBy"})
VALUES_FIELDS = COMMON_FIELDS | frozenset({"metrics", "dimension", "filters"})
IDENTITY = "platform-v1"
PENDING = "PENDING"


def legacy_selection(service, project_id, release_id, metrics):
    # 名称仅用来定位已发布的唯一指标，不接受任意 MetricFlow 表达式。
    release, catalog = service._catalog(project_id, release_id)
    named = {item["name"]: item["resourceId"] for item in catalog["resources"]
             if item["kind"] == ResourceKind.METRIC}
    if not isinstance(metrics, list) or not metrics or any(name not in named for name in metrics):
        raise ValueError("指标不存在")
    ids = [named[name] for name in metrics]
    _, options, mapping = service._options(project_id, QueryOptionsRequest(
        releaseId=release_id, metricResourceIds=ids))
    return release, ids, options, {token: key for key, token in mapping.items()}


def submit_legacy(service, project_id, mode, body, key, resource_id=None):
    mode = QueryMode(mode)
    allowed = COMMON_FIELDS if mode == QueryMode.PREVIEW else (
        VALUES_FIELDS if mode == QueryMode.DIMENSION_VALUES else QUERY_FIELDS)
    if not isinstance(body, dict) or set(body) - allowed or not body.get("releaseId"):
        raise ValueError("兼容查询参数无效")
    ids, tokens = [], {}
    if mode != QueryMode.PREVIEW:
        _, ids, _, tokens = legacy_selection(service, project_id, body["releaseId"], body.get("metrics"))

    def option(token):
        if token not in tokens:
            raise ValueError("查询维度不属于当前发布选项")
        return tokens[token]

    fields = dict(zip(body.get("metrics") or [], ids, strict=True))
    fields.update({token: option(token) for token in body.get("groupBy") or []})
    ordering = []
    for field in body.get("orderBy") or []:
        if not isinstance(field, str) or field.lstrip("-") not in fields:
            raise ValueError("排序字段未被选择")
        ordering.append({"fieldId": fields[field.lstrip("-")], "direction": "DESC" if field.startswith("-") else "ASC"})
    filters = []
    for item in body.get("filters") or []:
        if not isinstance(item, dict) or set(item) != {"field", "operator", "value"}:
            raise ValueError("筛选条件无效")
        filters.append({"optionId": option(item["field"]), "operator": item["operator"], "value": item["value"]})
    request = PublishedQueryRequest(
        releaseId=body["releaseId"], idempotencyKey=key, mode=mode, metricResourceIds=ids,
        groupBy=[{"optionId": option(token)} for token in body.get("groupBy") or []],
        filters=filters, orderBy=ordering, startTime=body.get("startTime"), endTime=body.get("endTime"),
        limit=body.get("limit") or 1000, datasetResourceId=resource_id,
        dimensionOptionId=option(body.get("dimension")) if mode == QueryMode.DIMENSION_VALUES else None)
    return {**service.submit_query(project_id, request, IDENTITY), "status": PENDING}
