"""公共时间按发布固定业务时区解释；不声称已完成引擎粒度对齐。"""

from zoneinfo import ZoneInfo

from dbt_metricflow_service.models.payloads import JsonObject

from ..models.queries import QueryRequest

TIME_FIELDS = ("start_time", "end_time")
BOUNDARY_POLICY = "metricflow_granularity_alignment"


def normalize_query_time(request: QueryRequest, timezone: str) -> QueryRequest:
    zone = ZoneInfo(timezone)
    values = {}
    for field in TIME_FIELDS:
        value = getattr(request, field)
        # 无 offset 的输入表示业务日历时间，带 offset 的输入转换到固定业务时区。
        values[field] = value.astimezone(zone).replace(tzinfo=None) if value and value.tzinfo else value
    return request.model_copy(update=values)


def canonical_query(request: QueryRequest, timezone: str) -> JsonObject:
    value = normalize_query_time(request, timezone).model_dump(mode="json", by_alias=True)
    value["metricResourceIds"] = sorted(set(value["metricResourceIds"]))
    return value


def query_time_metadata(request: QueryRequest, timezone: str) -> JsonObject:
    normalized = canonical_query(request, timezone)
    return {
        "businessTimezone": timezone,
        "normalizedTimeRange": {"startTime": normalized["startTime"], "endTime": normalized["endTime"]},
        "boundaryPolicy": BOUNDARY_POLICY,
    }
