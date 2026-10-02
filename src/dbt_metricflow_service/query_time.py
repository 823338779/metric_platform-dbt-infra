"""公共时间按发布固定业务时区解释；不声称已完成引擎粒度对齐。"""

from zoneinfo import ZoneInfo

from .publication_models import PublishedQueryRequest

DEFAULT_TIMEZONE = "Asia/Shanghai"
TIME_FIELDS = ("start_time", "end_time")
BOUNDARY_POLICY = "metricflow_granularity_alignment"


def normalize_query_time(request: PublishedQueryRequest, timezone: str) -> PublishedQueryRequest:
    zone = ZoneInfo(timezone)
    values = {}
    for field in TIME_FIELDS:
        value = getattr(request, field)
        # 旧平台已经发送业务日历值，只有带 offset 的新输入需要转换。
        values[field] = value.astimezone(zone).replace(tzinfo=None) if value and value.tzinfo else value
    return request.model_copy(update=values)


def canonical_query(request: PublishedQueryRequest, timezone: str) -> dict:
    value = normalize_query_time(request, timezone).model_dump(mode="json", by_alias=True)
    value["metricResourceIds"] = sorted(set(value["metricResourceIds"]))
    return value


def query_time_metadata(request: PublishedQueryRequest, timezone: str) -> dict:
    normalized = canonical_query(request, timezone)
    return {"businessTimezone": timezone, "normalizedTimeRange": {
                "startTime": normalized["startTime"], "endTime": normalized["endTime"]},
            "boundaryPolicy": BOUNDARY_POLICY}
