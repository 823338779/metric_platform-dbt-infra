"""公共时间按发布固定业务时区解释；不声称已完成引擎粒度对齐。"""

from zoneinfo import ZoneInfo

from dbt_metricflow_service import constants as strings

from ..models.payloads import NormalizedTimeRange, QueryTimeMetadata
from ..models.queries import QueryRequest

BOUNDARY_POLICY = strings.QUERY_TIME_BOUNDARY_POLICY


def normalize_query_time(request: QueryRequest, timezone: str) -> QueryRequest:
    zone = ZoneInfo(timezone)
    result = request.model_copy()
    result.start_time = (request.start_time.astimezone(zone).replace(tzinfo=None)
                         if request.start_time and request.start_time.tzinfo else request.start_time)
    result.end_time = (request.end_time.astimezone(zone).replace(tzinfo=None)
                       if request.end_time and request.end_time.tzinfo else request.end_time)
    return result


def canonical_query(request: QueryRequest) -> QueryRequest:
    result = request.model_copy()
    result.metrics = sorted(set(request.metrics))
    return result


def query_time_metadata(normalized: QueryRequest, timezone: str) -> QueryTimeMetadata:
    return QueryTimeMetadata(business_timezone=timezone, normalized_time_range=NormalizedTimeRange(
        start_time=normalized.start_time.isoformat() if normalized.start_time else None,
        end_time=normalized.end_time.isoformat() if normalized.end_time else None,
    ), boundary_policy=BOUNDARY_POLICY)
