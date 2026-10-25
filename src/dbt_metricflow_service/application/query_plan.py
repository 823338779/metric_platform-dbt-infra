"""服务查询规则：固定选项身份、时间归一化及到引擎参数的纯转换。"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from zoneinfo import ZoneInfo

from dbt_metricflow_service import constants as strings

from ..engines.models import CompiledQuery, DimensionQueryResult, NativeDimension, QueryFilter
from ..models.queries import (
    DimensionOption,
    DimensionValuesRequest,
    MetricQueryRequest,
    NormalizedTimeRange,
)
from .errors import ServiceError


@dataclass(frozen=True)
class ResolvedDimension:
    option: DimensionOption
    token: str
    grain_tokens: dict[str, str]


def resolve_dimensions(
    build_id: str, metrics: list[str], native: DimensionQueryResult,
) -> dict[str, ResolvedDimension]:
    entries = [entry.model_copy(deep=True) for entry in native.dimensions]
    time_tokens = {entry.granularity: entry.token for entry in native.time_dimensions}
    metric_time = next((entry for entry in entries if entry.name == strings.METRIC_TIME), None)
    if metric_time is None and time_tokens:
        metric_time = NativeDimension(token=next(iter(time_tokens.values())), name=strings.METRIC_TIME,
                                      type=strings.TIME_DIMENSION_TYPE, operators=[])
        entries.append(metric_time)
    if metric_time is not None:
        metric_time.grain_tokens = {**time_tokens, **metric_time.grain_tokens}
    resolved = {}
    for entry in entries:
        canonical = json.dumps([str(build_id), metrics, entry.token], separators=(",", ":"))
        option_id = hashlib.sha256(canonical.encode()).hexdigest()
        if option_id in resolved:
            continue
        label = entry.name or entry.token
        if "__" in entry.token and entry.name != strings.METRIC_TIME:
            label += "（" + " → ".join(entry.token.split("__")[:-1]) + "）"
        option = DimensionOption(
            option_id=option_id, name=entry.name, semantic_model_name=entry.semantic_model_name,
            display_name=label, dimension_type=entry.type, granularities=list(entry.grain_tokens),
            operators=entry.operators if entry.operators is not None else native.allowed_filters,
        )
        resolved[option_id] = ResolvedDimension(option, entry.token, entry.grain_tokens)
    return resolved


def normalize_query(
    request: MetricQueryRequest | DimensionValuesRequest, timezone: str,
) -> MetricQueryRequest | DimensionValuesRequest:
    zone = ZoneInfo(timezone)
    start = request.start_time
    end = request.end_time
    start = start.astimezone(zone).replace(tzinfo=None) if start and start.tzinfo else start
    end = end.astimezone(zone).replace(tzinfo=None) if end and end.tzinfo else end
    if start and end and start > end:
        raise ServiceError(strings.INVALID_TIME_RANGE, "startTime must not exceed endTime")
    return request.model_copy(update={"metrics": sorted(set(request.metrics)), "start_time": start, "end_time": end})


def time_range(request: MetricQueryRequest | DimensionValuesRequest) -> NormalizedTimeRange:
    return NormalizedTimeRange(start_time=request.start_time.isoformat() if request.start_time else None,
                               end_time=request.end_time.isoformat() if request.end_time else None)


def compile_query(
    request: MetricQueryRequest | DimensionValuesRequest, dimensions: dict[str, ResolvedDimension],
) -> CompiledQuery:
    groups: list[str] = []
    order_by: list[str] = []
    if isinstance(request, DimensionValuesRequest):
        dimension = dimensions.get(request.dimension_option_id)
        if dimension is None:
            raise ServiceError(strings.INVALID_DIMENSION_OPTION, "dimension does not belong to this selection")
        groups.append(dimension.token)
    else:
        order_fields = {name: name for name in request.metrics}
        for selected in request.group_by:
            dimension = dimensions.get(selected.option_id)
            if dimension is None or selected.grain is not None and selected.grain not in dimension.grain_tokens:
                raise ServiceError(strings.INVALID_DIMENSION_OPTION, "option or grain belongs to another selection")
            token = dimension.grain_tokens[selected.grain] if selected.grain is not None else dimension.token
            groups.append(token)
            order_fields[selected.option_id] = token
        for order in request.order_by:
            if order.field_id not in order_fields:
                raise ServiceError(strings.INVALID_ORDER_FIELD, "order field was not selected")
            order_by.append(("-" if order.direction == "DESC" else "") + order_fields[order.field_id])
    filters = []
    for selected in request.filters:
        dimension = dimensions.get(selected.option_id)
        if dimension is None or selected.operator not in dimension.option.operators:
            raise ServiceError(strings.INVALID_FILTER_OPTION, "filter does not belong to this selection")
        filters.append(QueryFilter(field=dimension.token, operator=selected.operator, value=selected.value))
    return CompiledQuery(mode=request.mode, metrics=request.metrics, group_by=groups, filters=filters,
                         start_time=request.start_time, end_time=request.end_time, order_by=order_by,
                         limit=request.limit)
