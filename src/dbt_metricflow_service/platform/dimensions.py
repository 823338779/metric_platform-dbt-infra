"""由原生可用维度生成固定构建和指标集合的公开维度选项身份。"""

import hashlib
import json
from dataclasses import dataclass

from dbt_metricflow_service import constants as strings

from ..models.payloads import AvailableDimensions, DimensionOption
from .models import DimensionQueryResult, NativeDimension

METRIC_TIME = strings.METRIC_TIME
PATH_SEPARATOR = "__"


@dataclass(frozen=True)
class DimensionBinding:
    token: str
    grain_tokens: dict[str, str]


def map_available_dimensions(
    build_id: str, metric_ids: list[str], native: DimensionQueryResult,
) -> tuple[AvailableDimensions, dict[str, DimensionBinding]]:
    selected = sorted(set(metric_ids))
    output: list[DimensionOption] = []
    mapping: dict[str, DimensionBinding] = {}
    entries = [entry.model_copy(deep=True) for entry in native.dimensions]
    time_tokens = {entry.granularity: entry.token for entry in native.time_dimensions}
    metric_time = next((entry for entry in entries if entry.name == METRIC_TIME), None)
    if metric_time is None and time_tokens:
        metric_time = NativeDimension(
            token=next(iter(time_tokens.values())),
            name=METRIC_TIME,
            type=strings.TIME_DIMENSION_TYPE,
            value_type=strings.DATETIME_VALUE_TYPE,
            operators=[],
        )
        entries.append(metric_time)
    if metric_time is not None:
        metric_time.grain_tokens = {**time_tokens, **metric_time.grain_tokens}
    for entry in entries:
        token = entry.token
        canonical = json.dumps([str(build_id), selected, token], separators=(",", ":"))
        option_id = hashlib.sha256(canonical.encode(strings.UTF8)).hexdigest()
        if option_id in mapping:
            continue
        label = entry.name or token
        if PATH_SEPARATOR in token and entry.name != METRIC_TIME:
            label += "（" + " → ".join(token.split(PATH_SEPARATOR)[:-1]) + "）"
        output.append(DimensionOption(
            option_id=option_id, name=entry.name, semantic_model_name=entry.semantic_model_name, display_name=label,
            dimension_type=entry.type, value_type=entry.value_type, granularities=list(entry.grain_tokens),
            operators=entry.operators if entry.operators is not None else native.allowed_filters,
        ))
        mapping[option_id] = DimensionBinding(token, entry.grain_tokens)
    return AvailableDimensions(build_id=str(build_id), metrics=selected, dimensions=output), mapping
