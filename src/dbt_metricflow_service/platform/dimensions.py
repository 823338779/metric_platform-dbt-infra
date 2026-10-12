"""由原生可用维度生成固定构建和指标集合的公开维度选项身份。"""

import hashlib
import json
from dataclasses import dataclass

from ..models.artifacts import ResourceKind
from ..models.payloads import AvailableDimensions, DimensionOption, JsonObject

UTF8 = "utf-8"
PATH_SEPARATOR = "__"
DISPLAY_SEPARATOR = " → "
GRAIN_TOKENS = "grainTokens"
METRIC_TIME = "metric_time"
TIME_TYPE = "time"
DATETIME_TYPE = "datetime"


@dataclass(frozen=True)
class DimensionBinding:
    """公开选项的私有执行绑定，不向客户端暴露原生路径。"""

    token: str  # 未指定粒度、过滤或取值时使用的默认原生字段。
    grain_tokens: dict[str, str]  # 引擎已核实的粒度到原生字段映射。


def map_available_dimensions(
    build_id: str, metric_ids: list[str], catalog: JsonObject, native: JsonObject
) -> tuple[AvailableDimensions, dict[str, DimensionBinding]]:
    indexed = {item["resourceId"]: item for item in catalog["resources"]}
    selected = sorted(set(metric_ids))
    output: list[DimensionOption] = []
    mapping: dict[str, DimensionBinding] = {}
    # 指标时间的各粒度归入基础选项，不再作为独立维度暴露。
    entries = [dict(entry) for entry in native["dimensions"]]
    time_tokens = {entry["granularity"]: entry["token"] for entry in native["timeDimensions"]}
    metric_time = next((entry for entry in entries if entry.get("name") == METRIC_TIME), None)
    if metric_time is None and time_tokens:
        metric_time = {
            "token": next(iter(time_tokens.values())), "name": METRIC_TIME,
            "type": TIME_TYPE, "valueType": DATETIME_TYPE, "operators": [],
        }
        entries.append(metric_time)
    if metric_time is not None:
        metric_time[GRAIN_TOKENS] = {**time_tokens, **metric_time.get(GRAIN_TOKENS, {})}
    # 原生路径只保存在服务映射内；每个 join 路径保持独立选项身份。
    for entry in entries:
        token = entry["token"]
        canonical = json.dumps([str(build_id), selected, token], separators=(",", ":"))
        option_id = hashlib.sha256(canonical.encode(UTF8)).hexdigest()
        if option_id in mapping:
            continue
        grain_tokens = entry.get(GRAIN_TOKENS, {})
        candidates = [
            item
            for item in indexed.values()
            if item["kind"] == ResourceKind.DIMENSION and item["name"] == entry.get("name")
        ]
        resource_id = candidates[0]["resourceId"] if len(candidates) == 1 else None
        label = candidates[0]["displayName"] if resource_id else entry.get("name", token)
        if PATH_SEPARATOR in token and entry.get("name") != METRIC_TIME:
            # 可见路径用于区分多种 join 选项；执行仍只接收不可伪造的选项映射。
            label += "（" + DISPLAY_SEPARATOR.join(token.split(PATH_SEPARATOR)[:-1]) + "）"
        output.append(
            {
                "optionId": option_id,
                "resourceId": resource_id,
                "displayName": label,
                "dimensionType": entry.get("type", "unknown"),
                "valueType": entry.get("valueType", "unknown"),
                "granularities": list(grain_tokens),
                "operators": entry.get("operators", native["allowedFilters"]),
            }
        )
        mapping[option_id] = DimensionBinding(token, grain_tokens)
    response: AvailableDimensions = {"buildId": str(build_id), "metricResourceIds": selected, "dimensions": output}
    return response, mapping
