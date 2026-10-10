"""由原生选项生成固定构建和指标集合的公开选项身份。"""

import hashlib
import json

from ..models.artifacts import ResourceKind
from ..models.payloads import JsonObject, QueryOption, QueryOptions

UTF8 = "utf-8"
PATH_SEPARATOR = "__"
DISPLAY_SEPARATOR = " → "
GRAIN_LABELS = {
    "second": "秒",
    "minute": "分钟",
    "hour": "小时",
    "day": "日",
    "week": "周",
    "month": "月",
    "quarter": "季度",
    "year": "年",
}


def map_options(
    build_id: str, metric_ids: list[str], catalog: JsonObject, native: JsonObject
) -> tuple[QueryOptions, dict[str, str]]:
    indexed = {item["resourceId"]: item for item in catalog["resources"]}
    selected = sorted(set(metric_ids))
    output: list[QueryOption] = []
    mapping: dict[str, str] = {}
    # 原生路径只保存在服务映射内；每个 join 路径保持独立选项身份。
    for entry in [*native["dimensions"], *native["timeDimensions"]]:
        token = entry["token"]
        canonical = json.dumps([str(build_id), selected, token], separators=(",", ":"))
        option_id = hashlib.sha256(canonical.encode(UTF8)).hexdigest()
        if option_id in mapping:
            continue
        grain = entry.get("granularity")
        candidates = [
            item
            for item in indexed.values()
            if item["kind"] == ResourceKind.DIMENSION and item["name"] == entry.get("name")
        ]
        resource_id = candidates[0]["resourceId"] if len(candidates) == 1 and not grain else None
        label = candidates[0]["displayName"] if resource_id else entry.get("name", token)
        if grain:
            label = "指标时间（" + GRAIN_LABELS.get(grain, grain) + "）"
        elif PATH_SEPARATOR in token:
            # 可见路径用于区分多种 join 选项；执行仍只接收不可伪造的选项映射。
            label += "（" + DISPLAY_SEPARATOR.join(token.split(PATH_SEPARATOR)[:-1]) + "）"
        output.append(
            {
                "optionId": option_id,
                "resourceId": resource_id,
                "displayName": label,
                "dimensionType": "time" if grain else entry.get("type", "unknown"),
                "valueType": "datetime" if grain else entry.get("valueType", "unknown"),
                "granularities": [grain] if grain else [],
                "operators": [] if grain else native["allowedFilters"],
            }
        )
        mapping[option_id] = token
    response: QueryOptions = {"buildId": str(build_id), "metricResourceIds": selected, "options": output}
    return response, mapping
