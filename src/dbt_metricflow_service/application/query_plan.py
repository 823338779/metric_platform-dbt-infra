"""将已校验资源身份和固定选项转换为引擎请求；无 I/O。"""

from typing import cast

from ..models.payloads import JsonObject, QueryOption
from ..models.queries import QueryMode, QueryRequest
from ..platform.models import PlatformQueryRequest
from .errors import ServiceError


def compile_query(
    request: QueryRequest,
    run_id: str,
    indexed: dict[str, JsonObject],
    options: list[QueryOption],
    mapping: dict[str, str],
) -> PlatformQueryRequest:
    if request.start_time and request.end_time and request.start_time > request.end_time:
        raise ServiceError("INVALID_TIME_RANGE", "startTime must not exceed endTime")
    if request.mode == QueryMode.PREVIEW:
        # QueryRequest 的模式校验保证预览请求含有数据集身份。
        resource = indexed.get(cast(str, request.dataset_resource_id))
        if not resource or "PREVIEW" not in resource["capabilities"]:
            raise ServiceError("INVALID_PREVIEW_RESOURCE", "resource cannot be previewed")
    option_index = {item["optionId"]: item for item in options}
    groups, filters = [], []
    for selected in request.group_by:
        option = option_index.get(selected.option_id)
        if not option or selected.grain and selected.grain not in option["granularities"]:
            raise ServiceError("INVALID_DIMENSION_OPTION", "option or grain belongs to another selection")
        groups.append(mapping[selected.option_id])
    for selected in request.filters:
        option = option_index.get(selected.option_id)
        if not option or selected.operator not in option["operators"]:
            raise ServiceError("INVALID_FILTER_OPTION", "filter does not belong to this selection")
        filters.append(
            {"field": mapping[selected.option_id], "operator": selected.operator, "value": selected.value}
        )
    if request.dimension_option_id and request.dimension_option_id not in mapping:
        raise ServiceError("INVALID_DIMENSION_OPTION", "dimension does not belong to this selection")
    metrics = sorted(set(request.metric_resource_ids))
    order_fields = {key: indexed[key]["name"] for key in metrics}
    order_fields.update({selection.option_id: mapping[selection.option_id] for selection in request.group_by})
    if any(order.field_id not in order_fields for order in request.order_by):
        raise ServiceError("INVALID_ORDER_FIELD", "order field was not selected")
    return PlatformQueryRequest.model_validate(
        {
            "run_id": run_id,
            "idempotency_key": request.idempotency_key,
            "mode": request.mode,
            "metrics": [indexed[key]["name"] for key in metrics],
            "group_by": groups,
            "filters": filters,
            "start_time": request.start_time,
            "end_time": request.end_time,
            "limit": request.limit,
            "dataset_resource_id": request.dataset_resource_id,
            "dimension": mapping.get(request.dimension_option_id)
            if request.dimension_option_id is not None
            else None,
            "order_by": [
                ("-" if order.direction == "DESC" else "") + order_fields[order.field_id]
                for order in request.order_by
            ],
        }
    )
