"""将已校验资源身份和固定选项转换为引擎请求；无 I/O。"""

from typing import Final, cast

from dbt_metricflow_service import constants as strings

from ..models.payloads import DimensionOption, JsonObject
from ..models.queries import QueryMode, QueryRequest
from ..platform.dimensions import DimensionBinding
from ..platform.models import PlatformQueryRequest, QueryFilter
from .errors import ServiceError

DIMENSION_SELECTION_CONFLICT_MESSAGE: Final = "dimension does not belong to this selection"
FILTER_SELECTION_CONFLICT_MESSAGE: Final = "filter does not belong to this selection"
INVALID_TIME_RANGE_MESSAGE: Final = "startTime must not exceed endTime"
OPTION_SELECTION_CONFLICT_MESSAGE: Final = "option or grain belongs to another selection"
UNSELECTED_ORDER_FIELD_MESSAGE: Final = "order field was not selected"
UNSUPPORTED_RESOURCE_PREVIEW_MESSAGE: Final = "resource cannot be previewed"

PREVIEW_TYPES = frozenset({strings.DBT_MODEL_RESOURCE, strings.DBT_SEED_RESOURCE,
                           strings.DBT_SNAPSHOT_RESOURCE, strings.DBT_SOURCE_RESOURCE})


def compile_query(
    request: QueryRequest,
    run_id: str,
    indexed: dict[str, JsonObject],
    dimensions: list[DimensionOption],
    mapping: dict[str, DimensionBinding],
) -> PlatformQueryRequest:
    if request.start_time and request.end_time and request.start_time > request.end_time:
        raise ServiceError(strings.INVALID_TIME_RANGE, INVALID_TIME_RANGE_MESSAGE)
    if request.mode == QueryMode.PREVIEW:
        # QueryRequest 的模式校验保证预览请求含有数据集身份。
        resource = indexed.get(cast(str, request.dataset_resource_id))
        if (not resource or resource.get(strings.DBT_RESOURCE_TYPE_FIELD) not in PREVIEW_TYPES
                or cast(JsonObject, resource.get(strings.JINJA_CONFIG_FUNCTION, {})).get(strings.DBT_MATERIALIZED_FIELD)
                == strings.DBT_EPHEMERAL_MATERIALIZATION):
            raise ServiceError(strings.INVALID_PREVIEW_RESOURCE, UNSUPPORTED_RESOURCE_PREVIEW_MESSAGE)
    option_index = {item.option_id: item for item in dimensions}
    groups, filters = [], []
    for selected in request.group_by:
        option = option_index.get(selected.option_id)
        if not option or selected.grain and selected.grain not in option.granularities:
            raise ServiceError(strings.INVALID_DIMENSION_OPTION, OPTION_SELECTION_CONFLICT_MESSAGE)
        # 粒度只能解析到服务验证过的字段；排序必须复用同一个解析结果。
        binding = mapping[selected.option_id]
        groups.append(binding.grain_tokens[selected.grain] if selected.grain else binding.token)
    for selected in request.filters:
        option = option_index.get(selected.option_id)
        if not option or selected.operator not in option.operators:
            raise ServiceError(strings.INVALID_FILTER_OPTION, FILTER_SELECTION_CONFLICT_MESSAGE)
        filters.append(
            QueryFilter(field=mapping[selected.option_id].token, operator=selected.operator, value=selected.value)
        )
    if request.dimension_option_id and request.dimension_option_id not in mapping:
        raise ServiceError(strings.INVALID_DIMENSION_OPTION, DIMENSION_SELECTION_CONFLICT_MESSAGE)
    metrics = sorted(set(request.metric_resource_ids))
    order_fields = {key: cast(str, indexed[key][strings.NAME_FIELD]) for key in metrics}
    order_fields.update({
        selection.option_id: token for selection, token in zip(request.group_by, groups, strict=True)
    })
    if any(order.field_id not in order_fields for order in request.order_by):
        raise ServiceError(strings.INVALID_ORDER_FIELD, UNSELECTED_ORDER_FIELD_MESSAGE)
    return PlatformQueryRequest(
        run_id=run_id, mode=request.mode, metrics=[cast(str, indexed[key][strings.NAME_FIELD]) for key in metrics],
        group_by=groups, filters=filters, start_time=request.start_time, end_time=request.end_time,
        limit=request.limit, dataset_resource_id=request.dataset_resource_id,
        dimension=mapping[request.dimension_option_id].token if request.dimension_option_id is not None else None,
        order_by=[("-" if order.direction == strings.DESC else "") + order_fields[order.field_id]
                  for order in request.order_by],
    )
