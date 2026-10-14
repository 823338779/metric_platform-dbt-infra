"""公开资源及选项映射保留查询语义，不依赖数据库或引擎。"""

import pytest

from dbt_metricflow_service.application.errors import ServiceError
from dbt_metricflow_service.application.query_plan import compile_query
from dbt_metricflow_service.models.payloads import DimensionOption
from dbt_metricflow_service.models.queries import QueryRequest
from dbt_metricflow_service.platform.dimensions import DimensionBinding, map_available_dimensions
from dbt_metricflow_service.platform.models import DimensionQueryResult
from tests.catalog_helpers import catalog, resource

RUN = "00000000-0000-0000-0000-000000000001"
RESOURCES = {
    "metric.revenue": resource("metric.revenue", name="revenue"),
    "dataset.orders": resource("dataset.orders", capabilities=["PREVIEW"]),
}
DIMENSIONS = [DimensionOption.model_validate({
    "optionId": "region", "resourceId": None, "displayName": "Region",
    "dimensionType": "categorical", "valueType": "string", "granularities": [], "operators": ["IN"],
})]
MAPPING = {"region": DimensionBinding("order__region", {})}


@pytest.mark.parametrize("mode", ["QUERY", "EXPLAIN"])
def test_query_maps_selected_ids_filters_and_order(mode):
    request = QueryRequest(
        mode=mode, metricResourceIds=["metric.revenue", "metric.revenue"],
        groupBy=[{"optionId": "region"}],
        filters=[{"optionId": "region", "operator": "IN", "value": ["east", None]}],
        orderBy=[{"fieldId": "metric.revenue", "direction": "DESC"}, {"fieldId": "region", "direction": "ASC"}],
        startTime="2026-10-01T00:00:00", limit=12,
    )
    plan = compile_query(request, RUN, RESOURCES, DIMENSIONS, MAPPING).model_dump(mode="json", by_alias=True)
    assert plan == {
        "runId": RUN, "mode": mode, "metrics": ["revenue"],
        "groupBy": ["order__region"],
        "filters": [{"field": "order__region", "operator": "IN", "value": ["east", None]}],
        "orderBy": ["-revenue", "order__region"], "startTime": "2026-10-01T00:00:00", "endTime": None,
        "limit": 12, "datasetResourceId": None, "dimension": None,
    }


def test_preview_and_dimension_values_keep_their_fixed_selection():
    preview = QueryRequest(mode="PREVIEW", datasetResourceId="dataset.orders")
    plan = compile_query(preview, RUN, RESOURCES, [], {})
    assert plan.dataset_resource_id == "dataset.orders"
    assert plan.metrics == []
    values = QueryRequest(
        mode="DIMENSION_VALUES",
        metricResourceIds=["metric.revenue"], dimensionOptionId="region",
    )
    assert compile_query(values, RUN, RESOURCES, DIMENSIONS, MAPPING).dimension == "order__region"


@pytest.mark.parametrize(("extra", "code"), [
    ({"groupBy": [{"optionId": "region", "grain": "month"}]}, "INVALID_DIMENSION_OPTION"),
    ({"filters": [{"optionId": "region", "operator": "=", "value": "east"}]}, "INVALID_FILTER_OPTION"),
    ({"orderBy": [{"fieldId": "region", "direction": "ASC"}]}, "INVALID_ORDER_FIELD"),
    ({"startTime": "2026-10-02", "endTime": "2026-10-01"}, "INVALID_TIME_RANGE"),
])
def test_unavailable_selections_keep_public_errors(extra, code):
    request = QueryRequest(mode="QUERY", metricResourceIds=["metric.revenue"], **extra)
    with pytest.raises(ServiceError) as failure:
        compile_query(request, RUN, RESOURCES, DIMENSIONS, MAPPING)
    assert failure.value.error.code == code


@pytest.mark.parametrize("grain", [None, "day", "month"])
def test_base_time_option_changes_grouping_and_order_with_selected_grain(grain):
    native = {
        "dimensions": [{
            "token": "metric_time__day", "name": "metric_time", "type": "time",
            "grainTokens": {"day": "metric_time__day", "month": "metric_time__month"},
        }],
        "timeDimensions": [
            {"token": "metric_time__day", "granularity": "day"},
            {"token": "metric_time__month", "granularity": "month"},
        ],
        "allowedFilters": ["="],
    }
    available, mapping = map_available_dimensions(
        RUN, ["metric.revenue"], catalog(), DimensionQueryResult.model_validate(native)
    )
    assert len(available.dimensions) == 1
    option = available.dimensions[0]
    assert option.display_name == "metric_time"
    assert option.granularities == ["day", "month"]
    selection = {"optionId": option.option_id, **({"grain": grain} if grain else {})}
    request = QueryRequest(
        mode="QUERY", metricResourceIds=["metric.revenue"], groupBy=[selection],
        orderBy=[{"fieldId": option.option_id, "direction": "DESC"}],
    )
    plan = compile_query(request, RUN, RESOURCES, available.dimensions, mapping)
    expected = "metric_time__" + (grain or "day")
    assert plan.group_by == [expected]
    assert plan.order_by == ["-" + expected]
    # 服务未声明的粒度不能靠前端拼接绕过校验。
    request.group_by[0].grain = "hour"
    with pytest.raises(ServiceError) as failure:
        compile_query(request, RUN, RESOURCES, available.dimensions, mapping)
    assert failure.value.error.code == "INVALID_DIMENSION_OPTION"


def test_metric_time_grains_form_one_option_even_without_a_native_base_dimension():
    native = {
        "dimensions": [
            {"token": "order__region", "name": "region", "type": "categorical"},
            {"token": "return__region", "name": "region", "type": "categorical"},
        ],
        "timeDimensions": [
            {"token": "metric_time__day", "granularity": "day"},
            {"token": "metric_time__month", "granularity": "month"},
        ],
        "allowedFilters": ["="],
    }
    available, mapping = map_available_dimensions(
        RUN, ["metric.revenue"], catalog(), DimensionQueryResult.model_validate(native)
    )
    assert len(available.dimensions) == 3
    option = available.dimensions[-1]
    assert option.display_name == "metric_time"
    assert option.granularities == ["day", "month"]
    assert option.operators == []
    assert mapping[option.option_id].grain_tokens["month"] == "metric_time__month"
