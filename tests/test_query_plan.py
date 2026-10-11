"""公开资源及选项映射保留查询语义，不依赖数据库或引擎。"""

import pytest

from dbt_metricflow_service.application.errors import ServiceError
from dbt_metricflow_service.application.query_plan import compile_query
from dbt_metricflow_service.models.queries import QueryRequest

RUN = "00000000-0000-0000-0000-000000000001"
RESOURCES = {
    "metric.revenue": {"name": "revenue"},
    "dataset.orders": {"capabilities": ["PREVIEW"]},
}
OPTIONS = [{
    "optionId": "region", "resourceId": None, "displayName": "Region",
    "dimensionType": "categorical", "valueType": "string", "granularities": [], "operators": ["IN"],
}]
MAPPING = {"region": "order__region"}


@pytest.mark.parametrize("mode", ["QUERY", "EXPLAIN"])
def test_query_maps_selected_ids_filters_and_order(mode):
    request = QueryRequest(
        idempotencyKey="query", mode=mode, metricResourceIds=["metric.revenue", "metric.revenue"],
        groupBy=[{"optionId": "region"}],
        filters=[{"optionId": "region", "operator": "IN", "value": ["east", None]}],
        orderBy=[{"fieldId": "metric.revenue", "direction": "DESC"}, {"fieldId": "region", "direction": "ASC"}],
        startTime="2026-10-01T00:00:00", limit=12,
    )
    plan = compile_query(request, RUN, RESOURCES, OPTIONS, MAPPING).model_dump(mode="json", by_alias=True)
    assert plan == {
        "runId": RUN, "idempotencyKey": "query", "mode": mode, "metrics": ["revenue"],
        "groupBy": ["order__region"],
        "filters": [{"field": "order__region", "operator": "IN", "value": ["east", None]}],
        "orderBy": ["-revenue", "order__region"], "startTime": "2026-10-01T00:00:00", "endTime": None,
        "limit": 12, "datasetResourceId": None, "dimension": None,
    }


def test_preview_and_dimension_values_keep_their_fixed_selection():
    preview = QueryRequest(idempotencyKey="preview", mode="PREVIEW", datasetResourceId="dataset.orders")
    plan = compile_query(preview, RUN, RESOURCES, [], {})
    assert plan.dataset_resource_id == "dataset.orders"
    assert plan.metrics == []
    values = QueryRequest(
        idempotencyKey="values", mode="DIMENSION_VALUES",
        metricResourceIds=["metric.revenue"], dimensionOptionId="region",
    )
    assert compile_query(values, RUN, RESOURCES, OPTIONS, MAPPING).dimension == "order__region"


@pytest.mark.parametrize(("extra", "code"), [
    ({"groupBy": [{"optionId": "region", "grain": "month"}]}, "INVALID_DIMENSION_OPTION"),
    ({"filters": [{"optionId": "region", "operator": "=", "value": "east"}]}, "INVALID_FILTER_OPTION"),
    ({"orderBy": [{"fieldId": "region", "direction": "ASC"}]}, "INVALID_ORDER_FIELD"),
    ({"startTime": "2026-10-02", "endTime": "2026-10-01"}, "INVALID_TIME_RANGE"),
])
def test_unavailable_selections_keep_public_errors(extra, code):
    request = QueryRequest(idempotencyKey="invalid", mode="QUERY", metricResourceIds=["metric.revenue"], **extra)
    with pytest.raises(ServiceError) as failure:
        compile_query(request, RUN, RESOURCES, OPTIONS, MAPPING)
    assert failure.value.error.code == code
