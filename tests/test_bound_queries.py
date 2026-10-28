import pytest
from pydantic import ValidationError

from dbt_metricflow_service.application.query_plan import resolve_dimensions
from dbt_metricflow_service.engines.models import DimensionQueryResult, NativeDimension
from dbt_metricflow_service.models.queries import MetricQueryRequest

BUILD = "00000000-0000-0000-0000-000000000001"


def selection(metric, token, kind="categorical", grains=None):
    return resolve_dimensions(BUILD, [metric], DimensionQueryResult(dimensions=[
        NativeDimension(token=token, name=token.split("__")[-1], type=kind,
                        grain_tokens=grains or {}, operators=["="]),
    ]))


def body_for(per_metric):
    return {
        "mode": "QUERY", "metrics": list(per_metric),
        "groupBy": [{"name": "country", "bindings": {
            metric: {"optionId": next(iter(options))} for metric, options in per_metric.items()
        }}],
    }


def test_binding_contract_accepts_per_metric_paths():
    request = MetricQueryRequest.model_validate(body_for({
        "sales": selection("sales", "buyer__country"),
        "refunds": selection("refunds", "seller__country"),
    }))
    assert request.group_by[0].name == "country"


@pytest.mark.parametrize("change", ["missing", "extra", "collision", "duplicate", "unsafe"])
def test_binding_contract_rejects_ambiguous_alignment(change):
    body = body_for({"sales": selection("sales", "buyer__country")})
    group = body["groupBy"][0]
    if change == "missing":
        group["bindings"] = {}
    elif change == "extra":
        group["bindings"]["refunds"] = {"optionId": "x"}
    elif change == "collision":
        group["name"] = "sales"
    elif change == "duplicate":
        body["groupBy"].append(group)
    else:
        group["name"] = 'country"; select 1'
    with pytest.raises(ValidationError):
        MetricQueryRequest.model_validate(body)


def test_equal_full_paths_share_queries_despite_different_option_ids():
    from dbt_metricflow_service.application.bound_query_plan import compile_bound_query

    per_metric = {metric: selection(metric, path) for metric, path in [
        ("sales", "buyer__country"), ("orders", "buyer__country"),
        ("refunds", "seller__country"), ("returns", "seller__country"),
    ]}
    body = body_for(per_metric)
    body["orderBy"] = [{"fieldId": "country", "direction": "ASC"}]
    plan = compile_bound_query(MetricQueryRequest.model_validate(body), {}, per_metric)
    assert [group.query.metrics for group in plan.groups] == [["sales", "orders"], ["refunds", "returns"]]
    assert plan.order_by == ["country"]
    assert plan.group_by == ["country"]


def test_binding_option_cannot_be_borrowed_from_another_metric():
    from dbt_metricflow_service.application.bound_query_plan import compile_bound_query
    from dbt_metricflow_service.application.errors import ServiceError

    per_metric = {metric: selection(metric, "buyer__country") for metric in ["sales", "orders"]}
    body = body_for(per_metric)
    body["groupBy"][0]["bindings"]["orders"] = body["groupBy"][0]["bindings"]["sales"]
    with pytest.raises(ServiceError) as caught:
        compile_bound_query(MetricQueryRequest.model_validate(body), {}, per_metric)
    assert caught.value.error.code == "INVALID_DIMENSION_OPTION"


@pytest.mark.parametrize("grains", [(None, None), ("day", "month")])
def test_time_alignment_requires_same_explicit_grain(grains):
    from dbt_metricflow_service.application.bound_query_plan import compile_bound_query
    from dbt_metricflow_service.application.errors import ServiceError

    per_metric = {metric: selection(metric, "order__ds", "time", {
        "day": "order__ds__day", "month": "order__ds__month",
    }) for metric in ["sales", "orders"]}
    body = body_for(per_metric)
    for metric, grain in zip(per_metric, grains, strict=True):
        body["groupBy"][0]["bindings"][metric]["grain"] = grain
    with pytest.raises(ServiceError):
        compile_bound_query(MetricQueryRequest.model_validate(body), {}, per_metric)


def test_grouping_compares_every_dimension_and_preserves_shared_filters():
    from dbt_metricflow_service.application.bound_query_plan import compile_bound_query

    per_metric = {metric: selection(metric, "buyer__country") for metric in ["sales", "orders"]}
    body = body_for(per_metric)
    region = {}
    for metric, token in [("sales", "buyer__region"), ("orders", "seller__region")]:
        options = selection(metric, token)
        per_metric[metric].update(options)
        region[metric] = {"optionId": next(iter(options))}
    body["groupBy"].append({"name": "region", "bindings": region})
    common = resolve_dimensions(BUILD, ["sales", "orders"], DimensionQueryResult(dimensions=[
        NativeDimension(token="order__status", name="status", type="categorical", operators=["="]),
    ]))
    body["filters"] = [{"optionId": next(iter(common)), "operator": "=", "value": "paid"}]
    plan = compile_bound_query(MetricQueryRequest.model_validate(body), common, per_metric)
    assert len(plan.groups) == 2
    assert [group.query.group_by for group in plan.groups] == [
        ["buyer__country", "buyer__region"], ["buyer__country", "seller__region"],
    ]
    assert all(group.query.filters[0].field == "order__status" and group.query.filters[0].value == "paid"
               for group in plan.groups)


def test_alignment_rejects_mixed_dimension_types():
    from dbt_metricflow_service.application.bound_query_plan import compile_bound_query
    from dbt_metricflow_service.application.errors import ServiceError

    per_metric = {"sales": selection("sales", "buyer__country"),
                  "orders": selection("orders", "order__ds", "time", {"day": "order__ds__day"})}
    body = body_for(per_metric)
    body["groupBy"][0]["bindings"]["orders"]["grain"] = "day"
    with pytest.raises(ServiceError):
        compile_bound_query(MetricQueryRequest.model_validate(body), {}, per_metric)
