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


def test_bindings_preserve_each_requested_metric_path():
    from dbt_metricflow_service.application.query_bindings import resolve_query_bindings

    per_metric = {metric: selection(metric, path) for metric, path in [
        ("sales", "buyer__country"), ("orders", "buyer__country"),
        ("refunds", "seller__country"), ("returns", "seller__country"),
    ]}
    body = body_for(per_metric)
    body["orderBy"] = [{"fieldId": "country", "direction": "ASC"}]
    plan = resolve_query_bindings(MetricQueryRequest.model_validate(body), {}, per_metric)
    assert {metric: path.token for metric, path in plan.bound_group_by[0].paths.items()} == {
        "sales": "buyer__country", "orders": "buyer__country",
        "refunds": "seller__country", "returns": "seller__country",
    }
    assert plan.order_by == ["country"]
    assert plan.bound_group_by[0].name == "country"


@pytest.mark.parametrize("wrong_selection", ["build", "metric"])
def test_option_from_wrong_build_or_metric_is_rejected(wrong_selection):
    from dbt_metricflow_service.application.errors import ServiceError
    from dbt_metricflow_service.application.query_bindings import resolve_query_bindings

    per_metric = {metric: selection(metric, "buyer__country") for metric in ["sales", "orders"]}
    body = body_for(per_metric)
    if wrong_selection == "metric":
        body["groupBy"][0]["bindings"]["orders"] = body["groupBy"][0]["bindings"]["sales"]
    else:
        per_metric["orders"] = resolve_dimensions("another-build", ["orders"], DimensionQueryResult(
            dimensions=[NativeDimension(token="buyer__country", type="categorical")],
        ))
    with pytest.raises(ServiceError) as caught:
        resolve_query_bindings(MetricQueryRequest.model_validate(body), {}, per_metric)
    assert caught.value.error.code == "INVALID_DIMENSION_OPTION"


@pytest.mark.parametrize("grains", [(None, None), ("day", "month")])
def test_time_alignment_requires_same_explicit_grain(grains):
    from dbt_metricflow_service.application.errors import ServiceError
    from dbt_metricflow_service.application.query_bindings import resolve_query_bindings

    per_metric = {metric: selection(metric, "order__ds", "time", {
        "day": "order__ds__day", "month": "order__ds__month",
    }) for metric in ["sales", "orders"]}
    body = body_for(per_metric)
    for metric, grain in zip(per_metric, grains, strict=True):
        body["groupBy"][0]["bindings"][metric]["grain"] = grain
    with pytest.raises(ServiceError):
        resolve_query_bindings(MetricQueryRequest.model_validate(body), {}, per_metric)


def test_resolver_preserves_mixed_order_shared_sort_and_filters():
    from dbt_metricflow_service.application.query_bindings import resolve_query_bindings

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
    shared_id = next(iter(common))
    body["groupBy"].insert(1, {"optionId": shared_id})
    body["orderBy"] = [{"fieldId": shared_id, "direction": "DESC"}]
    body["filters"] = [{"optionId": next(iter(common)), "operator": "=", "value": "paid"}]
    plan = resolve_query_bindings(MetricQueryRequest.model_validate(body), common, per_metric)
    assert [item.name for item in plan.bound_group_by] == ["country", None, "region"]
    assert [{metric: path.token for metric, path in item.paths.items()} for item in plan.bound_group_by] == [
        {"sales": "buyer__country", "orders": "buyer__country"},
        {"sales": "order__status", "orders": "order__status"},
        {"sales": "buyer__region", "orders": "seller__region"},
    ]
    assert plan.order_by == ["-order__status"]
    assert plan.filters[0].field == "order__status" and plan.filters[0].value == "paid"


def test_alignment_rejects_mixed_dimension_types():
    from dbt_metricflow_service.application.errors import ServiceError
    from dbt_metricflow_service.application.query_bindings import resolve_query_bindings

    per_metric = {"sales": selection("sales", "buyer__country"),
                  "orders": selection("orders", "order__ds", "time", {"day": "order__ds__day"})}
    body = body_for(per_metric)
    body["groupBy"][0]["bindings"]["orders"]["grain"] = "day"
    with pytest.raises(ServiceError):
        resolve_query_bindings(MetricQueryRequest.model_validate(body), {}, per_metric)
