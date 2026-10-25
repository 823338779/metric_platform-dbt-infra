"""Independent dimension/data APIs keep MetricFlow semantics and error boundaries."""

from datetime import datetime
from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4

import pytest

from dbt_metricflow_service.delegates.clients import dbt_client
from dbt_metricflow_service.delegates.clients import metric_flow_client as metricflow
from dbt_metricflow_service.delegates.clients.dbt_client import DbtClient
from dbt_metricflow_service.platform.models import PlatformQueryRequest


@pytest.mark.parametrize("field,expression", [
    ("consumer__dima", "{{ Dimension('consumer__dima') }} = 'NORTH'"),
    ("account__consumer__dima", "{{ Dimension('consumer__dima', entity_path=['account']) }} = 'NORTH'"),
    ("store__account__consumer__dima", "{{ Dimension('consumer__dima', entity_path=['store', 'account']) }} = 'NORTH'"),
    ("account__consumer__created_at__day",
     "{{ Dimension('consumer__created_at__day', entity_path=['account']) }} = 'NORTH'"),
])
def test_filter_preserves_role_path_in_native_dimension_expression(field, expression):
    """筛选的多跳角色须通过 entity_path 传给 MetricFlow，不能只使用分组 token。"""
    request = PlatformQueryRequest(
        runId=uuid4(), mode="QUERY", metrics=["orders"],
        filters=[{"field": field, "operator": "=", "value": "NORTH"}],
    )
    assert metricflow._where_constraints(request) == [expression]


@pytest.fixture
def engine_context(monkeypatch):
    calls = []
    table = SimpleNamespace(
        column_descriptions=[SimpleNamespace(column_name="orders", column_type=Decimal)],
        rows=[(Decimal("1.25"),), (Decimal("2.50"),)],
    )

    def explain(request):
        calls.append(("explain", request))
        return SimpleNamespace(sql_statement=SimpleNamespace(sql="select orders"))

    def query(request):
        calls.append(("query", request))
        return SimpleNamespace(result_df=table)

    engine = SimpleNamespace(
        list_metrics=lambda **_: [SimpleNamespace(name="orders")],
        list_dimensions=lambda **_: [
            SimpleNamespace(
                dunder_name="order__region", name="region", type="categorical", semantic_model_reference=None,
            ),
        ],
        explain=explain, query=query,
    )
    adapter = SimpleNamespace(type=lambda: "postgres")
    configuration = SimpleNamespace(
        setup=lambda **kwargs: calls.append(("setup", kwargs)), mf=engine,
        dbt_artifacts=SimpleNamespace(adapter=adapter),
        semantic_manifest_lookup=SimpleNamespace(
            value_mapping_lookup=SimpleNamespace(mapping_model_references=frozenset()),
        ),
    )
    monkeypatch.setattr(metricflow, "CLIConfiguration", lambda: configuration)
    monkeypatch.setattr(DbtClient, "write_semantic_manifest", lambda *_: None)
    return engine, configuration, calls


def test_dimensions_have_a_dedicated_result_and_do_not_query_rows(tmp_path, engine_context):
    client = metricflow.MetricFlowClient(b"manifest", project=tmp_path, profiles=tmp_path)
    result = client.available_dimensions(("orders",))
    assert result.metrics[0].name == "orders"
    assert result.dimensions[0].token == "order__region"
    assert result.time_dimensions
    assert not hasattr(result, "rows")
    assert not any(kind == "query" for kind, _ in engine_context[2])


@pytest.mark.parametrize("mode", ["QUERY", "DIMENSION_VALUES", "EXPLAIN"])
def test_data_request_preserves_filters_time_order_and_limit(tmp_path, engine_context, mode):
    request = PlatformQueryRequest(
        runId=uuid4(), mode=mode, metrics=["orders"], groupBy=["order__region"],
        dimension="order__region", limit=1, orderBy=["-orders"],
        startTime=datetime(2026, 1, 1), endTime=datetime(2026, 2, 1),
        filters=[{"field": "order__region", "operator": "IN", "value": ["O'Reilly", "East"]}],
    )
    result = metricflow.MetricFlowClient(b"manifest", project=tmp_path, profiles=tmp_path).query(request)
    assert not hasattr(result, "dimensions")
    kind, native = engine_context[2][-1]
    assert kind == ("explain" if mode == "EXPLAIN" else "query")
    assert list(native.metric_names) == ["orders"]
    assert list(native.group_by_names) == ["order__region"]
    assert native.limit == 2
    assert native.time_constraint_start == request.start_time
    assert native.time_constraint_end == request.end_time
    assert list(native.order_by_names) == ["-orders"]
    assert list(native.where_constraints) == ["{{ Dimension('order__region') }} IN ('O''Reilly', 'East')"]
    assert native.query_type == (
        metricflow.MetricFlowQueryType.DIMENSION_VALUES if mode == "DIMENSION_VALUES"
        else metricflow.MetricFlowQueryType.METRIC
    )
    if mode == "EXPLAIN":
        assert result.sql == "select orders"
        assert not any(kind == "query" for kind, _ in engine_context[2])
    else:
        assert result.rows == [["1.25"]]
        assert result.truncated is True


def test_only_dimension_selection_errors_are_business_errors(tmp_path, engine_context):
    with pytest.raises(metricflow.InvalidDimensionsRequest):
        metricflow.MetricFlowClient(b"manifest", project=tmp_path, profiles=tmp_path).available_dimensions(("missing",))



@pytest.mark.parametrize("operation", ["available_dimensions", "query"])
def test_initialization_errors_are_not_reclassified(tmp_path, engine_context, operation):
    def fail(**kwargs):
        raise ValueError("connection initialization")
    engine_context[1].setup = fail
    argument = ("orders",) if operation == "available_dimensions" else PlatformQueryRequest(
        runId=uuid4(), mode="QUERY", metrics=["orders"],
    )
    with pytest.raises(ValueError, match="connection initialization") as failure:
        getattr(metricflow.MetricFlowClient(b"manifest", project=tmp_path, profiles=tmp_path), operation)(argument)
    assert not isinstance(failure.value, metricflow.InvalidDimensionsRequest)


@pytest.mark.parametrize("mode", ["QUERY", "EXPLAIN", "DIMENSION_VALUES"])
def test_query_goes_directly_to_engine_without_metadata_preflight(tmp_path, engine_context, mode):
    engine, _, calls = engine_context

    def forbid(**kwargs):
        pytest.fail("query must not discover metrics or dimensions before execution")

    engine.list_metrics = engine.list_dimensions = forbid
    result = metricflow.MetricFlowClient(b"manifest", project=tmp_path, profiles=tmp_path).query(PlatformQueryRequest(
        runId=uuid4(), mode=mode, metrics=["unknown_metric"], groupBy=["unknown_dimension"],
        dimension="unknown_dimension", orderBy=["-unknown_dimension"],
    ))
    kind, native = calls[-1]
    assert kind == ("explain" if mode == "EXPLAIN" else "query")
    assert list(native.metric_names) == ["unknown_metric"]
    assert list(native.group_by_names) == ["unknown_dimension"]
    assert list(native.order_by_names) == ["-unknown_dimension"]
    assert sum(kind in {"query", "explain"} for kind, _ in calls) == 1
    assert result.sql if mode == "EXPLAIN" else result.rows


def test_missing_data_result_is_an_error(tmp_path, engine_context):
    engine_context[0].query = lambda _: SimpleNamespace(result_df=None)
    with pytest.raises(ValueError, match=metricflow.METRICFLOW_RESULT_MISSING_MESSAGE):
        metricflow.MetricFlowClient(b"manifest", project=tmp_path, profiles=tmp_path).query(PlatformQueryRequest(
            runId=uuid4(), mode="QUERY", metrics=["orders"],
        ))


@pytest.mark.parametrize("rows,truncated", [([], False), ([(1,), (2,)], True)])
def test_preview_uses_dbt_show_and_preserves_empty_columns(tmp_path, monkeypatch, engine_context, rows, truncated):
    engine, configuration, calls = engine_context
    monkeypatch.setattr(DbtClient, "_manifest", property(lambda _: "native manifest"))
    class Table(list):
        column_names = ["orders"]
    table = Table(rows)
    table.rows = rows
    def invoke(args):
        assert args[0] == "show"
        assert args[args.index("--limit") + 1] == "2"
        assert "resource_type:seed" in args
        return SimpleNamespace(success=True, result=SimpleNamespace(results=[SimpleNamespace(agate_table=table)]))
    def runner(*, manifest, callbacks):
        assert manifest == "native manifest"
        return SimpleNamespace(invoke=invoke)
    monkeypatch.setattr(dbt_client, "dbtRunner", runner)
    engine.list_metrics = lambda **_: pytest.fail("preview requested metric metadata")
    result = DbtClient(b"manifest").preview("table.orders", project=tmp_path, profiles=tmp_path, limit=1)
    assert result.rows == ([list(rows[0])] if rows else [])
    assert result.truncated is truncated
    assert result.columns[0].type == ("int" if rows else "unknown")


@pytest.mark.parametrize("operation", [
    "available_dimensions", "query",
])
def test_starrocks_uses_adapter_backed_engine(tmp_path, monkeypatch, engine_context, operation):
    engine, configuration, _ = engine_context
    adapter = configuration.dbt_artifacts.adapter
    adapter.type = lambda: "starrocks"
    client = object()
    clients = []

    def sql_client(selected):
        assert selected is adapter
        return client

    def create_engine(*, semantic_manifest_lookup, sql_client):
        assert semantic_manifest_lookup is configuration.semantic_manifest_lookup
        clients.append(sql_client)
        return engine

    configuration.mf = None
    monkeypatch.setattr(metricflow, "StarRocksSqlClient", sql_client)
    monkeypatch.setattr(metricflow, "MetricFlowEngine", create_engine)
    if operation == "available_dimensions":
        metricflow.MetricFlowClient(b"manifest", project=tmp_path, profiles=tmp_path).available_dimensions(("orders",))
    elif operation == "query":
        metricflow.MetricFlowClient(b"manifest", project=tmp_path, profiles=tmp_path).query(PlatformQueryRequest(
            runId=uuid4(), mode="QUERY", metrics=["orders"],
        ))
    assert clients == [client]


@pytest.mark.parametrize("mode", ["QUERY", "EXPLAIN"])
def test_native_query_errors_keep_business_error_classification(tmp_path, engine_context, mode):
    from metricflow_semantics.errors.error_classes import InvalidQueryException

    calls = []
    failure = InvalidQueryException("unknown native metric")

    def reject(request):
        calls.append(request)
        raise failure

    engine_context[0].query = engine_context[0].explain = reject
    with pytest.raises(metricflow.InvalidDimensionsRequest) as caught:
        metricflow.MetricFlowClient(b"manifest", project=tmp_path, profiles=tmp_path).query(PlatformQueryRequest(
            runId=uuid4(), mode=mode, metrics=["missing"]))
    assert caught.value.__cause__ is failure
    assert len(calls) == 1
