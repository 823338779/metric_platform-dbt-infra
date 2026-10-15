"""Independent dimension/data APIs keep MetricFlow semantics and error boundaries."""

from contextlib import nullcontext
from datetime import datetime
from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4

import pytest

from dbt_metricflow_service.platform import metricflow
from dbt_metricflow_service.platform.models import PlatformQueryRequest


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
    return engine, configuration, calls


def test_dimensions_have_a_dedicated_result_and_do_not_query_rows(tmp_path, engine_context):
    result = metricflow.execute_available_dimensions(tmp_path, tmp_path, ("orders",))
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
    result = metricflow.execute_programmatic(tmp_path, tmp_path, request)
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
        metricflow.execute_available_dimensions(tmp_path, tmp_path, ("missing",))
    with pytest.raises(ValueError) as failure:
        metricflow.execute_programmatic(tmp_path, tmp_path, PlatformQueryRequest(
            runId=uuid4(), mode="QUERY", metrics=["missing"],
        ))
    assert not isinstance(failure.value, metricflow.InvalidDimensionsRequest)


@pytest.mark.parametrize("operation", ["execute_available_dimensions", "execute_programmatic"])
def test_initialization_errors_are_not_reclassified(tmp_path, engine_context, operation):
    def fail(**kwargs):
        raise ValueError("connection initialization")
    engine_context[1].setup = fail
    argument = ("orders",) if operation == "execute_available_dimensions" else PlatformQueryRequest(
        runId=uuid4(), mode="QUERY", metrics=["orders"],
    )
    with pytest.raises(ValueError, match="connection initialization") as failure:
        getattr(metricflow, operation)(tmp_path, tmp_path, argument)
    assert not isinstance(failure.value, metricflow.InvalidDimensionsRequest)


def test_data_query_validates_dimensions_before_execution(tmp_path, engine_context):
    with pytest.raises(ValueError):
        metricflow.execute_programmatic(tmp_path, tmp_path, PlatformQueryRequest(
            runId=uuid4(), mode="QUERY", metrics=["orders"], groupBy=["unknown"],
        ))
    assert not any(kind == "query" for kind, _ in engine_context[2])


def test_missing_data_result_is_an_error(tmp_path, engine_context):
    engine_context[0].query = lambda _: SimpleNamespace(result_df=None)
    with pytest.raises(ValueError, match=metricflow.METRICFLOW_RESULT_MISSING_MESSAGE):
        metricflow.execute_programmatic(tmp_path, tmp_path, PlatformQueryRequest(
            runId=uuid4(), mode="QUERY", metrics=["orders"],
        ))


@pytest.mark.parametrize("rows,truncated", [([], False), ([(1,), (2,)], True)])
def test_preview_uses_adapter_and_preserves_empty_columns(tmp_path, monkeypatch, engine_context, rows, truncated):
    engine, configuration, calls = engine_context
    monkeypatch.setattr(metricflow, "catalog_from_artifacts", lambda _: SimpleNamespace(resources=[
        SimpleNamespace(resource_id="table.orders", kind="TABLE", relation='"fixed"."orders"'),
    ]))
    configuration.dbt_artifacts.adapter.connection_named = lambda _: nullcontext()

    def execute(sql, fetch):
        assert sql == 'select * from "fixed"."orders" limit 2'
        assert fetch is True
        class Table(list):
            column_names = ["orders"]
        table = Table(rows)
        table.rows = rows
        return None, table

    configuration.dbt_artifacts.adapter.execute = execute
    engine.list_metrics = lambda **_: pytest.fail("preview requested metric metadata")
    result = metricflow.execute_programmatic(tmp_path, tmp_path, PlatformQueryRequest(
        runId=uuid4(), mode="PREVIEW", datasetResourceId="table.orders", limit=1,
    ))
    assert result.rows == ([list(rows[0])] if rows else [])
    assert result.truncated is truncated
    assert result.columns[0].type == ("int" if rows else "unknown")


def test_probe_has_a_separate_entry_point(tmp_path, engine_context):
    result = metricflow.probe_query_capability(tmp_path, tmp_path)
    assert result.query_capability is True
    assert not hasattr(result, "rows")
    assert engine_context[2][-1][1].limit == 1


@pytest.mark.parametrize("operation", [
    "execute_available_dimensions", "execute_programmatic", "probe_query_capability",
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
    if operation == "execute_available_dimensions":
        metricflow.execute_available_dimensions(tmp_path, tmp_path, ("orders",))
    elif operation == "execute_programmatic":
        metricflow.execute_programmatic(tmp_path, tmp_path, PlatformQueryRequest(
            runId=uuid4(), mode="QUERY", metrics=["orders"],
        ))
    else:
        metricflow.probe_query_capability(tmp_path, tmp_path)
    assert clients == [client]


@pytest.mark.parametrize("missing_metrics", [True, False])
def test_probe_preserves_failure_and_missing_result_semantics(tmp_path, engine_context, missing_metrics):
    engine = engine_context[0]
    if missing_metrics:
        engine.list_metrics = lambda **_: []
        with pytest.raises(ValueError, match=metricflow.QUERYABLE_METRICS_MISSING_MESSAGE):
            metricflow.probe_query_capability(tmp_path, tmp_path)
    else:
        engine.query = lambda _: SimpleNamespace(result_df=None)
        assert metricflow.probe_query_capability(tmp_path, tmp_path).query_capability is False
