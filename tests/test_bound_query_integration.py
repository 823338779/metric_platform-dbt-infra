from contextlib import contextmanager
from datetime import datetime
from types import SimpleNamespace

import pytest
import yaml
from metricflow.dataflow.optimizer.dataflow_optimizer_factory import DataflowPlanOptimization
from metricflow.engine.metricflow_engine import MetricFlowEngine, MetricFlowQueryRequest
from metricflow.sql.optimizer.optimization_levels import SqlOptimizationLevel
from metricflow_semantics.specs.bound_group_by_parameter import BoundGroupByParameter, MetricGroupByBinding
from metricflow_semantics.specs.query_param_implementations import DimensionOrEntityParameter, TimeDimensionParameter
from test_sdk_integration import write_project

from dbt_metricflow_service.application.query_bindings import resolve_query_bindings
from dbt_metricflow_service.application.query_plan import resolve_dimensions
from dbt_metricflow_service.engines.dbt import DbtClient
from dbt_metricflow_service.engines.metricflow import MetricFlowClient
from dbt_metricflow_service.engines.planning import PlanningSqlClient
from dbt_metricflow_service.engines.results import serialize_rows
from dbt_metricflow_service.execution.query import QueryExecutor
from dbt_metricflow_service.models.queries import MetricQueryRequest, QueryMode
from dbt_metricflow_service.platform.manifest_capture import ManifestCapture

BUILD = "00000000-0000-0000-0000-000000000001"


def build_warehouse(project, mapped=False):
    write_project(project)
    (project / "models/orders.sql").write_text("""
select * from (values
 (1, 1, 2, timestamp '2024-01-01', 10, 1),
 (2, 1, 3, timestamp '2024-01-02', 20, 2),
 (3, 4, 4, timestamp '2024-02-01', 30, 3),
 (4, 2, 5, timestamp '2024-02-02', 40, 4)
) t(id, buyer_id, seller_id, ds, amount, refund)
""", encoding="utf-8")
    (project / "models/people.sql").write_text("""
select * from (values (1, 'CN'), (2, 'US'), (3, 'JP'), (4, NULL), (5, 'DE')) t(id, country)
""", encoding="utf-8")
    (project / "models/refunds.sql").write_text("""
select * from (values
 (1, 1, 2, timestamp '2024-01-01', 1),
 (2, 1, 3, timestamp '2024-01-02', 2),
 (3, 4, 4, timestamp '2024-02-01', 3),
 (4, 2, 5, timestamp '2024-02-02', 4)
) t(id, buyer_id, seller_id, ds, amount)
""", encoding="utf-8")
    path = project / "models/semantic.yml"
    semantic = yaml.safe_load(path.read_text(encoding="utf-8"))
    orders = semantic["semantic_models"][0]
    orders["dimensions"] = [orders["dimensions"][0]]
    for role in ["buyer", "seller"]:
        orders["entities"].append({"name": role, "type": "foreign", "expr": f"{role}_id"})
        semantic["semantic_models"].append({
            "name": role, "model": "ref('people')",
            "entities": [{"name": role, "type": "primary", "expr": "id"}],
            "dimensions": [{"name": "country", "type": "categorical"}],
        })
    refunds = {
        "name": "refunds", "model": "ref('refunds')", "defaults": {"agg_time_dimension": "ds"},
        "entities": [{"name": "refund", "type": "primary", "expr": "id"}, *orders["entities"][1:]],
        "dimensions": orders["dimensions"], "measures": [],
    }
    semantic["semantic_models"].append(refunds)
    for model, metric, expr in [(orders, "orders_count", "1"), (refunds, "refunds", "amount"),
                                (refunds, "returns_count", "1")]:
        model["measures"].append({"name": metric, "agg": "sum", "expr": expr})
        semantic["metrics"].append({"name": metric, "label": metric, "type": "simple",
                                    "type_params": {"measure": metric}})
    semantic["metrics"].extend([
        {"name": "double_revenue", "label": "Double revenue", "type": "derived",
         "type_params": {"expr": "revenue * 2", "metrics": [{"name": "revenue"}]}},
        {"name": "average_revenue", "label": "Average revenue", "type": "ratio",
         "type_params": {"numerator": "revenue", "denominator": "orders_count"}},
        {"name": "net_revenue", "label": "Net revenue", "type": "derived",
         "type_params": {"expr": "revenue - refunds", "metrics": [{"name": "revenue"}, {"name": "refunds"}]}},
    ])
    if mapped:
        (project / "models/country_labels.sql").write_text(
            "select * from (values ('CN', 'same label'), ('US', 'same label')) t(code, label)", encoding="utf-8")
        semantic["semantic_models"].append({
            "name": "country_labels", "model": "ref('country_labels')",
            "entities": [{"name": "country_label", "type": "primary", "expr": "code"}],
            "dimensions": [{"name": "code", "type": "categorical"}, {"name": "label", "type": "categorical"}],
        })
        for model in semantic["semantic_models"][1:3]:
            model["dimensions"][0]["value_mapping"] = {
                "semantic_model": "country_labels", "value_dimension": "code", "display_dimension": "label",
            }
    path.write_text(yaml.safe_dump(semantic), encoding="utf-8")
    (project / "profiles.yml").write_text(
        "execution:\n  target: test\n  outputs:\n    test:\n      type: duckdb\n"
        f"      path: '{(project / 'warehouse.duckdb').as_posix()}'\n", encoding="utf-8")
    result = DbtClient().build(project, project)
    assert result.success, result.exception
    capture = ManifestCapture(64 * 1024 * 1024)
    capture(result.manifest)
    return project, capture.content


@pytest.fixture(scope="module")
def warehouse(tmp_path_factory):
    return build_warehouse(tmp_path_factory.mktemp("bound-query"))


def prepare(warehouse, same_path=False, time=False, metric_paths=None):
    project, content = warehouse
    client = MetricFlowClient(content, project=project, profiles=project)
    metric_paths = metric_paths or {
        "revenue": "buyer__country", "orders_count": "buyer__country",
        "refunds": "seller__country", "returns_count": "seller__country",
    }
    metrics = list(metric_paths)
    per_metric = {metric: resolve_dimensions(BUILD, [metric], client.available_dimensions((metric,)))
                  for metric in metrics}
    bindings = {}
    for metric in metrics:
        token = "buyer__country" if same_path else metric_paths[metric]
        option_id = next(key for key, option in per_metric[metric].items() if option.token == token)
        bindings[metric] = {"optionId": option_id}
    body = {"mode": "QUERY", "metrics": metrics, "groupBy": [{"name": "country", "bindings": bindings}]}
    common = {}
    if time:
        common = resolve_dimensions(BUILD, metrics, client.available_dimensions(tuple(metrics)))
        option_id = next(key for key, option in common.items() if option.option.name == "metric_time")
        body["groupBy"].append({"optionId": option_id, "grain": "month"})
    return client, body, common, per_metric


def rows_by_country(result):
    names = [column.name for column in result.columns]
    return {row[names.index("country")]: dict(zip(names, row, strict=True)) for row in result.rows}


def normalized_rows(result, keys):
    names = [column.name for column in result.columns]
    rows = [dict(zip(names, row, strict=True)) for row in result.rows]
    by_key = {tuple(row[key] for key in keys): row for row in rows}
    assert len(by_key) == len(rows)
    return by_key


def native_request(metric_paths, *, time=False, **kwargs):
    group_by = [BoundGroupByParameter("country", tuple(
        MetricGroupByBinding(metric, DimensionOrEntityParameter(path)) for metric, path in metric_paths.items()
    ))]
    if time:
        group_by.append(TimeDimensionParameter("metric_time", grain="month"))
    return MetricFlowQueryRequest.create(metric_names=list(metric_paths), bound_group_by=group_by, **kwargs)


@pytest.mark.parametrize("passthrough", [False, True], ids=["dfs", "passthrough"])
@pytest.mark.parametrize("level", [SqlOptimizationLevel.O4, SqlOptimizationLevel.O5], ids=["subquery", "cte"])
@pytest.mark.parametrize("scenario", ["independent", "common", "filtered", "derived"])
def test_native_and_service_results_match_fixed_expectations(warehouse, monkeypatch, passthrough, level, scenario):
    paths = {"revenue": "buyer__country", "refunds": "seller__country"}
    if scenario == "common":
        paths["refunds"] = "buyer__country"
    if scenario == "derived":
        paths.update(double_revenue="seller__country", average_revenue="seller__country", net_revenue="buyer__country")
    client, body, common, per_metric = prepare(warehouse, time=True, metric_paths=paths)
    options = DataflowPlanOptimization.enabled_optimizations() - {
        DataflowPlanOptimization.PASSTHROUGH_METRIC_EVALUATION,
    }
    if passthrough:
        options |= {DataflowPlanOptimization.PASSTHROUGH_METRIC_EVALUATION}
    kwargs = {"sql_optimization_level": level, "dataflow_plan_optimizations": options}
    if scenario == "filtered":
        time_id = next(key for key, option in common.items() if option.option.name == "metric_time")
        body.update(startTime="2024-01-01", endTime="2024-01-31",
                    filters=[{"optionId": time_id, "operator": ">=", "value": "2024-01-01"}])
        kwargs.update(time_constraint_start=datetime(2024, 1, 1), time_constraint_end=datetime(2024, 1, 31),
                      where_constraints=["{{ Dimension('metric_time') }} >= '2024-01-01'"])
    plan = resolve_query_bindings(MetricQueryRequest.model_validate(body), common, per_metric)
    service = client.query(plan)
    calls = []
    query = client._sql_client.query

    def fetch(*args, **kwargs):
        calls.append(args[0])
        return query(*args, **kwargs)

    monkeypatch.setattr(client._sql_client, "query", fetch)
    request = native_request(paths, time=True, **kwargs)
    client._engine.explain(request)
    assert calls == []
    result = client._engine.query(request)
    assert len(calls) == 1
    assert result.result_df is not None
    native = serialize_rows(result.result_df, limit=100)
    keys = ("country", "metric_time__month")
    actual = normalized_rows(native, keys)
    assert actual == normalized_rows(service, keys)
    january, february = "2024-01-01T00:00:00", "2024-02-01T00:00:00"
    if scenario == "common":
        expected = {("CN", january): (30, 3), ("US", february): (40, 4), (None, february): (30, 3)}
    else:
        expected = {("CN", january): (30, None), ("US", january): (None, 1),
                    ("JP", january): (None, 2), ("US", february): (40, None),
                    ("DE", february): (None, 4), (None, february): (30, 3)}
        if scenario == "filtered":
            expected = {key: value for key, value in expected.items() if key[1] == january}
    assert {key: (row["revenue"], row["refunds"]) for key, row in actual.items()} == expected
    if scenario == "derived":
        assert {key: (row["double_revenue"], row["average_revenue"], row["net_revenue"])
                for key, row in actual.items()} == {
            ("CN", january): (None, None, 27), ("US", january): (20, 10, None),
            ("JP", january): (40, 20, None), ("US", february): (None, None, 36),
            ("DE", february): (80, 40, None), (None, february): (60, 30, 27),
        }
    if scenario == "derived":
        january = normalized_rows(native, keys)[("CN", "2024-01-01T00:00:00")]
        assert january["revenue"] == 30 and january["net_revenue"] == 27
        assert january["double_revenue"] is None
    # Service's lookahead row detects truncation; native limit itself remains exact.
    body.update(limit=1, orderBy=[{"fieldId": "country", "direction": "ASC"}])
    limited_plan = resolve_query_bindings(MetricQueryRequest.model_validate(body), common, per_metric)
    limited_service = client.query(limited_plan)
    limited = client._engine.query(native_request(paths, time=True, order_by_names=["country"], limit=2, **kwargs))
    assert limited.result_df is not None and len(limited.result_df.rows) == 2
    limited_native = serialize_rows(limited.result_df, limit=1)
    assert limited_native.truncated == limited_service.truncated is True
    assert normalized_rows(limited_native, keys) == normalized_rows(limited_service, keys)


def test_real_engine_executes_one_bound_query_and_aligns_results(warehouse, monkeypatch):
    client, body, common, per_metric = prepare(warehouse)
    request = MetricQueryRequest.model_validate(body)
    plan = resolve_query_bindings(request, common, per_metric)
    executed_sql = []
    query = client._sql_client.query

    def execute_once(sql, **kwargs):
        executed_sql.append(sql)
        return query(sql, **kwargs)

    monkeypatch.setattr(client._sql_client, "query", execute_once)
    explained = client.query(plan.model_copy(update={"mode": QueryMode.EXPLAIN}))
    assert executed_sql == []
    assert explained.sql
    result = client.query(plan)
    assert len(executed_sql) == 1
    rows = rows_by_country(result)
    assert set(rows) == {"CN", "US", "JP", "DE", None}
    assert rows["CN"] == {"country": "CN", "revenue": 30, "orders_count": 2,
                          "refunds": None, "returns_count": None}
    assert rows["US"]["revenue"] == 40 and rows["US"]["refunds"] == 1
    assert rows[None]["revenue"] == 30 and rows[None]["refunds"] == 3
    # 排序使用没有缺失值的公共键，避免依赖各数据库的 NULL 默认排序。
    body.update(limit=1, orderBy=[{"fieldId": "country", "direction": "ASC"}])
    limited = client.query(resolve_query_bindings(MetricQueryRequest.model_validate(body), common, per_metric))
    assert limited.truncated and len(limited.rows) == 1
    assert rows_by_country(limited)["CN"]["revenue"] == 30


def test_real_engine_keeps_compatible_metrics_in_one_query(warehouse):
    client, body, common, per_metric = prepare(warehouse, same_path=True)
    plan = resolve_query_bindings(MetricQueryRequest.model_validate(body), common, per_metric)
    result = rows_by_country(client.query(plan))
    assert result["CN"]["revenue"] == 30 and result["CN"]["refunds"] == 3


@pytest.mark.parametrize("shared_first", [False, True])
def test_interleaved_metric_groups_preserve_requested_column_order(warehouse, monkeypatch, shared_first):
    paths = {"revenue": "buyer__country", "refunds": "seller__country", "orders_count": "buyer__country"}
    client, body, common, per_metric = prepare(warehouse, time=True, metric_paths=paths)
    if shared_first:
        body["groupBy"].reverse()
    plan = resolve_query_bindings(MetricQueryRequest.model_validate(body), common, per_metric)
    executed_sql = []
    query = client._sql_client.query

    def fetch(sql, **kwargs):
        executed_sql.append(sql)
        return query(sql, **kwargs)

    monkeypatch.setattr(client._sql_client, "query", fetch)
    result = client.query(plan)
    assert len(executed_sql) == 1
    key_names = ["metric_time__month", "country"] if shared_first else ["country", "metric_time__month"]
    assert [column.name for column in result.columns] == [*key_names, "revenue", "refunds", "orders_count"]
    january, february = "2024-01-01T00:00:00", "2024-02-01T00:00:00"
    expected = [
        ("CN", january, 30, None, 2), ("US", january, None, 1, None),
        ("JP", january, None, 2, None), ("US", february, 40, None, 1),
        ("DE", february, None, 4, None), (None, february, 30, 3, 1),
    ]
    if shared_first:
        expected = [(row[1], row[0], *row[2:]) for row in expected]
    assert {tuple(row) for row in result.rows} == set(expected)


@pytest.mark.parametrize("duplicate", [False, True], ids=["opposite-paths", "repeated-source"])
def test_service_two_bound_dimensions_keep_complete_key_and_slot_order(warehouse, duplicate):
    paths = {"revenue": "buyer__country", "refunds": "seller__country"}
    client, body, common, per_metric = prepare(warehouse, time=True, metric_paths=paths)
    counterpart_paths = paths if duplicate else {"revenue": "seller__country", "refunds": "buyer__country"}
    body["groupBy"].append({"name": "counterparty", "bindings": {
        metric: {"optionId": next(key for key, option in per_metric[metric].items() if option.token == token)}
        for metric, token in counterpart_paths.items()
    }})
    result = client.query(resolve_query_bindings(MetricQueryRequest.model_validate(body), common, per_metric))
    assert [column.name for column in result.columns] == [
        "country", "metric_time__month", "counterparty", "revenue", "refunds",
    ]
    january, february = "2024-01-01T00:00:00", "2024-02-01T00:00:00"
    expected = (
        {("CN", january, "CN", 30, None), ("US", january, "US", None, 1),
         ("JP", january, "JP", None, 2), ("US", february, "US", 40, None),
         ("DE", february, "DE", None, 4), (None, february, None, 30, 3)}
        if duplicate else
        {("CN", january, "US", 10, None), ("CN", january, "JP", 20, None),
         ("US", january, "CN", None, 1), ("JP", january, "CN", None, 2),
         ("US", february, "DE", 40, None), ("DE", february, "US", None, 4),
         (None, february, None, 30, 3)}
    )
    assert len(result.rows) == len(expected)
    assert {tuple(row) for row in result.rows} == expected


def test_service_shared_in_filter_targets_source_not_public_output(warehouse):
    client, body, common, per_metric = prepare(
        warehouse, time=True, metric_paths={"revenue": "buyer__country", "refunds": "seller__country"},
    )
    buyer_option = next(key for key, option in common.items() if option.token == "buyer__country")
    body["filters"] = [{"optionId": buyer_option, "operator": "IN", "value": ["CN", "US"]}]
    result = client.query(resolve_query_bindings(MetricQueryRequest.model_validate(body), common, per_metric))
    january, february = "2024-01-01T00:00:00", "2024-02-01T00:00:00"
    assert [column.name for column in result.columns] == ["country", "metric_time__month", "revenue", "refunds"]
    # Seller JP/DE remain: the predicate applies to the source buyer, not the final country alias.
    assert len(result.rows) == 5
    assert {tuple(row) for row in result.rows} == {
        ("CN", january, 30, None), ("US", january, None, 1), ("JP", january, None, 2),
        ("US", february, 40, None), ("DE", february, None, 4),
    }


def test_executor_uses_single_metric_options_and_shared_time_filter(warehouse, settings, monkeypatch):
    project, content = warehouse
    _, body, common, _ = prepare(warehouse, time=True)
    time_id = next(key for key, option in common.items() if option.option.name == "metric_time")
    body["startTime"], body["endTime"] = "2024-01-01", "2024-01-31"
    body["filters"] = [{"optionId": time_id, "operator": ">=", "value": "2024-01-01"}]
    executor = QueryExecutor(settings, object())

    @contextmanager
    def context(*args, **kwargs):
        yield content, project, project

    monkeypatch.setattr(executor, "_engine_context", context)
    result = executor.execute_query(SimpleNamespace(build_id=BUILD), MetricQueryRequest.model_validate(body))
    rows = rows_by_country(result)
    assert set(rows) == {"CN", "US", "JP"}
    assert rows["CN"]["revenue"] == 30
    assert "metric_time__month" in rows["CN"]


def test_mapped_labels_never_replace_alignment_keys(tmp_path):
    from dbt_metricflow_service.engines.models import CompiledQuery

    client, body, common, per_metric = prepare(build_warehouse(tmp_path, mapped=True))
    # 普通查询仍按原有协议显示映射标签，并保留原始键形成的两行。
    ordinary = client.query(CompiledQuery(mode=QueryMode.QUERY, metrics=["revenue"],
                                          group_by=["buyer__country"]))
    assert sum("same label" in row for row in ordinary.rows) == 2
    plan = resolve_query_bindings(MetricQueryRequest.model_validate(body), common, per_metric)
    result = client.query(plan)
    assert len(result.rows) == 5
    rows = rows_by_country(result)
    assert set(rows) == {"CN", "US", "JP", "DE", None}
    assert rows["CN"]["refunds"] is None
    assert rows["US"]["revenue"] == 40 and rows["US"]["refunds"] == 1
    native = client._engine.query(native_request({
        "revenue": "buyer__country", "orders_count": "buyer__country",
        "refunds": "seller__country", "returns_count": "seller__country",
    }))
    assert native.result_df is not None
    actual = normalized_rows(serialize_rows(native.result_df, limit=100), ("country",))
    assert actual == normalized_rows(result, ("country",))
    # Bound compilation does not change display mapping for subsequent ordinary requests.
    ordinary_after = client.query(CompiledQuery(mode=QueryMode.QUERY, metrics=["revenue"],
                                                group_by=["buyer__country"]))
    assert ordinary_after.rows == ordinary.rows


@pytest.mark.parametrize("adapter", ["postgres", "starrocks"])
def test_native_bound_plan_renders_service_dialects(warehouse, adapter):
    client, _, _, _ = prepare(warehouse)
    engine = MetricFlowEngine(client._semantic_context, PlanningSqlClient(adapter))
    explained = engine.explain(native_request(
        {"revenue": "buyer__country", "refunds": "seller__country", "double_revenue": "seller__country"},
        time=True, where_constraints=["{{ Dimension('buyer__country') }} = 'CN'"],
        order_by_names=["country"], limit=2,
    ))
    assert "UNION" in explained.sql_statement.sql
    assert "UNION ALL" not in explained.sql_statement.sql
    assert "IS NULL" in explained.sql_statement.sql
    assert "LEFT OUTER JOIN" in explained.sql_statement.sql
