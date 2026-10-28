from contextlib import contextmanager
from types import SimpleNamespace

import pytest
import yaml
from test_sdk_integration import write_project

from dbt_metricflow_service.application.bound_query_plan import compile_bound_query
from dbt_metricflow_service.application.query_plan import resolve_dimensions
from dbt_metricflow_service.engines.dbt import DbtClient
from dbt_metricflow_service.engines.metricflow import MetricFlowClient
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
    for metric, expr in [("orders_count", "1"), ("refunds", "refund"), ("returns_count", "1")]:
        orders["measures"].append({"name": metric, "agg": "sum", "expr": expr})
        semantic["metrics"].append({"name": metric, "label": metric, "type": "simple",
                                    "type_params": {"measure": metric}})
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


def prepare(warehouse, same_path=False, time=False):
    project, content = warehouse
    client = MetricFlowClient(content, project=project, profiles=project)
    metrics = ["revenue", "orders_count", "refunds", "returns_count"]
    per_metric = {metric: resolve_dimensions(BUILD, [metric], client.available_dimensions((metric,)))
                  for metric in metrics}
    bindings = {}
    for index, metric in enumerate(metrics):
        token = "buyer__country" if same_path or index < 2 else "seller__country"
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


def test_real_engine_groups_four_metrics_into_two_queries_and_aligns_results(warehouse, monkeypatch):
    client, body, common, per_metric = prepare(warehouse)
    request = MetricQueryRequest.model_validate(body)
    plan = compile_bound_query(request, common, per_metric)
    executed_sql = []
    query = client._sql_client.query

    def execute_once(sql):
        executed_sql.append(sql)
        return query(sql)

    monkeypatch.setattr(client._sql_client, "query", execute_once)
    explained = client.query_bound(plan.model_copy(update={"mode": QueryMode.EXPLAIN}))
    assert executed_sql == []
    assert "mf_group_0 AS" in explained.sql and "mf_group_1 AS" in explained.sql
    assert "mf_group_2 AS" not in explained.sql
    assert explained.sql.upper().count("LIMIT") == 1
    result = client.query_bound(plan)
    assert len(executed_sql) == 1
    assert "mf_group_0 AS" in executed_sql[0] and "mf_group_1 AS" in executed_sql[0]
    rows = rows_by_country(result)
    assert set(rows) == {"CN", "US", "JP", "DE", None}
    assert rows["CN"] == {"country": "CN", "revenue": 30, "orders_count": 2,
                          "refunds": None, "returns_count": None}
    assert rows["US"]["revenue"] == 40 and rows["US"]["refunds"] == 1
    assert rows[None]["revenue"] == 30 and rows[None]["refunds"] == 3
    # 排序使用没有缺失值的公共键，避免依赖各数据库的 NULL 默认排序。
    body.update(limit=1, orderBy=[{"fieldId": "country", "direction": "ASC"}])
    limited = client.query_bound(compile_bound_query(MetricQueryRequest.model_validate(body), common, per_metric))
    assert limited.truncated and len(limited.rows) == 1
    assert rows_by_country(limited)["CN"]["revenue"] == 30


def test_real_engine_keeps_compatible_metrics_in_one_query(warehouse):
    client, body, common, per_metric = prepare(warehouse, same_path=True)
    plan = compile_bound_query(MetricQueryRequest.model_validate(body), common, per_metric)
    assert len(plan.groups) == 1
    result = rows_by_country(client.query_bound(plan))
    assert result["CN"]["revenue"] == 30 and result["CN"]["refunds"] == 3


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
    plan = compile_bound_query(MetricQueryRequest.model_validate(body), common, per_metric)
    result = client.query_bound(plan)
    assert len(result.rows) == 5
    rows = rows_by_country(result)
    assert set(rows) == {"CN", "US", "JP", "DE", None}
    assert rows["CN"]["refunds"] is None
    assert rows["US"]["revenue"] == 40 and rows["US"]["refunds"] == 1
