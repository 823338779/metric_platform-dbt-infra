import duckdb
import pytest
from metricflow_semantics.errors.error_classes import InvalidQueryException

from dbt_metricflow_service.engines.models import BoundQueryGroup, BoundQueryPlan, CompiledQuery, QueryMode


def plan(metrics, keys, limit=100, order=None):
    return BoundQueryPlan(mode=QueryMode.QUERY, metrics=metrics, group_by=keys, groups=[],
                          order_by=order or [], limit=limit)


def quote(name):
    return '"' + name.replace('"', '""') + '"'


def test_sql_merge_preserves_null_and_one_sided_keys_across_three_groups():
    from dbt_metricflow_service.engines.bound_queries import SqlQueryGroup, merge_sql

    groups = [
        SqlQueryGroup(("a",), "select * from (values ('CN', 1), (NULL, 2)) t(country, a)"),
        SqlQueryGroup(("b",), "select * from (values ('US', 3), (NULL, 4)) t(country, b)"),
        SqlQueryGroup(("c",), "select * from (values ('JP', 5), ('US', 6), (NULL, 7)) t(country, c)"),
    ]
    sql = merge_sql(plan(["a", "b", "c"], ["country"]), groups, quote)
    with duckdb.connect() as connection:
        rows = connection.execute(sql).fetchall()
    assert set(rows) == {("CN", 1, None, None), ("US", None, 3, 6),
                         ("JP", None, None, 5), (None, 2, 4, 7)}


def test_sql_merge_orders_and_limits_only_after_composite_key_alignment():
    from dbt_metricflow_service.engines.bound_queries import SqlQueryGroup, merge_sql

    groups = [
        SqlQueryGroup(("a",), "select * from (values ('CN', 1, 2), ('CN', 2, 3)) t(country, month, a)"),
        SqlQueryGroup(("b",), "select * from (values ('CN', 2, 90), ('CN', 1, 20)) t(country, month, b)"),
    ]
    sql = merge_sql(plan(["a", "b"], ["country", "month"], limit=1, order=["-b"]), groups, quote)
    with duckdb.connect() as connection:
        rows = connection.execute(sql).fetchall()
    # limit+1 保留第二行供 resultTruncated 判断，且 b=90 必须关联到 a=3。
    assert rows == [("CN", 2, 3, 90), ("CN", 1, 2, 20)]


def test_single_group_projects_without_an_alignment_join():
    from dbt_metricflow_service.engines.bound_queries import SqlQueryGroup, merge_sql

    sql = merge_sql(plan(["a", "b"], ["country"]),
                    [SqlQueryGroup(("a", "b"), "select 'CN' as country, 1 as a, 2 as b")], quote)
    assert "JOIN" not in sql.upper()
    with duckdb.connect() as connection:
        assert connection.execute(sql).fetchall() == [("CN", 1, 2)]


def test_compile_first_tries_whole_group_then_keeps_compatible_metrics_together():
    from dbt_metricflow_service.engines.bound_queries import compile_groups

    attempted = []

    def compile_group(group):
        metrics = tuple(group.query.metrics)
        attempted.append(metrics)
        if "a" in metrics and "b" in metrics:
            raise InvalidQueryException("incompatible")
        return "select " + ", ".join(metrics)

    candidate = BoundQueryGroup(query=CompiledQuery(mode=QueryMode.QUERY, metrics=["a", "b", "c"]),
                                dimensions=[])
    groups = compile_groups([candidate], compile_group)
    assert attempted[0] == ("a", "b", "c")
    assert [group.metrics for group in groups] == [("a", "c"), ("b",)]


@pytest.mark.parametrize("error", [RuntimeError("internal failure"), InvalidQueryException("bad path")])
def test_compile_does_not_split_on_internal_errors_or_mask_invalid_single_metrics(error):
    from dbt_metricflow_service.engines.bound_queries import compile_groups

    candidate = BoundQueryGroup(query=CompiledQuery(mode=QueryMode.QUERY, metrics=["a", "b"]), dimensions=[])
    def compile_group(group):
        raise error

    with pytest.raises(type(error)):
        compile_groups([candidate], compile_group)
