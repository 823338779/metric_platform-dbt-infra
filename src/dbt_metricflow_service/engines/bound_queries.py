"""原生编译验证后的指标分组，以及按公共键对齐的 SQL。"""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from metricflow_semantics.errors.error_classes import InvalidQueryException

from .models import BoundQueryGroup, BoundQueryPlan


@dataclass(frozen=True)
class SqlQueryGroup:
    metrics: tuple[str, ...]
    sql: str


def compile_groups(
    candidates: list[BoundQueryGroup], compile_group: Callable[[BoundQueryGroup], str],
) -> list[SqlQueryGroup]:
    result = []
    for candidate in candidates:
        try:
            sql = compile_group(candidate)
        except InvalidQueryException:
            if len(candidate.query.metrics) == 1:
                raise
        else:
            result.append(SqlQueryGroup(tuple(candidate.query.metrics), sql))
            continue
        # 只有原生组合校验失败才拆分；贪心优先放进已存在的可编译组。
        partitions: list[SqlQueryGroup] = []
        for metric in candidate.query.metrics:
            for index, partition in enumerate(partitions):
                metrics = (*partition.metrics, metric)
                combined = candidate.model_copy(update={
                    "query": candidate.query.model_copy(update={"metrics": list(metrics)}),
                })
                try:
                    sql = compile_group(combined)
                except InvalidQueryException:
                    continue
                partitions[index] = SqlQueryGroup(metrics, sql)
                break
            else:
                single = candidate.model_copy(update={
                    "query": candidate.query.model_copy(update={"metrics": [metric]}),
                })
                partitions.append(SqlQueryGroup((metric,), compile_group(single)))
        result.extend(partitions)
    return result


def merge_sql(
    plan: BoundQueryPlan, groups: list[SqlQueryGroup], quote: Callable[[str], str],
) -> str:
    ctes = [f"mf_group_{index} AS (\n{group.sql}\n)" for index, group in enumerate(groups)]
    metric_groups = {metric: index for index, group in enumerate(groups) for metric in group.metrics}
    if len(groups) == 1:
        source = "mf_group_0 AS g0"
        key_prefix = "g0"
    else:
        keys = ", ".join(quote(name) for name in plan.group_by)
        key_queries = [f"SELECT {keys} FROM mf_group_{index}" for index in range(len(groups))]
        ctes.append("mf_keys AS (\n" + "\nUNION\n".join(key_queries) + "\n)")
        source = "mf_keys AS k"
        key_prefix = "k"
        for index in range(len(groups)):
            predicates = []
            for name in plan.group_by:
                left, right = f"k.{quote(name)}", f"g{index}.{quote(name)}"
                predicates.append(f"({left} = {right} OR ({left} IS NULL AND {right} IS NULL))")
            source += f"\nLEFT JOIN mf_group_{index} AS g{index} ON " + " AND ".join(predicates)
    columns = [f"{key_prefix}.{quote(name)} AS {quote(name)}" for name in plan.group_by]
    columns.extend(f"g{metric_groups[metric]}.{quote(metric)} AS {quote(metric)}" for metric in plan.metrics)
    sql = "WITH " + ",\n".join(ctes) + "\nSELECT " + ", ".join(columns) + "\nFROM " + source
    if plan.order_by:
        sql += "\nORDER BY " + ", ".join(
            quote(order[1:]) + " DESC" if order.startswith("-") else quote(order) + " ASC"
            for order in plan.order_by
        )
    return sql + f"\nLIMIT {plan.limit + 1}"
