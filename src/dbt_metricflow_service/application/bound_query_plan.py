"""解析每指标维度绑定，将相同完整通路的指标放入同一候选组。"""
from __future__ import annotations

from .. import constants as strings
from ..engines.models import BoundGroupBy, BoundQueryGroup, BoundQueryPlan
from ..models.queries import BoundDimension, MetricQueryRequest, SelectedDimension
from .errors import ServiceError
from .query_plan import ResolvedDimension, compile_query


def _resolve(
    selected: SelectedDimension, options: dict[str, ResolvedDimension], name: str | None,
) -> BoundGroupBy:
    dimension = options.get(selected.option_id)
    if dimension is None or selected.grain is not None and selected.grain not in dimension.grain_tokens:
        raise ServiceError(strings.INVALID_DIMENSION_OPTION, "option or grain belongs to another selection")
    if dimension.option.dimension_type == strings.TIME_DIMENSION_TYPE and selected.grain is None:
        raise ServiceError(strings.INVALID_DIMENSION_OPTION, "bound queries require an explicit time grain")
    token = dimension.grain_tokens[selected.grain] if selected.grain else dimension.token
    return BoundGroupBy(name=name or token, token=token,
                        dimension_type=dimension.option.dimension_type, grain=selected.grain)


def compile_bound_query(
    request: MetricQueryRequest, common: dict[str, ResolvedDimension],
    per_metric: dict[str, dict[str, ResolvedDimension]],
) -> BoundQueryPlan:
    # 公共筛选仍按完整指标集合校验；排序只在最终对齐结果上执行。
    base = compile_query(request.model_copy(update={"group_by": [], "order_by": []}), common)
    groups: dict[tuple[tuple[str, str, str | None], ...], BoundQueryGroup] = {}
    output_names: list[str] = []
    expected_types: list[tuple[str, str | None]] | None = None
    order_fields = {metric: metric for metric in request.metrics}
    for metric in dict.fromkeys(request.metrics):
        dimensions = []
        for selected in request.group_by:
            if isinstance(selected, BoundDimension):
                resolved = _resolve(selected.bindings[metric], per_metric[metric], selected.name)
                order_fields[selected.name] = resolved.name
            else:
                resolved = _resolve(selected, common, None)
                order_fields[selected.option_id] = resolved.name
            dimensions.append(resolved)
        names = [dimension.name for dimension in dimensions]
        if len(set(names)) != len(names) or set(names).intersection(request.metrics):
            raise ServiceError(strings.INVALID_DIMENSION_OPTION, "output dimension names must be unique")
        types = [(dimension.dimension_type, dimension.grain) for dimension in dimensions]
        if expected_types is not None and types != expected_types:
            raise ServiceError(strings.INVALID_DIMENSION_OPTION, "aligned dimensions require matching types and grains")
        expected_types, output_names = types, names
        signature = tuple((dimension.token, dimension.dimension_type, dimension.grain) for dimension in dimensions)
        if signature in groups:
            groups[signature].query.metrics.append(metric)
        else:
            groups[signature] = BoundQueryGroup(
                query=base.model_copy(update={"metrics": [metric], "group_by": [d.token for d in dimensions]}),
                dimensions=dimensions,
            )
    order_by = []
    for selected_order in request.order_by:
        if selected_order.field_id not in order_fields:
            raise ServiceError(strings.INVALID_ORDER_FIELD, "order field was not selected")
        order_by.append(("-" if selected_order.direction == "DESC" else "") +
                        order_fields[selected_order.field_id])
    return BoundQueryPlan(mode=request.mode, metrics=list(dict.fromkeys(request.metrics)), group_by=output_names,
                          groups=list(groups.values()), order_by=order_by, limit=request.limit)
