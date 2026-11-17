"""Resolve public dimension options into native query bindings without grouping metrics."""
from __future__ import annotations

from .. import constants as strings
from ..engines.models import CompiledQuery, ResolvedBoundPath, ResolvedGroupByBinding
from ..models.queries import BoundDimension, MetricQueryRequest, SelectedDimension
from .errors import ServiceError
from .query_plan import ResolvedDimension, compile_query


def _resolve(selected: SelectedDimension, options: dict[str, ResolvedDimension]) -> ResolvedBoundPath:
    dimension = options.get(selected.option_id)
    if dimension is None or selected.grain is not None and selected.grain not in dimension.grain_tokens:
        raise ServiceError(strings.INVALID_DIMENSION_OPTION, "option or grain belongs to another selection")
    if dimension.option.dimension_type == strings.TIME_DIMENSION_TYPE and selected.grain is None:
        raise ServiceError(strings.INVALID_DIMENSION_OPTION, "bound queries require an explicit time grain")
    token = dimension.grain_tokens[selected.grain] if selected.grain else dimension.token
    return ResolvedBoundPath(token=token, dimension_type=dimension.option.dimension_type, grain=selected.grain)


def resolve_query_bindings(
    request: MetricQueryRequest, common: dict[str, ResolvedDimension],
    per_metric: dict[str, dict[str, ResolvedDimension]],
) -> CompiledQuery:
    # Shared filters retain the ordinary query validation and source-dimension semantics.
    base = compile_query(request.model_copy(update={"group_by": [], "order_by": []}), common)
    metrics = list(dict.fromkeys(request.metrics))
    bindings = []
    output_names = []
    order_fields = {metric: metric for metric in metrics}
    for selected in request.group_by:
        if isinstance(selected, BoundDimension):
            paths = {metric: _resolve(selected.bindings[metric], per_metric[metric]) for metric in metrics}
            name = selected.name
            field_id = name
            if len({(path.dimension_type, path.grain) for path in paths.values()}) != 1:
                raise ServiceError(
                    strings.INVALID_DIMENSION_OPTION, "aligned dimensions require matching types and grains",
                )
            bindings.append(ResolvedGroupByBinding(name=name, paths=paths))
        else:
            path = _resolve(selected, common)
            name = path.token
            field_id = selected.option_id
            bindings.append(ResolvedGroupByBinding(name=None, paths=dict.fromkeys(metrics, path)))
        output_names.append(name)
        order_fields[field_id] = name
    if len(set(output_names)) != len(output_names) or set(output_names).intersection(metrics):
        raise ServiceError(strings.INVALID_DIMENSION_OPTION, "output dimension names must be unique")
    order_by = []
    for order in request.order_by:
        if order.field_id not in order_fields:
            raise ServiceError(strings.INVALID_ORDER_FIELD, "order field was not selected")
        order_by.append(("-" if order.direction == "DESC" else "") + order_fields[order.field_id])
    return base.model_copy(update={"metrics": metrics, "bound_group_by": bindings, "order_by": order_by})
