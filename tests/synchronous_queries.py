"""只替换外部引擎调用；查询输入与结果不写入任务表。"""

from copy import deepcopy

from dbt_metricflow_service.platform.models import DataQueryResult, DimensionQueryResult

NATIVE_AVAILABLE_DIMENSIONS = {
    "metrics": [{"name": "orders"}],
    "dimensions": [
        {"token": "order__region", "name": "region", "type": "categorical"},
        {"token": "return__region", "name": "region", "type": "categorical"},
    ],
    "timeDimensions": [{"token": "metric_time__month", "granularity": "month"}],
    "allowedFilters": ["=", "IN"],
}
QUERY_RESULT = {"sql": "select 1", "columns": [], "rows": [], "truncated": False}


def inline_executor(*, query_result=None):
    calls = []

    def execute(build, request):
        calls.append((build, deepcopy(request)))
        return DataQueryResult.model_validate(deepcopy(QUERY_RESULT if query_result is None else query_result))

    def discover_dimensions(build, metrics):
        calls.append((build, metrics))
        return DimensionQueryResult.model_validate(deepcopy(NATIVE_AVAILABLE_DIMENSIONS))

    execute.calls = calls
    execute.discover_dimensions = discover_dimensions
    return execute
