"""只替换外部引擎调用；查询输入与结果不写入任务表。"""

from copy import deepcopy

from dbt_metricflow_service.platform.models import EngineResult

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
    def execute(build, payload):
        execute.calls.append((build, deepcopy(payload)))
        return EngineResult.model_validate(deepcopy(NATIVE_AVAILABLE_DIMENSIONS if payload.mode == "OPTIONS" else (
            QUERY_RESULT if query_result is None else query_result
        )))

    execute.calls = []
    return execute
