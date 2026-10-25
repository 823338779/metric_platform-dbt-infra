"""查询结果直接响应，保留精度、空值和响应大小限制。"""

import pytest

from dbt_metricflow_service.application.errors import ServiceError
from dbt_metricflow_service.models.queries import QueryRequest
from tests.synchronous_queries import inline_executor
from tests.test_publication_queries import query_service
from tests.test_publication_storage import store as store


def test_query_returns_all_bounded_rows_without_pagination(store, tmp_path):
    service, _, build = query_service(store, tmp_path)
    rows = [[i, "12345678901234567890.123456789", None] for i in range(403)]
    service.execute = inline_executor(query_result={"columns": [], "rows": rows, "truncated": True})
    request = QueryRequest(mode="QUERY", metrics=["orders"])
    result = service.submit(build.build_id, request, "p")
    assert result.rows == rows
    assert result.result_truncated
    assert not hasattr(service, "results")
    assert not hasattr(service, "status")


def test_oversized_response_is_rejected(store, tmp_path):
    service, _, build = query_service(store, tmp_path)
    service.execute = inline_executor(query_result={"columns": [], "rows": [["x" * (9 * 1024 * 1024)]]})
    with pytest.raises(ServiceError) as error:
        service.submit(build.build_id, QueryRequest(mode="QUERY", metrics=["orders"]), "p")
    assert error.value.error.code == "RESULT_TOO_LARGE"
