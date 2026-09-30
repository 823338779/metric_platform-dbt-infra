"""旧调用方的引擎名称在服务端转换为固定版本资源身份。"""

from uuid import uuid4

import pytest

from dbt_metricflow_service.publication_compatibility import submit_legacy
from tests.test_publication_queries import query_service
from tests.test_publication_storage import store as store


def test_legacy_query_uses_service_owned_identity(store, tmp_path):
    service, job, release = query_service(store, tmp_path)
    result = submit_legacy(service, job["project_id"], "QUERY", {
        "releaseId": release["release_id"], "metrics": ["orders"], "groupBy": ["order__region"]}, uuid4().hex)
    row = service.runtime.jobs.get(result["queryId"])
    assert row["request_json"]["publicationRequest"]["metricResourceIds"] == ["metric.sample.orders"]
    assert row["request_json"]["engineRequest"]["groupBy"] == ["order__region"]
    assert result["status"] == "PENDING"


def test_legacy_query_rejects_arbitrary_engine_expression(store, tmp_path):
    service, job, release = query_service(store, tmp_path)
    with pytest.raises(ValueError):
        submit_legacy(service, job["project_id"], "QUERY", {
            "releaseId": release["release_id"], "metrics": ["orders"], "groupBy": ["custom_sql()"]}, uuid4().hex)
