"""时间口径固定于发布与已受理查询，历史重试不改写引擎请求。"""

from uuid import uuid4

from dbt_metricflow_service.publications.models import PublishedQueryRequest
from tests.test_publication_queries import query_service
from tests.test_publication_storage import store as store


def test_equivalent_offsets_share_idempotent_query_and_business_calendar(store, tmp_path):
    service, job, release = query_service(store, tmp_path)
    request = PublishedQueryRequest(releaseId=release["release_id"], idempotencyKey=uuid4().hex,
        mode="QUERY", metricResourceIds=["metric.sample.orders"], startTime="2026-09-30T16:00:00Z")
    accepted = service.submit_query(job["project_id"], request, "platform")
    saved = service.runtime.jobs.get(accepted["queryId"])
    assert saved["request_json"]["engineRequest"]["startTime"] == "2026-10-01T00:00:00"
    equivalent = PublishedQueryRequest.model_validate({**request.model_dump(by_alias=True),
                                                       "startTime": "2026-10-01T00:00:00+08:00"})
    assert service.submit_query(job["project_id"], equivalent, "platform") == accepted
    assert saved["request_json"]["businessTimezone"] == "Asia/Shanghai"


def test_legacy_naive_calendar_is_preserved(store, tmp_path):
    service, job, release = query_service(store, tmp_path)
    request = PublishedQueryRequest(releaseId=release["release_id"], idempotencyKey=uuid4().hex,
        mode="QUERY", metricResourceIds=["metric.sample.orders"], startTime="2026-10-01T00:00:00")
    receipt = service.submit_query(job["project_id"], request, "platform")
    saved = service.runtime.jobs.get(receipt["queryId"])
    assert saved["request_json"]["engineRequest"]["startTime"] == "2026-10-01T00:00:00"
    assert saved["request_json"]["businessTimezone"] == "Asia/Shanghai"


def test_capabilities_disclose_project_path_and_time_contract(store, tmp_path):
    service, job, _ = query_service(store, tmp_path)
    publication = service.publication(job["project_id"])
    assert publication["protocolVersion"] == "agent-dbt-v1"
    assert "draft-validation-v1" in publication["capabilities"]
    assert publication["businessTimezone"] == "Asia/Shanghai"


def test_query_error_identifies_invalid_dimension(store, tmp_path):
    import pytest

    from dbt_metricflow_service.publications.errors import PublicationError

    service, job, release = query_service(store, tmp_path)
    request = PublishedQueryRequest(releaseId=release["release_id"], idempotencyKey=uuid4().hex,
        mode="QUERY", metricResourceIds=["metric.sample.orders"], groupBy=[{"optionId": "missing"}])
    with pytest.raises(PublicationError) as caught:
        service.submit_query(job["project_id"], request, "platform")
    assert caught.value.reason == "invalid_dimension_option"
    assert caught.value.field == "groupBy[0].optionId"


def test_historical_snapshot_without_timezone_retries_without_rewriting_engine(store, tmp_path):
    from psycopg2.extras import Json

    service, job, release = query_service(store, tmp_path)
    request = PublishedQueryRequest(releaseId=release["release_id"], idempotencyKey=uuid4().hex,
        mode="QUERY", metricResourceIds=["metric.sample.orders"], startTime="2026-10-01T00:00:00")
    accepted = service.submit_query(job["project_id"], request, "platform")
    saved = service.runtime.jobs.get(accepted["queryId"])["request_json"]
    saved.pop("businessTimezone")
    with store.db.transaction() as cursor:
        cursor.execute("UPDATE runtime_job SET request_json=%s WHERE job_id=%s", (Json(saved), accepted["queryId"]))
    equivalent = PublishedQueryRequest.model_validate({**request.model_dump(by_alias=True),
                                                       "startTime": "2026-09-30T16:00:00Z"})
    assert service.submit_query(job["project_id"], equivalent, "platform") == accepted
    assert service.runtime.jobs.get(accepted["queryId"])["request_json"] == saved
