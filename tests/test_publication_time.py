"""时间口径固定于发布与已受理查询，历史重试不改写引擎请求。"""

from uuid import uuid4

from dbt_metricflow_service.models.queries import QueryRequest as PublishedQueryRequest
from tests.test_publication_queries import query_service
from tests.test_publication_storage import store as store


def test_equivalent_offsets_share_idempotent_query_and_business_calendar(store, tmp_path):
    service, job, release = query_service(store, tmp_path)
    request = PublishedQueryRequest(
        idempotencyKey=uuid4().hex,
        mode="QUERY",
        metricResourceIds=["metric.sample.orders"],
        startTime="2026-09-30T16:00:00Z",
    )
    accepted = service.submit(release["build_id"], request, "platform")
    saved = service.jobs.get(accepted.query_id)
    assert saved["request_json"]["engineRequest"]["startTime"] == "2026-10-01T00:00:00"
    equivalent = PublishedQueryRequest.model_validate(
        {**request.model_dump(by_alias=True), "startTime": "2026-10-01T00:00:00+08:00"}
    )
    assert service.submit(release["build_id"], equivalent, "platform") == accepted
    assert saved["request_json"]["businessTimezone"] == "Asia/Shanghai"


def test_legacy_naive_calendar_is_preserved(store, tmp_path):
    service, job, release = query_service(store, tmp_path)
    request = PublishedQueryRequest(
        idempotencyKey=uuid4().hex,
        mode="QUERY",
        metricResourceIds=["metric.sample.orders"],
        startTime="2026-10-01T00:00:00",
    )
    receipt = service.submit(release["build_id"], request, "platform")
    saved = service.jobs.get(receipt.query_id)
    assert saved["request_json"]["engineRequest"]["startTime"] == "2026-10-01T00:00:00"
    assert saved["request_json"]["businessTimezone"] == "Asia/Shanghai"


def test_capabilities_disclose_project_path_and_time_contract(store, tmp_path):
    service, job, _ = query_service(store, tmp_path)
    request = PublishedQueryRequest(
        idempotencyKey=uuid4().hex, mode="QUERY", metricResourceIds=["metric.sample.orders"]
    )
    view = service.submit(service.fixture_options.build_id, request, "platform")
    assert view.business_timezone == "Asia/Shanghai"
    assert view.boundary_policy == "metricflow_granularity_alignment"


def test_query_error_identifies_invalid_dimension(store, tmp_path):
    import pytest

    from dbt_metricflow_service.application.errors import ServiceError as PublicationError

    service, job, release = query_service(store, tmp_path)
    request = PublishedQueryRequest(
        idempotencyKey=uuid4().hex,
        mode="QUERY",
        metricResourceIds=["metric.sample.orders"],
        groupBy=[{"optionId": "missing"}],
    )
    with pytest.raises(PublicationError) as caught:
        service.submit(release["build_id"], request, "platform")
    assert caught.value.error.code == "INVALID_DIMENSION_OPTION"


def test_historical_snapshot_without_timezone_retries_without_rewriting_engine(store, tmp_path):
    from psycopg2.extras import Json

    service, job, release = query_service(store, tmp_path)
    request = PublishedQueryRequest(
        idempotencyKey=uuid4().hex,
        mode="QUERY",
        metricResourceIds=["metric.sample.orders"],
        startTime="2026-10-01T00:00:00",
    )
    accepted = service.submit(release["build_id"], request, "platform")
    saved = service.jobs.get(accepted.query_id)["request_json"]
    saved.pop("businessTimezone")
    with store.db.transaction() as connection:
        connection.exec_driver_sql(
            "UPDATE runtime_job SET request_json=%s WHERE job_id=%s", (Json(saved), accepted.query_id)
        )
    equivalent = PublishedQueryRequest.model_validate(
        {**request.model_dump(by_alias=True), "startTime": "2026-09-30T16:00:00Z"}
    )
    assert service.submit(release["build_id"], equivalent, "platform") == accepted
    assert service.jobs.get(accepted.query_id)["request_json"] == saved
