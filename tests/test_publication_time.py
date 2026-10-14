"""每次查询使用固定构建的业务时区。"""


from dbt_metricflow_service.models.queries import QueryRequest as PublishedQueryRequest
from tests.test_publication_queries import query_service
from tests.test_publication_storage import store as store


def test_equivalent_offsets_share_business_calendar_but_execute_separately(store, tmp_path):
    service, job, release = query_service(store, tmp_path)
    request = PublishedQueryRequest(
        mode="QUERY",
        metricResourceIds=["metric.sample.orders"],
        startTime="2026-09-30T16:00:00Z",
    )
    accepted = service.submit(release.build_id, request, "platform")
    saved = service.execute.calls[-1][1]
    assert saved.start_time.isoformat() == "2026-10-01T00:00:00"
    equivalent = PublishedQueryRequest.model_validate(
        {**request.model_dump(by_alias=True), "startTime": "2026-10-01T00:00:00+08:00"}
    )
    count = len(service.execute.calls)
    result = service.submit(release.build_id, equivalent, "platform")
    assert result.normalized_time_range == accepted.normalized_time_range
    assert len(service.execute.calls) == count + 1
    assert service.execute.calls[-1][0].config_snapshot.business_timezone == "Asia/Shanghai"


def test_naive_business_calendar_is_preserved(store, tmp_path):
    service, job, release = query_service(store, tmp_path)
    request = PublishedQueryRequest(
        mode="QUERY",
        metricResourceIds=["metric.sample.orders"],
        startTime="2026-10-01T00:00:00",
    )
    service.submit(release.build_id, request, "platform")
    saved = service.execute.calls[-1][1]
    assert saved.start_time.isoformat() == "2026-10-01T00:00:00"
    assert service.execute.calls[-1][0].config_snapshot.business_timezone == "Asia/Shanghai"


def test_capabilities_disclose_project_path_and_time_contract(store, tmp_path):
    service, job, _ = query_service(store, tmp_path)
    request = PublishedQueryRequest(
        mode="QUERY", metricResourceIds=["metric.sample.orders"]
    )
    view = service.submit(service.fixture_available_dimensions.build_id, request, "platform")
    assert view.business_timezone == "Asia/Shanghai"
    assert view.boundary_policy == "metricflow_granularity_alignment"


def test_query_error_identifies_invalid_dimension(store, tmp_path):
    import pytest

    from dbt_metricflow_service.application.errors import ServiceError as PublicationError

    service, job, release = query_service(store, tmp_path)
    request = PublishedQueryRequest(
        mode="QUERY",
        metricResourceIds=["metric.sample.orders"],
        groupBy=[{"optionId": "missing"}],
    )
    with pytest.raises(PublicationError) as caught:
        service.submit(release.build_id, request, "platform")
    assert caught.value.error.code == "INVALID_DIMENSION_OPTION"
