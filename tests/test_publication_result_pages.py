"""分页只切片一次执行的耐久结果，不重跑 SQL，也不丢失精度和空值。"""

from uuid import uuid4

import pytest

from dbt_metricflow_service.application.errors import ServiceError as PublicationError
from dbt_metricflow_service.models.queries import QueryRequest as PublishedQueryRequest
from dbt_metricflow_service.runtime.completion import complete_job
from tests.test_publication_queries import query_service
from tests.test_publication_storage import store as store


def queued(service, job, release):
    receipt = service.submit(release["build_id"], PublishedQueryRequest(
         idempotencyKey=uuid4().hex, mode="QUERY",
        metricResourceIds=["metric.sample.orders"]), "platform")
    return receipt.query_id


def finish(service, job, payload):
    child = service.jobs.claim(str(uuid4()), toolchain_version=job["toolchain_version"], kinds=["METRIC_QUERY"])
    complete_job(service.jobs, child["job_id"], child["lease_token"], payload)


def test_pages_preserve_rows_and_status_never_reads_payload(store, tmp_path, monkeypatch):
    service, job, release = query_service(store, tmp_path)
    assert hasattr(service, "status"), "lightweight query status is not implemented"
    query = queued(service, job, release)
    with pytest.raises(PublicationError) as pending:
        service.results(query)
    assert pending.value.error.code == "RESULT_NOT_READY"
    rows = [[i, "12345678901234567890.123456789", None] for i in range(403)]
    finish(service, job, {"columns": [{"name": "value", "type": "string"}], "rows": rows, "truncated": True})
    original_result = service.jobs.result
    monkeypatch.setattr(service.jobs, "result", lambda *_: pytest.fail("status read payload_json"))
    assert service.status(query).model_dump(mode="json", by_alias=True)["resultAvailable"] is True
    monkeypatch.setattr(service.jobs, "result", original_result)
    collected, offset = [], 0
    while offset is not None:
        page = service.results(query, offset, 200).model_dump(by_alias=True)
        assert page["availableRows"] == 403
        assert page["resultTruncated"] is True
        collected.extend(page["rows"])
        offset = page["nextOffset"]
    assert collected == rows
    assert service.results(query, 999).rows == []
    with pytest.raises(PublicationError):
        service.status(uuid4())


def test_large_row_is_explicit_error_and_small_page_advances_by_returned_rows(store, tmp_path):
    service, job, release = query_service(store, tmp_path)
    assert hasattr(service, "results"), "result pagination is not implemented"
    query = queued(service, job, release)
    finish(service, job, {"columns": [], "rows": [["x" * (9 * 1024 * 1024)]], "truncated": False})
    with pytest.raises(PublicationError) as too_large:
        service.results(query)
    assert too_large.value.error.code == "RESULT_TOO_LARGE"


def test_transport_shrinks_whole_rows_and_explain_preserves_sql(monkeypatch):
    import dbt_metricflow_service.platform.results as results

    monkeypatch.setattr(results, "MAX_PAGE_BYTES", 300)
    rows = [["x" * 100], ["y" * 100]]
    first = results.result_page({"rows": rows, "columns": []}, {}, 0, 200)
    assert first["rows"] == rows[:1]
    assert first["nextOffset"] == 1
    second = results.result_page({"rows": rows, "columns": []}, {}, first["nextOffset"], 200)
    assert first["rows"] + second["rows"] == rows
    explained = results.result_page({"sql": "select 1", "rows": [], "columns": []}, {}, 0, 100)
    assert explained["sql"] == "select 1"
    assert explained["nextOffset"] is None
