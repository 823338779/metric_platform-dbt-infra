"""分页只切片一次执行的耐久结果，不重跑 SQL，也不丢失精度和空值。"""

from uuid import uuid4

import pytest

from dbt_metricflow_service.publications.errors import PublicationError
from dbt_metricflow_service.publications.models import PublishedQueryRequest
from tests.test_publication_queries import query_service
from tests.test_publication_storage import store as store


def queued(service, job, release):
    receipt = service.submit_query(job["project_id"], PublishedQueryRequest(
        releaseId=release["release_id"], idempotencyKey=uuid4().hex, mode="QUERY",
        metricResourceIds=["metric.sample.orders"]), "platform")
    return receipt["queryId"]


def finish(service, job, payload):
    child = service.runtime.jobs.claim(str(uuid4()), toolchain_version=job["toolchain_version"], kinds=["METRIC_QUERY"])
    service.runtime.jobs.finish(child["job_id"], child["lease_token"], payload)


def test_pages_preserve_rows_and_status_never_reads_payload(store, tmp_path, monkeypatch):
    service, job, release = query_service(store, tmp_path)
    assert hasattr(service, "query_status"), "lightweight query status is not implemented"
    query = queued(service, job, release)
    with pytest.raises(PublicationError) as pending:
        service.query_result_page(job["project_id"], query)
    assert pending.value.reason == "result_not_ready"
    rows = [[i, "12345678901234567890.123456789", None] for i in range(403)]
    finish(service, job, {"columns": [{"name": "value", "type": "string"}], "rows": rows, "truncated": True})
    original_result = service.runtime.jobs.result
    monkeypatch.setattr(service.runtime.jobs, "result", lambda *_: pytest.fail("status read payload_json"))
    assert service.query_status(job["project_id"], query)["resultAvailable"] is True
    monkeypatch.setattr(service.runtime.jobs, "result", original_result)
    collected, offset = [], 0
    while offset is not None:
        page = service.query_result_page(job["project_id"], query, offset, 200)
        assert page["availableRows"] == 403
        assert page["resultTruncated"] is True
        collected.extend(page["rows"])
        offset = page["nextOffset"]
    assert collected == rows
    assert service.query_result_page(job["project_id"], query, 999)["rows"] == []
    with pytest.raises(KeyError):
        service.query_status("wrong-project", query)


def test_large_row_is_explicit_error_and_small_page_advances_by_returned_rows(store, tmp_path):
    service, job, release = query_service(store, tmp_path)
    assert hasattr(service, "query_result_page"), "result pagination is not implemented"
    query = queued(service, job, release)
    finish(service, job, {"columns": [], "rows": [["x" * (9 * 1024 * 1024)]], "truncated": False})
    with pytest.raises(PublicationError) as too_large:
        service.query_result_page(job["project_id"], query)
    assert too_large.value.reason == "result_row_too_large"


def test_transport_shrinks_whole_rows_and_explain_preserves_sql(monkeypatch):
    import dbt_metricflow_service.publications.results as results

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
