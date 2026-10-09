"""资源身份映射、选项约束与固定版本幂等。"""

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from threading import Barrier
from types import SimpleNamespace
from uuid import uuid4

import pytest

from dbt_metricflow_service.publications.models import PublishedQueryRequest, QueryOptionsRequest
from dbt_metricflow_service.publications.queries import QueryService
from dbt_metricflow_service.publications.service import ReleaseGone
from dbt_metricflow_service.runtime.completion import complete_job
from dbt_metricflow_service.storage.jobs import StoreConflict
from tests.test_publication_catalog import build
from tests.test_publication_storage import store as store
from tests.test_publication_transaction import prepared


def query_service(store, tmp_path):
    jobs, job, release, output, artifacts = prepared(store, tmp_path)
    complete_job(jobs, job["job_id"], job["lease_token"], output_set_id=output)
    catalog = build().model_dump(mode="json", by_alias=True)
    catalog["projectId"], catalog["releaseId"] = job["project_id"], release["release_id"]
    runtime = SimpleNamespace(db=store.db, jobs=jobs, artifacts=artifacts,
                              settings=SimpleNamespace(command_timeout_seconds=60))
    runtime.options = lambda run, metrics: {
        "metrics": [{"name": name} for name in metrics],
        "dimensions": [{"token": "order__region", "name": "region", "type": "categorical"},
                       {"token": "return__region", "name": "region", "type": "categorical"}],
        "timeDimensions": [{"token": "metric_time__month", "granularity": "month"}],
        "allowedFilters": ["=", "IN"],
    }
    service = QueryService(runtime)
    original = service.publications._active_release
    service.catalogs._catalog = lambda project, version: (original(project, version), deepcopy(catalog))
    return service, job, release


def test_options_are_versioned_and_do_not_merge_paths(store, tmp_path):
    service, job, release = query_service(store, tmp_path)
    request = QueryOptionsRequest(releaseId=release["release_id"], metricResourceIds=["metric.sample.orders"])
    options = service.query_options(job["project_id"], request)["options"]
    assert len({item["optionId"] for item in options}) == 3
    assert all("token" not in item for item in options)
    assert next(item for item in options if item["granularities"])["resourceId"] is None
    assert next(item for item in options if item["granularities"])["operators"] == []
    assert len({item["displayName"] for item in options}) == 3


@pytest.mark.parametrize("extra", [
    {"datasetResourceId": "model.sample.orders"},
    {"dimensionOptionId": "unselected"},
])
def test_query_rejects_parameters_from_another_mode(store, tmp_path, extra):
    service, job, release = query_service(store, tmp_path)
    request = PublishedQueryRequest(releaseId=release["release_id"], idempotencyKey=uuid4().hex,
                                    mode="QUERY", metricResourceIds=["metric.sample.orders"], **extra)
    with pytest.raises(ValueError):
        service.submit_query(job["project_id"], request, "platform")


def test_query_pins_run_and_retry_survives_replacement(store, tmp_path):
    service, job, release = query_service(store, tmp_path)
    request = PublishedQueryRequest(releaseId=release["release_id"], idempotencyKey=uuid4().hex,
                                    mode="QUERY", metricResourceIds=["metric.sample.orders"])
    accepted = service.submit_query(job["project_id"], request, "platform")
    saved = service.runtime.jobs.get(accepted["queryId"])
    assert saved["parent_run_id"] == job["job_id"]
    assert saved["request_json"]["engineRequest"]["metrics"] == ["orders"]
    next_jobs, next_job, _, output, _ = prepared(store, tmp_path, job["project_id"])
    complete_job(next_jobs, next_job["job_id"], next_job["lease_token"], output_set_id=output)
    assert service.submit_query(job["project_id"], request, "platform") == accepted
    with pytest.raises(ReleaseGone):
        service.submit_query(job["project_id"], request.model_copy(update={"idempotency_key": uuid4().hex}), "platform")
    with pytest.raises(StoreConflict):
        service.submit_query(job["project_id"], request.model_copy(update={"limit": 5}), "platform")


def test_concurrent_same_key_admits_one_query(store, tmp_path):
    service, job, release = query_service(store, tmp_path)
    request = PublishedQueryRequest(releaseId=release["release_id"], idempotencyKey=uuid4().hex,
                                    mode="QUERY", metricResourceIds=["metric.sample.orders"])
    rendezvous = Barrier(2)
    original = service._options

    def options(*args):
        result = original(*args)
        rendezvous.wait(timeout=5)
        return result

    service._options = options
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(service.submit_query, job["project_id"], request, "platform") for _ in range(2)]
        results = [future.result(timeout=10) for future in futures]
    assert results[0] == results[1]


def test_retry_recovers_admission_committed_between_initial_lookup_and_version_check(store, tmp_path, monkeypatch):
    service, job, release = query_service(store, tmp_path)
    request = PublishedQueryRequest(releaseId=release["release_id"], idempotencyKey=uuid4().hex,
                                    mode="QUERY", metricResourceIds=["metric.sample.orders"])
    accepted = service.submit_query(job["project_id"], request, "platform")
    jobs, candidate, _, output, _ = prepared(store, tmp_path, job["project_id"])
    complete_job(jobs, candidate["job_id"], candidate["lease_token"], output_set_id=output)
    original = service.runtime.jobs.by_key
    lookups = 0

    def first_lookup_preceded_commit(*args):
        nonlocal lookups
        lookups += 1
        return None if lookups == 1 else original(*args)

    monkeypatch.setattr(service.runtime.jobs, "by_key", first_lookup_preceded_commit)
    assert service.submit_query(job["project_id"], request, "platform") == accepted
