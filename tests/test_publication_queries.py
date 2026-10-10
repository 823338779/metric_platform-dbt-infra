"""资源身份映射、选项约束与固定版本幂等。"""

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from threading import Barrier
from uuid import uuid4

import pytest

from dbt_metricflow_service.application.errors import ServiceError
from dbt_metricflow_service.application.queries import QueryService
from dbt_metricflow_service.models.queries import OptionsRequest as QueryOptionsRequest
from dbt_metricflow_service.models.queries import QueryRequest as PublishedQueryRequest
from dbt_metricflow_service.runtime.completion import complete_job
from tests.test_publication_catalog import build
from tests.test_publication_storage import store as store
from tests.test_v3_catalog_queries import ready_build


def query_service(store, tmp_path):
    from psycopg2.extras import Json

    from dbt_metricflow_service.application.catalog import CatalogService
    from tests.test_v3_catalog_queries import ready_build

    builds, view, jobs, artifacts = ready_build(store, tmp_path)
    with store.db.transaction() as connection:
        connection.exec_driver_sql(
            "UPDATE engine_build SET config_snapshot=config_snapshot || %s::jsonb WHERE build_id=%s",
            (Json({"businessTimezone": "Asia/Shanghai"}), str(view.build_id)),
        )
    record = builds.store.get(view.build_id)
    catalog = build().model_dump(mode="json", by_alias=True)
    catalogs = CatalogService(builds.store, artifacts)
    catalogs.read = lambda version: (builds.store.get(version), deepcopy(catalog))
    service = QueryService(builds, catalogs, jobs)
    request = QueryOptionsRequest(idempotencyKey=uuid4().hex, metricResourceIds=["metric.sample.orders"])
    task = service.submit_options(view.build_id, request, "platform")
    child = jobs.claim(str(uuid4()), toolchain_version=record["toolchain_version"], kinds=["QUERY_OPTIONS"])
    complete_job(
        jobs,
        child["job_id"],
        child["lease_token"],
        {
            "metrics": [{"name": "orders"}],
            "dimensions": [
                {"token": "order__region", "name": "region", "type": "categorical"},
                {"token": "return__region", "name": "region", "type": "categorical"},
            ],
            "timeDimensions": [{"token": "metric_time__month", "granularity": "month"}],
            "allowedFilters": ["=", "IN"],
        },
    )
    service.fixture_options = task
    return service, jobs.get(record["run_id"]), {"build_id": str(view.build_id)}


def test_options_are_versioned_and_do_not_merge_paths(store, tmp_path):
    service, job, release = query_service(store, tmp_path)
    options = service.get_options(service.fixture_options.options_task_id).options
    assert len({item["optionId"] for item in options}) == 3
    assert all("token" not in item for item in options)
    assert next(item for item in options if item["granularities"])["resourceId"] is None
    assert next(item for item in options if item["granularities"])["operators"] == []
    assert len({item["displayName"] for item in options}) == 3


@pytest.mark.parametrize(
    "extra",
    [
        {"datasetResourceId": "model.sample.orders"},
        {"dimensionOptionId": "unselected"},
    ],
)
def test_query_rejects_parameters_from_another_mode(store, tmp_path, extra):
    service, job, release = query_service(store, tmp_path)
    with pytest.raises(ValueError):
        PublishedQueryRequest(
            idempotencyKey=uuid4().hex, mode="QUERY", metricResourceIds=["metric.sample.orders"], **extra
        )


def test_query_pins_run_and_retry_survives_replacement(store, tmp_path):
    service, job, release = query_service(store, tmp_path)
    request = PublishedQueryRequest(
        idempotencyKey=uuid4().hex, mode="QUERY", metricResourceIds=["metric.sample.orders"]
    )
    accepted = service.submit(release["build_id"], request, "platform")
    saved = service.jobs.get(accepted.query_id)
    assert saved["parent_run_id"] == job["job_id"]
    assert saved["request_json"]["engineRequest"]["metrics"] == ["orders"]
    ready_build(store, tmp_path)
    assert service.submit(release["build_id"], request, "platform") == accepted
    assert (
        service.submit(
            release["build_id"], request.model_copy(update={"idempotency_key": uuid4().hex}), "platform"
        ).build_id
        == accepted.build_id
    )
    with pytest.raises(ServiceError):
        service.submit(release["build_id"], request.model_copy(update={"limit": 5}), "platform")


def test_concurrent_same_key_admits_one_query(store, tmp_path):
    service, job, release = query_service(store, tmp_path)
    request = PublishedQueryRequest(
        idempotencyKey=uuid4().hex, mode="QUERY", metricResourceIds=["metric.sample.orders"]
    )
    rendezvous = Barrier(2)
    original = service.catalogs.read

    def options(*args):
        result = original(*args)
        rendezvous.wait(timeout=5)
        return result

    service.catalogs.read = options
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(service.submit, release["build_id"], request, "platform") for _ in range(2)]
        results = [future.result(timeout=10) for future in futures]
    assert results[0] == results[1]


def test_retry_recovers_admission_committed_between_initial_lookup_and_version_check(store, tmp_path, monkeypatch):
    service, job, release = query_service(store, tmp_path)
    request = PublishedQueryRequest(
        idempotencyKey=uuid4().hex, mode="QUERY", metricResourceIds=["metric.sample.orders"]
    )
    accepted = service.submit(release["build_id"], request, "platform")
    ready_build(store, tmp_path)
    original = service.jobs.by_key
    lookups = 0

    def first_lookup_preceded_commit(*args):
        nonlocal lookups
        lookups += 1
        return None if lookups == 1 else original(*args)

    monkeypatch.setattr(service.jobs, "by_key", first_lookup_preceded_commit)
    assert service.submit(release["build_id"], request, "platform") == accepted
