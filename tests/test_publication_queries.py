"""资源身份映射、选项约束与固定版本实时查询。"""

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from threading import Barrier

import pytest

from dbt_metricflow_service.application.queries import QueryService
from dbt_metricflow_service.models.queries import AvailableDimensionsRequest
from dbt_metricflow_service.models.queries import QueryRequest as PublishedQueryRequest
from tests.synchronous_queries import inline_executor
from tests.test_catalog_queries import ready_build
from tests.test_publication_catalog import build
from tests.test_publication_storage import store as store


def query_service(store, tmp_path):
    from psycopg2.extras import Json

    from dbt_metricflow_service.application.catalog import CatalogService
    from tests.test_catalog_queries import ready_build

    builds, view, jobs, artifacts = ready_build(store, tmp_path)
    with store.db.transaction() as connection:
        connection.exec_driver_sql(
            "UPDATE engine_build SET config_snapshot=config_snapshot || %s::jsonb WHERE build_id=%s",
            (Json({"businessTimezone": "Asia/Shanghai"}), str(view.build_id)),
        )
    record = builds.store.get(view.build_id)
    catalog = build().model_dump(mode="json", by_alias=True)
    catalogs = CatalogService(builds.store, artifacts)
    catalogs.read_build = lambda build: deepcopy(catalog)
    service = QueryService(builds, catalogs, inline_executor())
    request = AvailableDimensionsRequest(metricResourceIds=["metric.sample.orders"])
    service.fixture_available_dimensions = service.available_dimensions(view.build_id, request, "platform")
    return service, jobs.get(record["run_id"]), {"build_id": str(view.build_id)}


def test_available_dimensions_are_versioned_and_do_not_merge_paths(store, tmp_path):
    service, job, release = query_service(store, tmp_path)
    dimensions = service.fixture_available_dimensions.dimensions
    assert len({item["optionId"] for item in dimensions}) == 3
    assert all("token" not in item for item in dimensions)
    assert next(item for item in dimensions if item["granularities"])["resourceId"] is None
    assert next(item for item in dimensions if item["granularities"])["operators"] == []
    assert len({item["displayName"] for item in dimensions}) == 3


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
            mode="QUERY", metricResourceIds=["metric.sample.orders"], **extra
        )


def test_query_pins_run_and_reexecutes_after_replacement(store, tmp_path):
    service, job, release = query_service(store, tmp_path)
    request = PublishedQueryRequest(mode="QUERY", metricResourceIds=["metric.sample.orders"])
    first = service.submit(release["build_id"], request, "platform")
    assert service.execute.calls[-1][0]["run_id"] == job["job_id"]
    assert service.execute.calls[-1][1]["request"]["metrics"] == ["orders"]
    count = len(service.execute.calls)
    ready_build(store, tmp_path)
    second = service.submit(release["build_id"], request, "platform")
    assert second.build_id == first.build_id
    assert len(service.execute.calls) == count + 1
    service.submit(release["build_id"], request.model_copy(update={"limit": 5}), "platform")
    assert service.execute.calls[-1][1]["request"]["limit"] == 5


def test_concurrent_identical_requests_both_execute(store, tmp_path):
    service, job, release = query_service(store, tmp_path)
    request = PublishedQueryRequest(mode="QUERY", metricResourceIds=["metric.sample.orders"])
    rendezvous = Barrier(2)
    original = service.catalogs.read_build

    def read_catalog(*args):
        result = original(*args)
        rendezvous.wait(timeout=5)
        return result

    service.catalogs.read_build = read_catalog
    count = len(service.execute.calls)
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(service.submit, release["build_id"], request, "platform") for _ in range(2)]
        results = [future.result(timeout=10) for future in futures]
    assert all(result.build_id == results[0].build_id for result in results)
    assert len(service.execute.calls) == count + 2
