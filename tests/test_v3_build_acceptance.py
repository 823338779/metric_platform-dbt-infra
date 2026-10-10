"""构建身份和队列必须先于 Git I/O 原子受理。"""

from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest

from tests.test_publication_storage import store as store
from tests.test_v3_contract import build_body

CALLER = "test-client"
TOOLCHAIN = "v3-tests"


def service(store):
    from dbt_metricflow_service.application.builds import BuildService
    from dbt_metricflow_service.storage.builds import BuildStore

    builds = BuildStore(store.db)
    builds.register_binding(
        build_body()["repository"],
        "warehouse",
        "1",
        {"profileBindingId": "postgres", "businessTimezone": "UTC", "environments": ["PREVIEW", "PRODUCTION"]},
    )
    return BuildService(builds, TOOLCHAIN, 600)


def request(**changes):
    from dbt_metricflow_service.models.builds import BuildRequest

    return BuildRequest.model_validate(build_body(idempotencyKey=uuid4().hex, **changes))


def test_retry_restores_before_resolving_inputs(store):
    from dbt_metricflow_service.application.errors import ServiceError

    app = service(store)
    body = request()
    first = app.submit(body, CALLER)
    retry = app.submit(body, CALLER)
    assert first.build_id == retry.build_id
    with pytest.raises(ServiceError) as failure:
        app.submit(body.model_copy(update={"commit_sha": "b" * 40}), CALLER)
    assert failure.value.status == 409
    assert app.submit(request(), CALLER).build_id != first.build_id


def test_acceptance_is_atomic_before_fetch(store):
    from dbt_metricflow_service.storage.jobs import JobStore

    app = service(store)
    body = request(deploymentPolicy="ON_SUCCESS")
    with ThreadPoolExecutor(max_workers=4) as pool:
        ids = list(pool.map(lambda _: app.submit(body, CALLER).build_id, range(4)))
    assert len(set(ids)) == 1
    row = app.store.get(str(ids[0]))
    job = JobStore(store.db).get(row["run_id"])
    assert job["input_set_id"] is None
    assert job["status"] == "QUEUED"
    assert str(ids[0]) != job["job_id"]
    assert app.get(ids[0]).initial_deployment["generation"] >= 1


def test_build_only_does_not_allocate_generation(store):
    app = service(store)
    view = app.submit(request(branchName="feature/" + uuid4().hex), CALLER)
    assert view.initial_deployment is None
    assert view.build_status == "QUEUED"
    assert view.catalog_available is False


def test_rejected_binding_creates_no_build(store):
    from dbt_metricflow_service.application.errors import ServiceError

    app = service(store)
    with pytest.raises(ServiceError) as failure:
        app.submit(request(executionBinding="missing"), CALLER)
    assert failure.value.status == 422
