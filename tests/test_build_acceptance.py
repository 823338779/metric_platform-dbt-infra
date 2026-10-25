"""构建身份和队列必须先于 Git I/O 原子受理。"""

from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest

from tests.test_build_contract import build_body
from tests.test_publication_storage import store as store

CALLER = "test-client"
TOOLCHAIN = "v3-tests"


def service(store):
    from dbt_metricflow_service.application.builds import BuildService
    from dbt_metricflow_service.storage.builds import BuildStore

    builds = BuildStore(store.db)
    return BuildService(builds, TOOLCHAIN)


def request(**changes):
    from dbt_metricflow_service.models.execution import SnapshotBuildRequest as BuildRequest

    return BuildRequest.model_validate(build_body(idempotencyKey=uuid4().hex, **changes))


def test_retry_restores_before_resolving_inputs(store):
    from dbt_metricflow_service.application.errors import ServiceError

    app = service(store)
    body = request()
    first = app.submit_snapshot(body, CALLER)
    retry = app.submit_snapshot(body, CALLER)
    assert first.build_id == retry.build_id
    with pytest.raises(ServiceError) as failure:
        app.submit_snapshot(body.model_copy(update={"commit_sha": "b" * 40}), CALLER)
    assert failure.value.status == 409
    assert app.submit_snapshot(request(), CALLER).build_id != first.build_id


def test_acceptance_is_atomic_before_fetch(store):
    from dbt_metricflow_service.storage.jobs import JobStore

    app = service(store)
    body = request(deploymentPolicy="ON_SUCCESS")
    with ThreadPoolExecutor(max_workers=4) as pool:
        ids = list(pool.map(lambda _: app.submit_snapshot(body, CALLER).build_id, range(4)))
    assert len(set(ids)) == 1
    row = app.store.get(str(ids[0]))
    job = JobStore(store.db).get(row.run_id)
    assert job.input_set_id is None
    assert job.status == "QUEUED"
    assert str(ids[0]) != job.job_id
    assert app.snapshot_view(ids[0]).initial_deployment.generation >= 1


def test_build_only_does_not_allocate_generation(store):
    app = service(store)
    view = app.submit_snapshot(request(branchName="feature/" + uuid4().hex), CALLER)
    assert view.initial_deployment is None
    assert view.build_status == "QUEUED"
    assert view.catalog_available is False


def test_rejected_binding_creates_no_build(store):
    from pydantic import ValidationError
    from sqlalchemy import func, select

    from dbt_metricflow_service.storage.entities import Build

    with store.db.session() as session:
        before = session.scalar(select(func.count()).select_from(Build))
    with pytest.raises(ValidationError):
        service(store).submit_snapshot(request(executionBinding="missing"), CALLER)
    with store.db.session() as session:
        assert session.scalar(select(func.count()).select_from(Build)) == before


def test_request_schema_is_persisted_and_part_of_idempotency(store):
    from dbt_metricflow_service.application.errors import ServiceError
    from dbt_metricflow_service.storage.jobs import JobStore

    app = service(store)
    from tests.test_execution_snapshot_storage import snapshot

    schema = "caller_" + uuid4().hex
    body = request(execution=snapshot(schema=schema))
    first = app.submit_snapshot(body, CALLER)
    row = app.store.get(first.build_id)
    assert JobStore(store.db).get(row.run_id).schema_name == schema
    assert row.request_json.execution.schema_name == schema
    with pytest.raises(ServiceError) as failure:
        app.submit_snapshot(
            body.model_copy(update={"execution": body.execution.model_copy(update={"schema_name": "other_schema"})}),
            CALLER,
        )
    assert failure.value.status == 409
    second = app.submit_snapshot(request(execution=body.execution), CALLER)
    assert JobStore(store.db).get(app.store.get(second.build_id).run_id).schema_name == schema
