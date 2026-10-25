"""历史目录与查询受理不读取当前部署指针。"""

import json
from uuid import uuid4

import pytest

from dbt_metricflow_service.storage.artifacts import ArtifactStore
from dbt_metricflow_service.storage.jobs import JobStore
from dbt_metricflow_service.storage.records import ArtifactMetadata
from tests.synchronous_queries import inline_executor
from tests.test_build_acceptance import CALLER, request, service
from tests.test_publication_storage import store as store


def ready_build(store, tmp_path, before_finish=None):
    app = service(store)
    app.toolchain = uuid4().hex
    build = app.submit_snapshot(request(branchName="feature/" + uuid4().hex), CALLER)
    jobs = JobStore(store.db)
    run = app.store.get(build.build_id).run_id
    job = jobs.claim(str(uuid4()), toolchain_version=app.toolchain, kinds=["BUILD_RUN"])
    while job.job_id != run:
        jobs.fail(job.job_id, job.lease_token, "TEST_FINISHED")
        job = jobs.claim(str(uuid4()), toolchain_version=app.toolchain, kinds=["BUILD_RUN"])
    directory = tmp_path / uuid4().hex
    target = directory / "target"
    target.mkdir(parents=True)
    (directory / "dbt_project.yml").write_text("name: sample\nversion: '1.0'\n", encoding="utf-8")
    artifacts = ArtifactStore(store.db)
    evidence = {
        "source_commit_sha": build.commit_sha,
        "project_digest": "a" * 64,
        "config_version": job.config_version,
        "toolchain_version": job.toolchain_version,
    }
    source = artifacts.capture(job.project_id, directory, metadata=ArtifactMetadata.model_validate(evidence))
    assert jobs.attach_input(run, job.lease_token, source, project_digest=evidence["project_digest"])
    from tests.test_publication_catalog import package

    manifest = package()
    manifest["metrics"] = {"metric.revenue": {
        "name": "revenue", "label": "Revenue", "type": "simple",
        "type_params": {"metric_aggregation_params": {"semantic_model": "orders", "agg": "sum"}},
        "depends_on": {"nodes": ["semantic_model.sample.orders"]},
    }}
    manifest["parent_map"] = {"metric.revenue": ["semantic_model.sample.orders"]}
    manifest["child_map"] = {"semantic_model.sample.orders": ["metric.revenue"]}
    (target / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    output = artifacts.capture(
        job.project_id,
        directory,
        kind="EXECUTION",
        producer_attempt_id=job.attempt_id,
        metadata=ArtifactMetadata(**evidence,
            source_set_id=source,
        ),
    )
    if before_finish:
        before_finish(app, build, job, output)
    assert jobs.finish(run, job.lease_token, output_set_id=output)
    return app, build, jobs, artifacts


def test_completion_rejects_output_from_other_source(store, tmp_path):
    def mismatch(app, build, job, output):
        with store.db.transaction() as connection:
            connection.exec_driver_sql(
                "UPDATE runtime_artifact_set SET source_commit_sha=%s WHERE set_id=%s", ("b" * 40, output)
            )

    with pytest.raises(ValueError, match="fixed source evidence"):
        ready_build(store, tmp_path, mismatch)


def test_cancel_before_finish_has_one_terminal_state(store, tmp_path):
    def cancel(app, build, job, output):
        assert app.cancel_snapshot(build.build_id, CALLER)[1] == 202

    app, build, jobs, artifacts = ready_build(store, tmp_path, cancel)
    assert app.snapshot_view(build.build_id).build_status == "CANCELLED"
    assert app.snapshot_view(build.build_id).catalog_available is False
    assert app.cancel_snapshot(build.build_id, CALLER)[1] == 200


def test_catalog_survives_physical_removal(store, tmp_path):
    from dbt_metricflow_service.application.catalog import CatalogService

    app, build, jobs, artifacts = ready_build(store, tmp_path)
    with store.db.transaction() as connection:
        connection.exec_driver_sql(
            "UPDATE runtime_job SET run_lifecycle='CLEANED' WHERE job_id=%s", (app.store.get(build.build_id).run_id,)
        )
    catalogs = CatalogService(app.store, artifacts)
    assert "metric.revenue" in catalogs.list(build.build_id)["metrics"]
    assert app.snapshot_view(build.build_id).catalog_available
    assert not app.snapshot_view(build.build_id).query_available


@pytest.mark.parametrize("lifecycle", ["CLEANING", "CLEANED"])
def test_build_retry_preserves_physical_availability(store, tmp_path, lifecycle):

    app, build, jobs, artifacts = ready_build(store, tmp_path)
    row = app.store.get(build.build_id)
    with store.db.transaction() as connection:
        connection.exec_driver_sql(
            "UPDATE runtime_job SET run_lifecycle=%s WHERE job_id=%s", (lifecycle, row.run_id),
        )
    retry = app.submit_snapshot(row.request_json, CALLER)
    assert retry.build_id == build.build_id
    assert retry.catalog_available
    assert not retry.query_available
    assert retry.query_unavailable_reason == "PHYSICAL_OBJECTS_REMOVED"
    assert retry == app.snapshot_view(build.build_id)


def test_cleanup_completion_preserves_catalog_and_source(store, tmp_path):
    from dbt_metricflow_service.application.catalog import CatalogService

    app, build, jobs, artifacts = ready_build(store, tmp_path)
    run = app.store.get(build.build_id).run_id
    jobs.reserve_cleanup(run)
    cleanup = jobs.claim(str(uuid4()), toolchain_version=app.toolchain, kinds=["RUN_CLEANUP"])
    assert jobs.finish(cleanup.job_id, cleanup.lease_token)
    assert CatalogService(app.store, artifacts).list(build.build_id)["metrics"]
    assert app.snapshot_view(build.build_id).catalog_available
    assert app.snapshot_view(build.build_id).query_unavailable_reason == "PHYSICAL_OBJECTS_REMOVED"


def test_option_scope_includes_build_and_metric_set(store, tmp_path):
    from dbt_metricflow_service.application.catalog import CatalogService
    from dbt_metricflow_service.application.queries import QueryService
    from dbt_metricflow_service.models.queries import AvailableDimensionsRequest

    app, build, jobs, artifacts = ready_build(store, tmp_path)
    execute = inline_executor()
    queries = QueryService(app, CatalogService(app.store, artifacts), execute, execute.discover_dimensions)
    body = AvailableDimensionsRequest(metricResourceIds=["metric.revenue"])
    first = queries.available_dimensions(build.build_id, body, CALLER)
    assert first.dimensions == queries.available_dimensions(build.build_id, body, CALLER).dimensions
    assert first.build_id == build.build_id
    assert first.dimensions


def test_historical_environment_missing_is_503(store, tmp_path):
    from dbt_metricflow_service.application.catalog import CatalogService
    from dbt_metricflow_service.application.errors import ServiceError
    from dbt_metricflow_service.application.queries import QueryService
    from dbt_metricflow_service.models.queries import AvailableDimensionsRequest

    app, build, jobs, artifacts = ready_build(store, tmp_path)
    app.toolchain = "different"
    execute = inline_executor()
    query = QueryService(app, CatalogService(app.store, artifacts), execute, execute.discover_dimensions)
    with pytest.raises(ServiceError) as failure:
        query.available_dimensions(
            build.build_id, AvailableDimensionsRequest(metricResourceIds=["metric.revenue"]), CALLER
        )
    assert failure.value.status == 503


def test_explain_returns_in_memory_without_result_store(store, tmp_path, monkeypatch):
    from dbt_metricflow_service.application.catalog import CatalogService
    from dbt_metricflow_service.application.queries import QueryService
    from dbt_metricflow_service.models.queries import QueryRequest

    app, build, jobs, artifacts = ready_build(store, tmp_path)
    monkeypatch.setattr(jobs, "reserve", lambda *_: pytest.fail("query created a task"))
    execute = inline_executor()
    queries = QueryService(app, CatalogService(app.store, artifacts), execute, execute.discover_dimensions)
    result = queries.submit(build.build_id, QueryRequest(mode="EXPLAIN", metricResourceIds=["metric.revenue"]), CALLER)
    assert result.sql == "select 1"
    assert not hasattr(result, "query_id")
