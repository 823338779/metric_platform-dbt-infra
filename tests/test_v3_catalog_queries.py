"""历史目录与查询受理不读取当前部署指针。"""

import json
from uuid import uuid4

import pytest

from dbt_metricflow_service.runtime.completion import complete_job
from dbt_metricflow_service.storage.artifacts import ArtifactStore
from dbt_metricflow_service.storage.jobs import JobStore
from tests.test_publication_storage import store as store
from tests.test_v3_build_acceptance import CALLER, request, service


def ready_build(store, tmp_path, before_finish=None):
    app = service(store)
    app.toolchain = uuid4().hex
    build = app.submit(request(branchName="feature/" + uuid4().hex), CALLER)
    jobs = JobStore(store.db)
    run = app.store.get(build.build_id)["run_id"]
    job = jobs.claim(str(uuid4()), toolchain_version=app.toolchain, kinds=["BUILD_RUN"])
    while job["job_id"] != run:
        jobs.fail(job["job_id"], job["lease_token"], "TEST_FINISHED")
        job = jobs.claim(str(uuid4()), toolchain_version=app.toolchain, kinds=["BUILD_RUN"])
    directory = tmp_path / uuid4().hex
    target = directory / "target"
    target.mkdir(parents=True)
    (directory / "dbt_project.yml").write_text("name: sample\nversion: '1.0'\n", encoding="utf-8")
    artifacts = ArtifactStore(store.db)
    evidence = {
        "source_commit_sha": build.commit_sha,
        "project_digest": "a" * 64,
        "config_version": job["config_version"],
        "toolchain_version": job["toolchain_version"],
    }
    source = artifacts.capture(job["project_id"], directory, metadata=evidence)
    assert jobs.attach_input(run, job["lease_token"], source, project_digest=evidence["project_digest"])
    for name in ("manifest.json", "semantic_manifest.json", "catalog.json", "run_results.json"):
        (target / name).write_text("{}", encoding="utf-8")
    catalog = {
        "schemaVersion": 1,
        "projectId": job["project_id"],
        "releaseId": str(build.build_id),
        "resources": [
            {
                "resourceId": "metric.revenue",
                "nativeId": "metric.revenue",
                "attributes": {
                    "metricType": "simple",
                    "definitionSummary": "Revenue",
                    "inputMetricIds": [],
                    "datasetIds": [],
                    "timeConfig": {},
                },
                "name": "revenue",
                "displayName": "Revenue",
                "kind": "METRIC",
                "capabilities": ["QUERY"],
                "nativeDetails": {},
            }
        ],
        "relations": [],
        "relationBindings": [],
    }
    (target / "published_catalog.json").write_text(json.dumps(catalog), encoding="utf-8")
    output = artifacts.capture(
        job["project_id"],
        directory,
        kind="EXECUTION",
        producer_attempt_id=job["attempt_id"],
        metadata={
            **evidence,
            "source_set_id": source,
            "validation_json": {"allTestsPassed": True, "representativeQueryPassed": True, "relationsVerified": True},
            "catalog_json": catalog,
        },
    )
    if before_finish:
        before_finish(app, build, job, output)
    assert complete_job(jobs, run, job["lease_token"], output_set_id=output)
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
        assert app.cancel(build.build_id, CALLER)[1] == 202

    app, build, jobs, artifacts = ready_build(store, tmp_path, cancel)
    assert app.get(build.build_id).build_status == "CANCELLED"
    assert app.get(build.build_id).catalog_available is False
    assert app.cancel(build.build_id, CALLER)[1] == 200


def test_catalog_survives_physical_removal(store, tmp_path):
    from dbt_metricflow_service.application.catalog import CatalogService

    app, build, jobs, artifacts = ready_build(store, tmp_path)
    with store.db.transaction() as connection:
        connection.exec_driver_sql(
            "UPDATE runtime_job SET run_lifecycle='CLEANED' WHERE job_id=%s", (app.store.get(build.build_id)["run_id"],)
        )
    catalogs = CatalogService(app.store, artifacts)
    assert catalogs.list(build.build_id).items[0].resource_id == "metric.revenue"
    assert app.get(build.build_id).catalog_available
    assert not app.get(build.build_id).query_available


def test_cleanup_completion_preserves_catalog_and_source(store, tmp_path):
    from dbt_metricflow_service.application.catalog import CatalogService

    app, build, jobs, artifacts = ready_build(store, tmp_path)
    run = app.store.get(build.build_id)["run_id"]
    jobs.reserve_cleanup(run)
    cleanup = jobs.claim(str(uuid4()), toolchain_version=app.toolchain, kinds=["RUN_CLEANUP"])
    assert complete_job(jobs, cleanup["job_id"], cleanup["lease_token"], {})
    assert CatalogService(app.store, artifacts).list(build.build_id).items
    assert app.get(build.build_id).catalog_available
    assert app.get(build.build_id).query_unavailable_reason == "PHYSICAL_OBJECTS_REMOVED"


def test_option_scope_includes_build_and_metric_set(store, tmp_path):
    from dbt_metricflow_service.application.catalog import CatalogService
    from dbt_metricflow_service.application.queries import QueryService
    from dbt_metricflow_service.models.queries import OptionsRequest

    app, build, jobs, artifacts = ready_build(store, tmp_path)
    queries = QueryService(app, CatalogService(app.store, artifacts), jobs)
    body = OptionsRequest(idempotencyKey=str(uuid4()), metricResourceIds=["metric.revenue"])
    first = queries.submit_options(build.build_id, body, CALLER)
    assert first.options_task_id == queries.submit_options(build.build_id, body, CALLER).options_task_id
    assert first.build_id == build.build_id
    assert first.state == "QUEUED"


def test_historical_environment_missing_is_503(store, tmp_path):
    from dbt_metricflow_service.application.catalog import CatalogService
    from dbt_metricflow_service.application.errors import ServiceError
    from dbt_metricflow_service.application.queries import QueryService
    from dbt_metricflow_service.models.queries import OptionsRequest

    app, build, jobs, artifacts = ready_build(store, tmp_path)
    app.toolchain = "different"
    query = QueryService(app, CatalogService(app.store, artifacts), jobs)
    with pytest.raises(ServiceError) as failure:
        query.submit_options(
            build.build_id, OptionsRequest(idempotencyKey="o", metricResourceIds=["metric.revenue"]), CALLER
        )
    assert failure.value.status == 503


def test_result_metadata_and_explain_survive_without_loading_status_payload(store, tmp_path, monkeypatch):
    from dbt_metricflow_service.application.catalog import CatalogService
    from dbt_metricflow_service.application.queries import QueryService
    from dbt_metricflow_service.models.queries import QueryRequest

    app, build, jobs, artifacts = ready_build(store, tmp_path)
    queries = QueryService(app, CatalogService(app.store, artifacts), jobs)
    view = queries.submit(
        build.build_id,
        QueryRequest(idempotencyKey=uuid4().hex, mode="EXPLAIN", metricResourceIds=["metric.revenue"]),
        CALLER,
    )
    job = jobs.claim(str(uuid4()), toolchain_version=app.toolchain, kinds=["METRIC_QUERY"])
    assert complete_job(jobs, job["job_id"], job["lease_token"], {"sql": "select 1", "rows": [], "columns": []})
    assert queries.results(view.query_id).sql == "select 1"
    monkeypatch.setattr(jobs, "result", lambda *_: pytest.fail("status loaded result body"))
    assert queries.status(view.query_id).result_available
    with store.db.transaction() as connection:
        connection.exec_driver_sql("DELETE FROM runtime_job_result WHERE job_id=%s", (str(view.query_id),))
    assert not queries.status(view.query_id).result_available
