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


def ready_build(store, tmp_path, before_finish=None, **build_changes):
    app = service(store)
    app.toolchain = uuid4().hex
    build = app.submit_snapshot(request(**{"branchName": "feature/" + uuid4().hex, **build_changes}), CALLER)
    jobs = JobStore(store.db)
    run = app.store.get(build.build_id).run_id
    job = jobs.claim(str(uuid4()), toolchain_version=app.toolchain)
    while job.job_id != run:
        jobs.fail(job.job_id, job.lease_token, "TEST_FINISHED")
        job = jobs.claim(str(uuid4()), toolchain_version=app.toolchain)
    directory = tmp_path / uuid4().hex
    target = directory / "target"
    target.mkdir(parents=True)
    (directory / "dbt_project.yml").write_text("name: sample\nversion: '1.0'\nprofile: sample\n", encoding="utf-8")
    (directory / "models").mkdir()
    (directory / "models" / "orders.yml").write_text("models: []", encoding="utf-8")
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
    metric = manifest["metrics"]["metric.sample.orders"]
    metric.update(name="revenue", unique_id="metric.revenue")
    manifest["metrics"] = {"metric.revenue": metric}
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


def test_legacy_cancel_flag_does_not_override_completion(tmp_path):
    from types import SimpleNamespace

    from tests.schema_helpers import isolated_database

    # 升级保留旧列，但运行时代码不能再读取取消意图或改变构建终态。
    with isolated_database() as db:
        db.initialize()
        with db.transaction() as connection:
            connection.exec_driver_sql(
                "ALTER TABLE engine_build ADD COLUMN cancel_requested boolean NOT NULL DEFAULT false"
            )

        def legacy_cancel(app, build, job, output):
            with db.transaction() as connection:
                connection.exec_driver_sql(
                    "UPDATE engine_build SET cancel_requested=true WHERE build_id=%s", (str(build.build_id),)
                )

        app, build, jobs, artifacts = ready_build(SimpleNamespace(db=db), tmp_path, legacy_cancel)
        detail = app.snapshot_view(build.build_id)
        assert detail.build_status == "SUCCEEDED"
        assert detail.catalog_available is True
        assert "cancelRequested" not in detail.model_dump(by_alias=True)


def test_option_scope_includes_build_and_metric_set(store, tmp_path):
    from dbt_metricflow_service.application.queries import QueryService
    from dbt_metricflow_service.models.queries import AvailableDimensionsRequest

    app, build, jobs, artifacts = ready_build(store, tmp_path)
    execute = inline_executor()
    queries = QueryService(app, execute, execute.discover_dimensions)
    body = AvailableDimensionsRequest(metrics=["revenue"])
    first = queries.available_dimensions(build.build_id, body, CALLER)
    assert first.dimensions == queries.available_dimensions(build.build_id, body, CALLER).dimensions
    assert first.build_id == build.build_id
    assert first.dimensions


def test_historical_toolchain_does_not_block_query(store, tmp_path):
    from dbt_metricflow_service.application.queries import QueryService
    from dbt_metricflow_service.models.queries import AvailableDimensionsRequest

    app, build, jobs, artifacts = ready_build(store, tmp_path)
    app.toolchain = "different"
    execute = inline_executor()
    query = QueryService(app, execute, execute.discover_dimensions)
    result = query.available_dimensions(
        build.build_id, AvailableDimensionsRequest(metrics=["revenue"]), CALLER,
    )
    assert result.dimensions



def test_explain_returns_in_memory_without_result_store(store, tmp_path, monkeypatch):
    from dbt_metricflow_service.application.queries import QueryService
    from dbt_metricflow_service.models.queries import QueryRequest

    app, build, jobs, artifacts = ready_build(store, tmp_path)
    monkeypatch.setattr(jobs, "reserve", lambda *_: pytest.fail("query created a task"))
    execute = inline_executor()
    queries = QueryService(app, execute, execute.discover_dimensions)
    result = queries.submit(build.build_id, QueryRequest(mode="EXPLAIN", metrics=["revenue"]), CALLER)
    assert result.sql == "select 1"
    assert not hasattr(result, "query_id")
