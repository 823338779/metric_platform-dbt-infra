"""真实 PostgreSQL 验证观察快照、调度公平性和事实事务的锁顺序。"""

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Event, current_thread
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import event

from dbt_metricflow_service.models.deployments import DeploymentKey
from dbt_metricflow_service.storage.deployments import DeploymentStore
from tests.schema_helpers import isolated_database
from tests.test_publication_storage import store as store
from tests.test_v3_build_acceptance import CALLER, request, service
from tests.test_v3_deployments import deployment_service


@pytest.mark.parametrize("head", [None, "a" * 40])
def test_observation_does_not_overwrite_concurrent_deployment(store, head):
    app = service(store)
    branch = "feature/" + uuid4().hex
    first = app.submit(request(branchName=branch, deploymentPolicy="ON_SUCCESS"), CALLER)
    second = app.submit(request(branchName=branch, commitSha="b" * 40), CALLER)
    key = DeploymentKey(repository=first.repository, environment=first.environment, branchName=branch)
    update = """UPDATE engine_deployment_target SET active_build_id=%s,version=version+1,
        observed_head_sha=%s,source_state='MATCHED' WHERE repository=%s AND branch_name=%s"""
    with store.db.transaction() as connection:
        connection.exec_driver_sql(update, (str(first.build_id), first.commit_sha, first.repository, branch))

    def concurrent_switch(*_):
        with store.db.transaction() as connection:
            connection.exec_driver_sql(update, (str(second.build_id), second.commit_sha, first.repository, branch))
        return head

    deploy = deployment_service(store, head)
    deploy.head_reader = concurrent_switch
    view = deploy.current(key)
    assert view.active_build_id == second.build_id
    assert view.active_commit_sha == second.commit_sha
    assert view.observed_head_sha == second.commit_sha
    assert view.source_state == "MATCHED"


def test_pending_scan_reaches_beyond_first_unresolved_batch():
    with isolated_database() as db:
        db.initialize()
        app = service(SimpleNamespace(db=db))
        for _ in range(101):
            app.submit(request(branchName="feature/" + uuid4().hex, deploymentPolicy="ON_SUCCESS"), CALLER)
        deployments = DeploymentStore(db)
        first = deployments.pending()
        second = deployments.pending()
        assert len(first) == len(second) == 100
        assert len({row["build_id"] for row in first + second}) == 101


def test_build_acceptance_and_observation_have_consistent_lock_order(store, monkeypatch):
    app = service(store)
    branch = "feature/" + uuid4().hex
    first = app.submit(request(branchName=branch, deploymentPolicy="ON_SUCCESS"), CALLER)
    key = DeploymentKey(repository=first.repository, environment=first.environment, branchName=branch)
    deploy = deployment_service(store, first.commit_sha)
    build_inserted, observer_started, observer_locked = Event(), Event(), Event()
    original = DeploymentStore.accept_in_transaction

    def paused_accept(self, *args, **kwargs):
        # 插入构建已持有变化计数器；观察若先持有目标锁，就会产生真实数据库死锁。
        build_inserted.set()
        assert observer_started.wait(3)
        observer_locked.wait(0.5)
        return original(self, *args, **kwargs)

    def after_statement(conn, cursor, statement, parameters, context, executemany):
        if (current_thread().name.startswith("observer") and statement.lstrip().startswith("SELECT")
                and "engine_deployment_target" in statement and "FOR UPDATE" in statement):
            observer_locked.set()

    def observe():
        assert build_inserted.wait(3)
        observer_started.set()
        return deploy.current(key)

    monkeypatch.setattr(DeploymentStore, "accept_in_transaction", paused_accept)
    event.listen(store.db.engine, "after_cursor_execute", after_statement)
    try:
        with ThreadPoolExecutor(max_workers=1, thread_name_prefix="builder") as builders:
            with ThreadPoolExecutor(max_workers=1, thread_name_prefix="observer") as observers:
                built = builders.submit(app.submit, request(branchName=branch, deploymentPolicy="ON_SUCCESS"), CALLER)
                observed = observers.submit(observe)
                assert built.result(timeout=8).initial_deployment["generation"] == 2
                assert observed.result(timeout=8).branch_name == branch
    finally:
        event.remove(store.db.engine, "after_cursor_execute", after_statement)


def test_query_and_cleanup_acceptance_are_atomic(store, tmp_path, monkeypatch):
    from dbt_metricflow_service.application.catalog import CatalogService
    from dbt_metricflow_service.application.errors import ServiceError
    from dbt_metricflow_service.application.queries import QueryService
    from dbt_metricflow_service.models.queries import QueryRequest
    from dbt_metricflow_service.storage.jobs import CleanupBlocked
    from tests.test_v3_catalog_queries import ready_build

    app, build, jobs, artifacts = ready_build(store, tmp_path)
    queries = QueryService(app, CatalogService(app.store, artifacts), jobs)
    run = app.store.get(build.build_id)["run_id"]
    barrier = Barrier(2)
    original = queries._reserve

    def reserve(*args):
        barrier.wait(timeout=5)
        return original(*args)

    def query():
        try:
            return queries.submit(
                build.build_id, QueryRequest(idempotencyKey=uuid4().hex, mode="EXPLAIN",
                                            metricResourceIds=["metric.revenue"]), CALLER,
            )
        except ServiceError as error:
            assert error.status == 410
            return None

    def cleanup():
        barrier.wait(timeout=5)
        try:
            return jobs.reserve_cleanup(run)
        except CleanupBlocked:
            return None

    monkeypatch.setattr(queries, "_reserve", reserve)
    with ThreadPoolExecutor(max_workers=2) as pool:
        query_future, cleanup_future = pool.submit(query), pool.submit(cleanup)
        accepted_query, accepted_cleanup = query_future.result(), cleanup_future.result()
    assert (accepted_query is None) != (accepted_cleanup is None)
    assert jobs.get(run)["run_lifecycle"] == ("CLEANING" if accepted_cleanup else "ACTIVE")


def test_failed_new_intent_fences_inflight_old_switch(store, tmp_path, monkeypatch):
    from dbt_metricflow_service.application.catalog import CatalogService
    from dbt_metricflow_service.application.deployments import DeploymentService
    from dbt_metricflow_service.models.deployments import DeploymentRequest
    from tests.test_v3_catalog_queries import ready_build

    app, build, jobs, artifacts = ready_build(store, tmp_path)
    deployments = DeploymentStore(store.db)
    deploy = DeploymentService(deployments, lambda *_: build.commit_sha,
                               builds=app, catalogs=CatalogService(app.store, artifacts))
    key = DeploymentKey(repository=build.repository, environment=build.environment, branchName=build.branch_name)
    old = deploy.submit(DeploymentRequest(buildId=build.build_id, branchName=build.branch_name,
                                         expectedTargetVersion=0, idempotencyKey=uuid4().hex), CALLER)
    switching, continue_switch = Event(), Event()
    settle = deployments.settle

    def delayed_switch(*args, **kwargs):
        switching.set()
        assert continue_switch.wait(5)
        return settle(*args, **kwargs)

    monkeypatch.setattr(deployments, "settle", delayed_switch)
    with ThreadPoolExecutor(max_workers=1) as pool:
        old_switch = pool.submit(deploy.reconcile, key, old.generation)
        try:
            assert switching.wait(5)
            newer = app.submit(request(branchName=build.branch_name, deploymentPolicy="ON_SUCCESS"), CALLER)
            with store.db.fact_transaction() as connection:
                connection.exec_driver_sql("UPDATE runtime_job SET status='FAILED' WHERE job_id=%s",
                                           (app.store.get(newer.build_id)["run_id"],))
        finally:
            continue_switch.set()
        assert old_switch.result().deployment_status == "STALE"
    assert deploy.reconcile(key, newer.initial_deployment["generation"]).deployment_status == "FAILED"
    assert deploy.current(key).active_build_id is None
