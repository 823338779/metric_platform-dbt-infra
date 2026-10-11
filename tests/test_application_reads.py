"""应用读取复用构建快照，历史浏览只加载当前页。"""

import json
from contextlib import contextmanager
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import event

from dbt_metricflow_service.application.catalog import CatalogService
from dbt_metricflow_service.application.errors import ServiceError
from dbt_metricflow_service.application.queries import QueryService
from dbt_metricflow_service.models.queries import QueryRequest
from dbt_metricflow_service.storage.paging import page as legacy_page
from tests.synchronous_queries import inline_executor
from tests.test_publication_storage import store as store
from tests.test_v3_build_acceptance import CALLER, request, service
from tests.test_v3_catalog_queries import ready_build
from tests.test_v3_deployments import deployment_service


@contextmanager
def reads(db):
    statements = []

    def record(connection, cursor, statement, parameters, context, many):
        if statement.lstrip().upper().startswith("SELECT"):
            statements.append(statement)

    event.listen(db.engine, "after_cursor_execute", record)
    try:
        yield statements
    finally:
        event.remove(db.engine, "after_cursor_execute", record)


def test_build_page_batches_initial_deployments(store):
    app = service(store)
    branch = "page/" + uuid4().hex
    builds = [app.submit(request(branchName=branch, deploymentPolicy="ON_SUCCESS"), CALLER) for _ in range(4)]
    with reads(store.db) as statements:
        page = app.list(builds[0].repository, branch, limit=3)
    assert [item.build_id for item in page.items] == [item.build_id for item in builds[:0:-1]]
    assert all(item.initial_deployment is not None for item in page.items)
    assert len(statements) <= 2
    assert "LIMIT" in statements[0].upper()


@pytest.mark.parametrize("deployments", [False, True])
def test_history_cursor_filters_and_concurrent_insert(store, deployments):
    app = service(store)
    branch = "history/" + uuid4().hex
    builds = [app.submit(request(branchName=branch, deploymentPolicy="ON_SUCCESS"), CALLER) for _ in range(3)]
    history = deployment_service(store, "a" * 40) if deployments else app
    arguments = {"repository": builds[0].repository, "branch_name": branch, "limit": 2}
    first = history.list(**arguments)
    assert [item.build_id for item in first.items] == [builds[2].build_id, builds[1].build_id]
    app.submit(request(branchName=branch, deploymentPolicy="ON_SUCCESS"), CALLER)
    second = history.list(**arguments, cursor=first.next_cursor)
    assert [item.build_id for item in second.items] == [builds[0].build_id]
    assert second.next_cursor is None
    with pytest.raises(ServiceError) as failure:
        history.list(**{**arguments, "branch_name": branch + "other"}, cursor=first.next_cursor)
    assert failure.value.error.code == "INVALID_CURSOR"
    with reads(store.db) as statements:
        history.list(**arguments)
    assert "LIMIT" in statements[0].upper()


def test_query_submission_does_not_read_deployments(store, tmp_path):
    app, build, jobs, artifacts = ready_build(store, tmp_path)
    queries = QueryService(app, CatalogService(app.store, artifacts), jobs, inline_executor(jobs, app.toolchain))
    with reads(store.db) as statements:
        view = queries.submit(
            build.build_id,
            QueryRequest(idempotencyKey=uuid4().hex, mode="EXPLAIN", metricResourceIds=["metric.revenue"]),
            CALLER,
        )
    assert view.state == "SUCCEEDED"
    assert not any("engine_deployment_attempt" in statement for statement in statements)
    # 受理、同步完成事务和结果元数据各读一次构建；可用性及目录校验复用快照。
    assert sum("FROM engine_build" in statement for statement in statements) == 3


@pytest.mark.parametrize("deployments", [False, True])
def test_history_rejects_old_sorting_cursor(store, deployments):
    app = service(store)
    branch = "legacy-cursor/" + uuid4().hex
    build = app.submit(request(branchName=branch, deploymentPolicy="ON_SUCCESS"), CALLER)
    scope = [build.repository, None, branch] if deployments else [build.repository, branch]
    identity = json.dumps([build.repository, build.environment, branch, 1]) if deployments else str(build.build_id)
    position = str(build.created_at) + identity
    _, cursor = legacy_page([position, position], scope=scope, identity=str, limit=1)
    history = deployment_service(store, "a" * 40) if deployments else app
    with pytest.raises(ServiceError) as failure:
        history.list(build.repository, branch_name=branch, cursor=cursor)
    assert failure.value.error.code == "INVALID_CURSOR"


@pytest.mark.parametrize("deployments", [False, True])
def test_history_equal_timestamps_have_no_gaps(store, deployments):
    app = service(store)
    branch = "same-time/测试/" + uuid4().hex
    builds = [app.submit(request(branchName=branch, deploymentPolicy="ON_SUCCESS"), CALLER) for _ in range(11)]
    table = "engine_deployment_attempt" if deployments else "engine_build"
    with store.db.fact_transaction() as connection:
        connection.exec_driver_sql(
            f"UPDATE {table} SET created_at=%s WHERE repository=%s AND branch_name=%s",
            (datetime(2026, 1, 1, tzinfo=UTC), builds[0].repository, branch),
        )
    history = deployment_service(store, "a" * 40) if deployments else app
    cursor, seen = None, []
    while True:
        result = history.list(builds[0].repository, branch_name=branch, cursor=cursor, limit=2)
        seen.extend(item.generation if deployments else item.build_id for item in result.items)
        cursor = result.next_cursor
        if cursor is None:
            break
    assert seen == (list(range(11, 0, -1)) if deployments else sorted((item.build_id for item in builds), reverse=True))
