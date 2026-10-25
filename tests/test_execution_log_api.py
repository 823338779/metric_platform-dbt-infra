"""前端通过 HTTP 按构建增量读取已持久化的 dbt 详细日志。"""

import threading
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import event

from dbt_metricflow_service.api.builds import router
from dbt_metricflow_service.api.errors import register_exception_handlers
from dbt_metricflow_service.execution.logs import EngineLog
from dbt_metricflow_service.settings import Settings
from dbt_metricflow_service.storage.builds import BuildStore
from dbt_metricflow_service.storage.execution_logs import ExecutionLogStore
from dbt_metricflow_service.storage.jobs import JobStore
from tests.dbt_execution_helpers import invoke_job_for_test
from tests.schema_helpers import isolated_database
from tests.test_build_acceptance import CALLER, TOOLCHAIN, request


@pytest.fixture
def context(tmp_path):
    from dbt_metricflow_service.application.builds import BuildService

    with isolated_database() as db:
        db.initialize()
        service = BuildService(BuildStore(db), TOOLCHAIN)
        build = service.submit_snapshot(request(), CALLER)
        job = JobStore(db).claim(str(uuid4()), toolchain_version=TOOLCHAIN)

        async def guard():
            return CALLER

        app = FastAPI()
        register_exception_handlers(app)
        app.include_router(router(service, guard, Settings(
            temp_root=tmp_path, profiles_dir=tmp_path, max_output_bytes=1024,
        )))
        with TestClient(app) as client:
            yield client, service, db, build, job


def write(db, job, *messages):
    ExecutionLogStore(db).append(job, "BUILDING", "build", [
        EngineLog(datetime(2026, 10, 25, 1, 2, 3, tzinfo=UTC), "debug", message,
                  event_name="JinjaLogDebug", event_code="I063", invocation_id="invocation",
                  node_id="model.project.orders", thread_name="Thread-1")
        for message in messages
    ])


def test_api_returns_structured_rows_and_continues_polling_after_empty_page(context):
    client, _, db, build, job = context
    url = f"/builds/{build.build_id}/execution-logs"
    response = client.get(url)
    assert response.status_code == 200
    empty = response.json()
    assert empty["items"] == []
    assert empty["hasMore"] is False
    assert empty["nextCursor"]
    write(db, job, "first\nline", "<script>text only</script>", "third")
    response = client.get(url, params={"cursor": empty["nextCursor"], "limit": 2})
    assert response.status_code == 200
    first = response.json()
    assert first["hasMore"] is True
    assert [row["message"] for row in first["items"]] == ["first\nline", "<script>text only</script>"]
    row = first["items"][0]
    assert isinstance(row["logId"], str)
    assert row["jobId"] == job.job_id
    assert row["attemptId"] == job.attempt_id
    assert row["phase"] == "BUILDING"
    assert row["command"] == "build"
    assert row["level"] == "debug"
    assert row["eventName"] == "JinjaLogDebug"
    assert row["eventCode"] == "I063"
    assert row["nodeId"] == "model.project.orders"
    assert row["invocationId"] == "invocation"
    assert row["threadName"] == "Thread-1"
    assert row["eventTime"] == "2026-10-25T01:02:03Z"
    second = client.get(url, params={"cursor": first["nextCursor"], "limit": 2}).json()
    assert [row["message"] for row in second["items"]] == ["third"]
    assert second["hasMore"] is False
    tail = client.get(url, params={"cursor": second["nextCursor"]}).json()
    assert tail["items"] == []
    assert tail["nextCursor"] == second["nextCursor"]
    write(db, job, "late event")
    assert [row["message"] for row in client.get(url, params={"cursor": tail["nextCursor"]}).json()["items"]] == [
        "late event",
    ]


def test_logs_and_cursors_cannot_cross_builds_and_old_logs_are_removed(context):
    client, service, db, build, job = context
    write(db, job, "first build private log")
    another = service.submit_snapshot(request(), CALLER)
    other_job = JobStore(db).claim(str(uuid4()), toolchain_version=TOOLCHAIN)
    write(db, other_job, "second build")
    first = client.get(f"/builds/{build.build_id}/execution-logs").json()
    second_url = f"/builds/{another.build_id}/execution-logs"
    second = client.get(second_url)
    assert second.status_code == 200
    assert [row["message"] for row in second.json()["items"]] == ["second build"]
    invalid = client.get(second_url, params={"cursor": first["nextCursor"]})
    assert invalid.status_code == 422
    assert invalid.json()["error"]["code"] == "INVALID_CURSOR"
    stages = client.get(f"/builds/{build.build_id}/logs")
    assert stages.status_code == 404
    assert "/builds/{build_id}/logs" not in client.get("/openapi.json").json()["paths"]
    assert client.get(f"/builds/{build.build_id}/execution-logs", params={
        "cursor": f"{build.build_id}:1",
    }).status_code == 422


def test_execution_progress_is_read_from_build_detail_not_synthetic_logs(context):
    client, _, db, build, job = context
    jobs = JobStore(db)
    assert jobs.phase(job.job_id, job.lease_token, "PARSING")
    detail = client.get(f"/builds/{build.build_id}").json()
    assert detail["phase"] == "PARSING"
    assert detail["buildStatus"] == "RUNNING"
    assert detail["error"] is None
    assert jobs.fail(job.job_id, job.lease_token, "COMMAND_FAILED")
    detail = client.get(f"/builds/{build.build_id}").json()
    assert detail["buildStatus"] == "FAILED"
    assert detail["error"]["code"] == "COMMAND_FAILED"
    assert detail["error"]["phase"] == "PARSING"
    stored = jobs.get(job.job_id)
    assert (stored.phase, stored.status, stored.error_code) == ("PARSING", "FAILED", "COMMAND_FAILED")
    assert client.get(f"/builds/{build.build_id}/execution-logs").json()["items"] == []


@pytest.mark.parametrize("params", [{"cursor": "bad"}, {"limit": 0}, {"limit": 201}, {"limit": "invalid"}])
def test_api_rejects_invalid_pagination(context, params):
    client, _, _, build, _ = context
    assert client.get(f"/builds/{build.build_id}/execution-logs", params=params).status_code == 422


def test_api_reports_missing_build_and_preserves_bigint_precision(context):
    client, _, db, build, job = context
    missing = client.get(f"/builds/{uuid4()}/execution-logs")
    assert missing.status_code == 404
    assert missing.json()["error"]["code"] == "BUILD_NOT_FOUND"
    with db.transaction() as connection:
        connection.exec_driver_sql("SELECT setval('runtime_execution_log_log_id_seq',9007199254740993,false)")
    write(db, job, "big id")
    response = client.get(f"/builds/{build.build_id}/execution-logs")
    assert response.status_code == 200
    assert response.json()["items"][0]["logId"] == "9007199254740993"


def test_api_time_is_utc_even_when_database_session_uses_local_timezone(context):
    client, _, db, build, job = context
    write(db, job, "time zone")

    def local_timezone(connection):
        connection.exec_driver_sql("SET LOCAL TIME ZONE 'Asia/Shanghai'")

    event.listen(db.engine, "begin", local_timezone)
    try:
        response = client.get(f"/builds/{build.build_id}/execution-logs")
        assert response.status_code == 200
        assert response.json()["items"][0]["eventTime"] == "2026-10-25T01:02:03Z"
        assert response.json()["items"][0]["createdAt"].endswith("Z")
    finally:
        event.remove(db.engine, "begin", local_timezone)


@pytest.mark.parametrize("position", ["-1", "9223372036854775808", "1.5", ""])
def test_api_rejects_invalid_position_inside_scoped_cursor(context, position):
    from dbt_metricflow_service.storage.paging import write_cursor

    client, _, _, build, _ = context
    cursor = write_cursor(["build-execution-logs", str(build.build_id)], position)
    response = client.get(f"/builds/{build.build_id}/execution-logs", params={"cursor": cursor})
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "INVALID_CURSOR"


async def test_frontend_api_reads_real_dbt_parse_events(context, tmp_path):
    from types import SimpleNamespace

    from dbt_metricflow_service.execution.build import BuildExecutor
    from dbt_metricflow_service.execution.environment import build_engine_environment
    from tests.dbt_execution_helpers import EngineCall
    from tests.resource_helpers import make_resource_project

    client, _, db, build, job = context
    project, profiles, _ = make_resource_project(tmp_path)
    executor = BuildExecutor(SimpleNamespace(max_output_bytes=32), JobStore(db), None)
    spec = EngineCall(
        ("dbt", "parse", "--project-dir", str(project), "--profiles-dir", str(profiles)), project,
        build_engine_environment(project, profiles, "main", "test"), False,
    )
    await invoke_job_for_test(executor, job, spec, "PARSING")
    response = client.get(f"/builds/{build.build_id}/execution-logs", params={"limit": 200})
    assert response.status_code == 200
    rows = response.json()["items"]
    assert any(row["eventName"] == "MainReportVersion" and "Running with dbt" in row["message"] for row in rows)
    assert any(row["eventName"] == "MainReportArgs" and row["level"] == "debug" for row in rows)
    assert {row["phase"] for row in rows} == {"PARSING"}
    assert {row["attemptId"] for row in rows} == {job.attempt_id}


def test_polling_does_not_skip_a_batch_when_writers_overlap(context):
    client, _, db, build, job = context
    inserted, release = threading.Event(), threading.Event()

    def hold_first_insert(connection, cursor, statement, parameters, execution_context, many):
        if threading.current_thread().name.endswith("_0") and statement.startswith("INSERT INTO runtime_execution_log"):
            inserted.set()
            assert release.wait(5)

    event.listen(db.engine, "after_cursor_execute", hold_first_insert)
    url = f"/builds/{build.build_id}/execution-logs"
    try:
        with ThreadPoolExecutor(max_workers=2, thread_name_prefix="log-api-test") as pool:
            first = pool.submit(write, db, job, "first transaction")
            try:
                assert inserted.wait(3)
                second = pool.submit(write, db, job, "second transaction")
                try:
                    second.result(timeout=0.5)
                except TimeoutError:
                    pass
                during = client.get(url).json()
            finally:
                release.set()
            first.result(timeout=3)
            second.result(timeout=3)
        following = client.get(url, params={"cursor": during["nextCursor"]}).json()
        assert [row["message"] for row in during["items"] + following["items"]] == [
            "first transaction", "second transaction",
        ]
    finally:
        event.remove(db.engine, "after_cursor_execute", hold_first_insert)
