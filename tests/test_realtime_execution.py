"""真实进程内执行边界：查询不创建数据库记录，锁和临时目录及时释放。"""

import json
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace
from uuid import uuid4

import pytest

from dbt_metricflow_service.application.errors import ServiceError
from dbt_metricflow_service.platform.models import DataQueryResult, DimensionQueryResult, PlatformQueryRequest
from dbt_metricflow_service.runtime.service import Runtime
from tests.data_helpers import leased_job, stored_build
from tests.test_execution_profiles import secrets as secrets
from tests.test_execution_profiles import settings as infrastructure
from tests.test_execution_snapshot_storage import snapshot
from tests.test_publication_storage import store as store


@pytest.fixture
def realtime(tmp_path, secrets):
    runtime = Runtime.__new__(Runtime)
    runtime.settings = replace(infrastructure(), profiles_dir=tmp_path, command_timeout_seconds=5,
                               max_output_bytes=64, temp_root=tmp_path / "queries", max_result_bytes=1024)
    build = stored_build(config_snapshot=snapshot())
    events = []

    @contextmanager
    def query_run(run_id):
        assert run_id == build.run_id
        events.append("lock")
        try:
            yield leased_job(project_id="test", schema_name="run_test", profile_binding_id="test")
        finally:
            events.append("unlock")

    def materialize(set_id, directory):
        assert set_id == build.output_set_id
        assert events[-1] == "lock"
        (directory / "dbt_project.yml").write_text("name: test\n", encoding="utf-8")

    # 故意不提供 reserve/claim/finish/result 等持久化方法。
    runtime.jobs = SimpleNamespace(query_run=query_run)
    runtime.artifacts = SimpleNamespace(materialize=materialize)
    return runtime, build, events


def test_repeated_calls_execute_in_memory_and_remove_workspaces(realtime, monkeypatch):
    import dbt_metricflow_service.platform.metricflow as engine

    runtime, build, events = realtime
    calls, paths = [], []
    payload = PlatformQueryRequest(runId=uuid4(), mode="QUERY", metrics=["orders"])

    def execute(project, profiles, request):
        assert request is payload
        assert events[-1] == "lock"
        assert (project / "dbt_project.yml").is_file()
        paths.append(project)
        calls.append(request)
        return DataQueryResult(rows=[[len(calls)]])

    monkeypatch.setattr(engine, "execute_programmatic", execute)
    assert runtime.execute_query(build, payload) == DataQueryResult(rows=[[1]])
    assert runtime.execute_query(build, payload) == DataQueryResult(rows=[[2]])
    assert paths[0] != paths[1]
    assert all(not path.exists() for path in paths)
    assert events == ["lock", "unlock", "lock", "unlock"]


@pytest.mark.parametrize(
    "failure,code,status",
    [
        ("invalid", "INVALID_QUERY", 422),
        ("connection", "COMMAND_FAILED", 503),
        ("timeout", "COMMAND_TIMEOUT", 504),
    ],
)
def test_errors_release_lock_and_do_not_publish_late_results(realtime, monkeypatch, failure, code, status):
    import time

    import dbt_metricflow_service.platform.metricflow as engine

    runtime, build, events = realtime
    paths = []
    if failure == "timeout":
        runtime.settings = replace(runtime.settings, command_timeout_seconds=0.01)

    def execute(project, profiles, request):
        paths.append(project)
        if failure == "invalid":
            raise engine.InvalidDimensionsRequest("private-details")
        if failure == "connection":
            raise ConnectionError("private-details")
        time.sleep(0.04)
        assert events == ["lock"]
        assert project.exists()
        return DataQueryResult(rows=[[123]])

    monkeypatch.setattr(engine, "execute_programmatic", execute)
    with pytest.raises(ServiceError) as caught:
        runtime.execute_query(build, PlatformQueryRequest(runId=uuid4(), mode="QUERY", metrics=["orders"]))
    assert caught.value.status == status
    assert caught.value.error.code == code
    assert "private-details" not in str(caught.value)
    assert events == ["lock", "unlock"]
    assert all(not path.exists() for path in paths)


@pytest.mark.parametrize(
    "value,limit,error_code",
    [
        (DataQueryResult(rows=[["中"]]), 0, None),
        (DataQueryResult(rows=[["中"]]), -1, "RESULT_TOO_LARGE"),
        (DataQueryResult(), 0, None),
        (DataQueryResult(), -1, "RESULT_TOO_LARGE"),
        ([], 1024, "RESULT_INVALID"),
        (None, 1024, "RESULT_INVALID"),
    ],
)
def test_realtime_result_size_and_shape(realtime, monkeypatch, value, limit, error_code):
    import dbt_metricflow_service.platform.metricflow as engine

    if isinstance(value, DataQueryResult):
        serialized = json.dumps(
            value.model_dump(mode="python", by_alias=True, exclude_unset=True), ensure_ascii=False,
        )
        limit += len(serialized.encode("utf-8"))

    runtime, build, events = realtime
    runtime.settings = replace(runtime.settings, max_result_bytes=limit)
    monkeypatch.setattr(engine, "execute_programmatic", lambda *args, **kwargs: value)
    if error_code is None:
        assert runtime.execute_query(build, PlatformQueryRequest(runId=uuid4(), mode="QUERY")) is value
    else:
        with pytest.raises(ServiceError) as caught:
            runtime.execute_query(build, PlatformQueryRequest(runId=uuid4(), mode="QUERY"))
        assert caught.value.error.code == error_code
    assert events == ["lock", "unlock"]


@pytest.mark.parametrize("binary", [False, True])
def test_realtime_non_json_result_is_a_sanitized_service_failure(realtime, monkeypatch, binary):
    import dbt_metricflow_service.platform.metricflow as engine

    runtime, build, events = realtime

    class PrivateCell:
        def __repr__(self):
            return "private-cell-details"

    cell = b"private-cell-details" if binary else PrivateCell()
    monkeypatch.setattr(engine, "execute_programmatic", lambda *args, **kwargs: DataQueryResult(rows=[[cell]]))
    with pytest.raises(ServiceError) as caught:
        runtime.execute_query(build, PlatformQueryRequest(runId=uuid4(), mode="QUERY"))
    assert caught.value.status == 503
    assert caught.value.error.code == "COMMAND_FAILED"
    assert "private-cell-details" not in str(caught.value)
    assert events == ["lock", "unlock"]


def test_waiting_queries_do_not_hold_cleanup_connections(realtime, monkeypatch):
    import time

    import dbt_metricflow_service.platform.metricflow as engine

    runtime, build, events = realtime

    def execute(*args, **kwargs):
        time.sleep(0.01)
        return DataQueryResult(rows=[[1]])

    monkeypatch.setattr(engine, "execute_programmatic", execute)
    with ThreadPoolExecutor(max_workers=9) as pool:
        results = list(pool.map(
            lambda _: runtime.execute_query(build, PlatformQueryRequest(runId=uuid4(), mode="QUERY")), range(9),
        ))
    assert results == [DataQueryResult(rows=[[1]])] * 9
    assert events == ["lock", "unlock"] * 9


def test_real_database_query_path_only_reads_without_persisting_records(store, tmp_path, monkeypatch, secrets):
    import dbt_metricflow_service.platform.metricflow as engine
    from tests.test_application_reads import reads
    from tests.test_catalog_queries import ready_build

    builds, view, jobs, artifacts = ready_build(store, tmp_path)
    build = builds.store.get(view.build_id)
    runtime = Runtime.__new__(Runtime)
    runtime.settings = replace(infrastructure(), profiles_dir=tmp_path, command_timeout_seconds=5,
                               max_output_bytes=64, temp_root=tmp_path / "queries")
    runtime.jobs, runtime.artifacts = jobs, artifacts
    monkeypatch.setattr(engine, "execute_programmatic", lambda *args, **kwargs: DataQueryResult(rows=[[1]]))
    monkeypatch.setattr(engine, "execute_available_dimensions", lambda *args, **kwargs: DimensionQueryResult())
    with reads(store.db) as statements:
        options = ("revenue",)
        query = PlatformQueryRequest(runId=uuid4(), mode="QUERY")
        assert runtime.available_dimensions(build, options) == DimensionQueryResult()
        assert runtime.execute_query(build, query) == DataQueryResult(rows=[[1]])
    assert statements
    assert all(statement.lstrip().upper().startswith("SELECT") for statement in statements)
    with store.db.transaction() as connection:
        assert (
            connection.exec_driver_sql(
                "SELECT count(*) FROM runtime_job WHERE parent_run_id=%s",
                (build.run_id,),
            ).scalar_one()
            == 0
        )


def test_dimension_runtime_repeats_calls_with_explicit_metrics(realtime, monkeypatch):
    import dbt_metricflow_service.platform.metricflow as engine

    runtime, build, events = realtime
    metrics = ("orders",)
    paths = []

    def discover(project, profiles, *, metrics):
        assert metrics == ("orders",)
        assert events[-1] == "lock"
        assert (project / "dbt_project.yml").is_file()
        paths.append(project)
        return DimensionQueryResult(allowed_filters=["="] if len(paths) == 1 else ["IN"])

    monkeypatch.setattr(engine, "execute_available_dimensions", discover)
    monkeypatch.setattr(engine, "execute_programmatic", lambda *args, **kwargs: pytest.fail("data API was called"))
    assert runtime.available_dimensions(build, metrics).allowed_filters == ["="]
    assert runtime.available_dimensions(build, metrics).allowed_filters == ["IN"]
    assert paths[0] != paths[1]
    assert all(not path.exists() for path in paths)
    assert events == ["lock", "unlock", "lock", "unlock"]


@pytest.mark.parametrize("dimensions", [True, False])
def test_runtime_rejects_the_other_operations_result_type(realtime, monkeypatch, dimensions):
    import dbt_metricflow_service.platform.metricflow as engine

    runtime, build, events = realtime
    if dimensions:
        monkeypatch.setattr(engine, "execute_available_dimensions", lambda *args, **kwargs: DataQueryResult())
        def execute():
            return runtime.available_dimensions(build, ("orders",))
    else:
        monkeypatch.setattr(engine, "execute_programmatic", lambda *args, **kwargs: DimensionQueryResult())
        def execute():
            return runtime.execute_query(build, PlatformQueryRequest(runId=uuid4(), mode="QUERY"))
    with pytest.raises(ServiceError) as caught:
        execute()
    assert caught.value.error.code == "RESULT_INVALID"
    assert caught.value.status == 503
    assert events == ["lock", "unlock"]


@pytest.mark.parametrize("failure,code,status", [
    ("invalid", "INVALID_QUERY", 422), ("connection", "COMMAND_FAILED", 503),
    ("timeout", "COMMAND_TIMEOUT", 504),
])
def test_dimension_errors_keep_http_semantics_and_release_workspace(realtime, monkeypatch, failure, code, status):
    import time

    import dbt_metricflow_service.platform.metricflow as engine

    runtime, build, events = realtime
    paths = []
    if failure == "timeout":
        runtime.settings = replace(runtime.settings, command_timeout_seconds=0.01)

    def discover(project, profiles, *, metrics):
        paths.append(project)
        if failure == "invalid":
            raise engine.InvalidDimensionsRequest("private-details")
        if failure == "connection":
            raise ConnectionError("private-details")
        time.sleep(0.04)
        assert project.exists()
        assert events == ["lock"]
        return DimensionQueryResult()

    monkeypatch.setattr(engine, "execute_available_dimensions", discover)
    with pytest.raises(ServiceError) as caught:
        runtime.available_dimensions(build, ("orders",))
    assert caught.value.error.code == code
    assert caught.value.status == status
    assert "private-details" not in str(caught.value)
    assert all(not path.exists() for path in paths)
    assert events == ["lock", "unlock"]


@pytest.mark.parametrize("dimensions", [True, False])
@pytest.mark.parametrize("late_failure", [False, True])
def test_cancelled_queries_wait_for_worker_and_restore_environment(realtime, monkeypatch, dimensions, late_failure):
    import asyncio
    import os
    import threading

    import dbt_metricflow_service.platform.metricflow as engine
    import dbt_metricflow_service.runtime.service as service
    from dbt_metricflow_service.execution.embedded import ENGINE_LOCK

    runtime, build, events = realtime
    started, release = threading.Event(), threading.Event()
    paths, profile_paths = [], []
    original_environment = dict(os.environ)
    original_run = asyncio.run

    def execute(project, profiles, **kwargs):
        paths.append(project)
        profile_paths.append(profiles)
        os.environ["DBT_ENV_SECRET_QUERY_CANCEL"] = "private-query-value"
        started.set()
        assert release.wait(5)
        assert project.exists()
        assert (profiles / "profiles.yml").exists()
        assert events == ["lock"]
        if late_failure:
            raise ConnectionError("private-query-value")
        return DimensionQueryResult() if dimensions else DataQueryResult(rows=[[1]])

    async def cancel_while_running(coroutine):
        task = asyncio.create_task(coroutine)
        try:
            assert await asyncio.to_thread(started.wait, 5)
            task.cancel()
            await asyncio.sleep(0.02)
            task.cancel()
            await asyncio.sleep(0.02)
            assert not task.done()
            assert ENGINE_LOCK.locked()
            assert paths[0].exists()
            assert profile_paths[0].exists()
            assert events == ["lock"]
        finally:
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await task
        raise asyncio.CancelledError

    monkeypatch.setattr(service.asyncio, "run", lambda coroutine: original_run(cancel_while_running(coroutine)))
    monkeypatch.setattr(engine, "execute_available_dimensions" if dimensions else "execute_programmatic", execute)
    with pytest.raises(asyncio.CancelledError):
        if dimensions:
            runtime.available_dimensions(build, ("orders",))
        else:
            runtime.execute_query(build, PlatformQueryRequest(runId=uuid4(), mode="QUERY"))
    assert not ENGINE_LOCK.locked()
    assert events == ["lock", "unlock"]
    assert all(not path.exists() for path in paths + profile_paths)
    assert dict(os.environ) == original_environment


@pytest.mark.parametrize("dimensions", [True, False])
@pytest.mark.parametrize("invalid_dimensions", [False, True])
def test_timeout_takes_precedence_over_late_engine_errors(realtime, monkeypatch, dimensions, invalid_dimensions):
    import time

    import dbt_metricflow_service.platform.metricflow as engine

    runtime, build, events = realtime
    runtime.settings = replace(runtime.settings, command_timeout_seconds=0.01)
    paths = []

    def execute(project, profiles, **kwargs):
        paths.append(project)
        time.sleep(0.04)
        assert project.exists()
        assert events == ["lock"]
        raise engine.InvalidDimensionsRequest("private-late-error") if invalid_dimensions else ConnectionError(
            "private-late-error"
        )

    monkeypatch.setattr(engine, "execute_available_dimensions" if dimensions else "execute_programmatic", execute)
    with pytest.raises(ServiceError) as caught:
        if dimensions:
            runtime.available_dimensions(build, ("orders",))
        else:
            runtime.execute_query(build, PlatformQueryRequest(runId=uuid4(), mode="QUERY"))
    assert caught.value.error.code == "COMMAND_TIMEOUT"
    assert caught.value.status == 504
    assert "private-late-error" not in str(caught.value)
    assert events == ["lock", "unlock"]
    assert all(not path.exists() for path in paths)
