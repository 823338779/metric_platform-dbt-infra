"""真实进程内执行边界：查询不创建数据库记录，锁和临时目录及时释放。"""

import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from types import SimpleNamespace
from uuid import uuid4

import pytest

from dbt_metricflow_service.application.errors import ServiceError
from dbt_metricflow_service.execution.embedded import ENGINE_LOCK
from dbt_metricflow_service.execution.query import QueryExecutor
from dbt_metricflow_service.platform.models import DataQueryResult, DimensionQueryResult, PlatformQueryRequest
from tests.data_helpers import stored_build
from tests.test_execution_profiles import secrets as secrets
from tests.test_execution_profiles import settings as infrastructure
from tests.test_execution_snapshot_storage import snapshot
from tests.test_publication_storage import store as store


@pytest.fixture
def realtime(tmp_path, secrets):
    runtime = QueryExecutor.__new__(QueryExecutor)
    runtime.settings = replace(infrastructure(), profiles_dir=tmp_path, max_output_bytes=64,
                               temp_root=tmp_path / "queries", max_result_bytes=1024)
    build = stored_build(config_snapshot=snapshot())
    events = []

    def materialize(set_id, directory):
        assert set_id == build.output_set_id
        assert ENGINE_LOCK.locked()
        (directory / "dbt_project.yml").write_text("name: test\n", encoding="utf-8")

    # 故意不提供 reserve/claim/finish/result 等持久化方法。
    runtime.jobs = SimpleNamespace()
    runtime.artifacts = SimpleNamespace(materialize=materialize, read_file=lambda *_: b"saved manifest")
    return runtime, build, events


def test_repeated_calls_execute_in_memory_and_remove_workspaces(realtime, monkeypatch):
    import dbt_metricflow_service.delegates.clients.metric_flow_client as engine

    runtime, build, events = realtime
    calls, paths = [], []
    payload = PlatformQueryRequest(runId=uuid4(), mode="QUERY", metrics=["orders"])

    def execute(self, request):
        project = self._project
        assert self._manifest == b"saved manifest"
        assert request is payload
        assert ENGINE_LOCK.locked()
        assert (project / "dbt_project.yml").is_file()
        paths.append(project)
        calls.append(request)
        return DataQueryResult(rows=[[len(calls)]])

    monkeypatch.setattr(engine.MetricFlowClient, "query", execute)
    assert runtime.execute_query(build, payload) == DataQueryResult(rows=[[1]])
    assert runtime.execute_query(build, payload) == DataQueryResult(rows=[[2]])
    assert paths[0] != paths[1]
    assert all(not path.exists() for path in paths)
    assert not events


def test_preview_context_dispatches_directly_to_dbt_client(realtime, monkeypatch):
    from dbt_metricflow_service.delegates.clients.dbt_client import DbtClient
    from dbt_metricflow_service.delegates.clients.metric_flow_client import MetricFlowClient

    runtime, build, events = realtime
    paths = []

    def preview(self, selector, *, project, profiles, limit):
        assert self._content == b"saved manifest"
        assert selector == "orders"
        assert limit == 10
        assert (profiles / "profiles.yml").is_file()
        assert not events
        paths.append(project)
        return DataQueryResult(rows=[[1]])

    monkeypatch.setattr(DbtClient, "preview", preview)
    monkeypatch.setattr(MetricFlowClient, "query", lambda *_: pytest.fail("preview called MetricFlow"))
    result = runtime.execute_query(build, PlatformQueryRequest(
        runId=uuid4(), mode="PREVIEW", selector="orders", limit=10,
    ))
    assert result.rows == [[1]]
    assert not events
    assert not paths[0].exists()


@pytest.mark.parametrize("failure", [None, "slow", "native"])
def test_lineage_uses_temporary_context_without_data_cleanup_lock(realtime, monkeypatch, failure):
    import os
    import time

    from dbt_metricflow_service.delegates.clients.dbt_client import DbtClient
    from dbt_metricflow_service.execution.embedded import ENGINE_LOCK

    runtime, build, events = realtime
    original_environment = dict(os.environ)
    paths = []

    def materialize(set_id, project):
        assert set_id == build.output_set_id
        (project / "dbt_project.yml").write_text("name: test\nprofile: test\n")

    def lineage(self, selector, *, project, profiles):
        assert ENGINE_LOCK.locked()
        assert not events
        assert self._content == b"saved manifest"
        assert selector == "1+orders+1"
        paths.extend([project, profiles])
        os.environ["DBT_LINEAGE_TEST"] = "temporary"
        if failure == "slow":
            time.sleep(0.04)
            assert project.exists()
        if failure == "native":
            raise ValueError("native error")
        return [{"unique_id": "model.sample.orders", "depends_on": {"nodes": []}}]

    runtime.jobs = SimpleNamespace()  # 元数据查询不访问任务队列。
    runtime.artifacts.materialize = materialize
    monkeypatch.setattr(DbtClient, "lineage", lineage)
    if failure == "native":
        with pytest.raises(ServiceError) as caught:
            runtime.lineage(build, "1+orders+1")
        assert caught.value.status == 503
    else:
        assert runtime.lineage(build, "1+orders+1")[0]["unique_id"] == "model.sample.orders"
    assert not ENGINE_LOCK.locked()
    assert all(not path.exists() for path in paths)
    assert dict(os.environ) == original_environment


@pytest.mark.parametrize(
    "failure,code,status",
    [
        ("invalid", "INVALID_QUERY", 422),
        ("connection", "COMMAND_FAILED", 503),
        ("slow", None, None),
    ],
)
def test_errors_release_lock_and_do_not_publish_late_results(realtime, monkeypatch, failure, code, status):
    import time

    import dbt_metricflow_service.delegates.clients.metric_flow_client as engine

    runtime, build, events = realtime
    paths = []

    def execute(self, request):
        project = self._project
        paths.append(project)
        if failure == "invalid":
            raise engine.InvalidDimensionsRequest("private-details")
        if failure == "connection":
            raise ConnectionError("private-details")
        time.sleep(0.04)
        assert not events
        assert project.exists()
        return DataQueryResult(rows=[[123]])

    monkeypatch.setattr(engine.MetricFlowClient, "query", execute)
    request = PlatformQueryRequest(runId=uuid4(), mode="QUERY", metrics=["orders"])
    if failure == "slow":
        assert runtime.execute_query(build, request).rows == [[123]]
    else:
        with pytest.raises(ServiceError) as caught:
            runtime.execute_query(build, request)
        assert caught.value.status == status
        assert caught.value.error.code == code
        assert "private-details" not in str(caught.value)
    assert not events
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
    import dbt_metricflow_service.delegates.clients.metric_flow_client as engine

    if isinstance(value, DataQueryResult):
        serialized = json.dumps(
            value.model_dump(mode="python", by_alias=True, exclude_unset=True), ensure_ascii=False,
        )
        limit += len(serialized.encode("utf-8"))

    runtime, build, events = realtime
    runtime.settings = replace(runtime.settings, max_result_bytes=limit)
    monkeypatch.setattr(engine.MetricFlowClient, "query", lambda *args, **kwargs: value)
    if error_code is None:
        assert runtime.execute_query(build, PlatformQueryRequest(runId=uuid4(), mode="QUERY")) is value
    else:
        with pytest.raises(ServiceError) as caught:
            runtime.execute_query(build, PlatformQueryRequest(runId=uuid4(), mode="QUERY"))
        assert caught.value.error.code == error_code
    assert not events


@pytest.mark.parametrize("binary", [False, True])
def test_realtime_non_json_result_is_a_sanitized_service_failure(realtime, monkeypatch, binary):
    import dbt_metricflow_service.delegates.clients.metric_flow_client as engine

    runtime, build, events = realtime

    class PrivateCell:
        def __repr__(self):
            return "private-cell-details"

    cell = b"private-cell-details" if binary else PrivateCell()
    monkeypatch.setattr(engine.MetricFlowClient, "query", lambda *args, **kwargs: DataQueryResult(rows=[[cell]]))
    with pytest.raises(ServiceError) as caught:
        runtime.execute_query(build, PlatformQueryRequest(runId=uuid4(), mode="QUERY"))
    assert caught.value.status == 503
    assert caught.value.error.code == "COMMAND_FAILED"
    assert "private-cell-details" not in str(caught.value)
    assert not events


def test_waiting_queries_do_not_hold_cleanup_connections(realtime, monkeypatch):
    import time

    import dbt_metricflow_service.delegates.clients.metric_flow_client as engine

    runtime, build, events = realtime

    def execute(*args, **kwargs):
        time.sleep(0.01)
        return DataQueryResult(rows=[[1]])

    monkeypatch.setattr(engine.MetricFlowClient, "query", execute)
    with ThreadPoolExecutor(max_workers=9) as pool:
        results = list(pool.map(
            lambda _: runtime.execute_query(build, PlatformQueryRequest(runId=uuid4(), mode="QUERY")), range(9),
        ))
    assert results == [DataQueryResult(rows=[[1]])] * 9
    assert not events


def test_real_database_query_path_only_reads_without_persisting_records(store, tmp_path, monkeypatch, secrets):
    import dbt_metricflow_service.delegates.clients.metric_flow_client as engine
    from tests.test_application_reads import reads
    from tests.test_catalog_queries import ready_build

    builds, view, jobs, artifacts = ready_build(store, tmp_path)
    build = builds.store.get(view.build_id)
    runtime = QueryExecutor.__new__(QueryExecutor)
    runtime.settings = replace(infrastructure(), profiles_dir=tmp_path,
                               max_output_bytes=64, temp_root=tmp_path / "queries")
    runtime.jobs, runtime.artifacts = jobs, artifacts
    monkeypatch.setattr(engine.MetricFlowClient, "query", lambda *args, **kwargs: DataQueryResult(rows=[[1]]))
    monkeypatch.setattr(engine.MetricFlowClient, "available_dimensions", lambda *args, **kwargs: DimensionQueryResult())
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
    import dbt_metricflow_service.delegates.clients.metric_flow_client as engine

    runtime, build, events = realtime
    metrics = ("orders",)
    paths = []

    def discover(self, *, metrics):
        project = self._project
        assert metrics == ("orders",)
        assert ENGINE_LOCK.locked()
        assert (project / "dbt_project.yml").is_file()
        paths.append(project)
        return DimensionQueryResult(allowed_filters=["="] if len(paths) == 1 else ["IN"])

    monkeypatch.setattr(engine.MetricFlowClient, "available_dimensions", discover)
    monkeypatch.setattr(engine.MetricFlowClient, "query", lambda *args, **kwargs: pytest.fail("data API was called"))
    assert runtime.available_dimensions(build, metrics).allowed_filters == ["="]
    assert runtime.available_dimensions(build, metrics).allowed_filters == ["IN"]
    assert paths[0] != paths[1]
    assert all(not path.exists() for path in paths)
    assert not events


@pytest.mark.parametrize("dimensions", [True, False])
def test_runtime_rejects_the_other_operations_result_type(realtime, monkeypatch, dimensions):
    import dbt_metricflow_service.delegates.clients.metric_flow_client as engine

    runtime, build, events = realtime
    if dimensions:
        monkeypatch.setattr(engine.MetricFlowClient, "available_dimensions", lambda *args, **kwargs: DataQueryResult())
        def execute():
            return runtime.available_dimensions(build, ("orders",))
    else:
        monkeypatch.setattr(engine.MetricFlowClient, "query", lambda *args, **kwargs: DimensionQueryResult())
        def execute():
            return runtime.execute_query(build, PlatformQueryRequest(runId=uuid4(), mode="QUERY"))
    with pytest.raises(ServiceError) as caught:
        execute()
    assert caught.value.error.code == "RESULT_INVALID"
    assert caught.value.status == 503
    assert not events


@pytest.mark.parametrize("failure,code,status", [
    ("invalid", "INVALID_QUERY", 422), ("connection", "COMMAND_FAILED", 503),
    ("slow", None, None),
])
def test_dimension_errors_keep_http_semantics_and_release_workspace(realtime, monkeypatch, failure, code, status):
    import time

    import dbt_metricflow_service.delegates.clients.metric_flow_client as engine

    runtime, build, events = realtime
    paths = []

    def discover(self, *, metrics):
        project = self._project
        paths.append(project)
        if failure == "invalid":
            raise engine.InvalidDimensionsRequest("private-details")
        if failure == "connection":
            raise ConnectionError("private-details")
        time.sleep(0.04)
        assert project.exists()
        assert not events
        return DimensionQueryResult()

    monkeypatch.setattr(engine.MetricFlowClient, "available_dimensions", discover)
    if failure == "slow":
        assert runtime.available_dimensions(build, ("orders",)) == DimensionQueryResult()
    else:
        with pytest.raises(ServiceError) as caught:
            runtime.available_dimensions(build, ("orders",))
        assert caught.value.error.code == code
        assert caught.value.status == status
        assert "private-details" not in str(caught.value)
    assert all(not path.exists() for path in paths)
    assert not events


@pytest.mark.parametrize("dimensions", [True, False])
@pytest.mark.parametrize("late_failure", [False, True])
def test_cancelled_queries_wait_for_worker_and_restore_environment(realtime, monkeypatch, dimensions, late_failure):
    import asyncio
    import os
    import threading

    import dbt_metricflow_service.delegates.clients.metric_flow_client as engine
    import dbt_metricflow_service.execution.query as service
    from dbt_metricflow_service.execution.embedded import ENGINE_LOCK

    runtime, build, events = realtime
    started, release = threading.Event(), threading.Event()
    paths, profile_paths = [], []
    original_environment = dict(os.environ)
    original_run = asyncio.run

    def execute(self, **kwargs):
        project = self._project
        paths.append(project)
        profiles = self._profiles
        profile_paths.append(profiles)
        os.environ["DBT_ENV_SECRET_QUERY_CANCEL"] = "private-query-value"
        started.set()
        assert release.wait(5)
        assert project.exists()
        assert (profiles / "profiles.yml").exists()
        assert not events
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
            assert not events
        finally:
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await task
        raise asyncio.CancelledError

    monkeypatch.setattr(service.asyncio, "run", lambda coroutine: original_run(cancel_while_running(coroutine)))
    monkeypatch.setattr(engine.MetricFlowClient, "available_dimensions" if dimensions else "query", execute)
    with pytest.raises(asyncio.CancelledError):
        if dimensions:
            runtime.available_dimensions(build, ("orders",))
        else:
            runtime.execute_query(build, PlatformQueryRequest(runId=uuid4(), mode="QUERY"))
    assert not ENGINE_LOCK.locked()
    assert not events
    assert all(not path.exists() for path in paths + profile_paths)
    assert dict(os.environ) == original_environment


@pytest.mark.parametrize("dimensions", [True, False])
@pytest.mark.parametrize("invalid_dimensions", [False, True])
def test_slow_engine_errors_keep_original_error(realtime, monkeypatch, dimensions, invalid_dimensions):
    import time

    import dbt_metricflow_service.delegates.clients.metric_flow_client as engine

    runtime, build, events = realtime
    paths = []

    def execute(self, **kwargs):
        project = self._project
        paths.append(project)
        time.sleep(0.04)
        assert project.exists()
        assert not events
        raise engine.InvalidDimensionsRequest("private-late-error") if invalid_dimensions else ConnectionError(
            "private-late-error"
        )

    monkeypatch.setattr(engine.MetricFlowClient, "available_dimensions" if dimensions else "query", execute)
    with pytest.raises(ServiceError) as caught:
        if dimensions:
            runtime.available_dimensions(build, ("orders",))
        else:
            runtime.execute_query(build, PlatformQueryRequest(runId=uuid4(), mode="QUERY"))
    assert caught.value.error.code == ("INVALID_QUERY" if invalid_dimensions else "COMMAND_FAILED")
    assert caught.value.status == (422 if invalid_dimensions else 503)
    assert "private-late-error" not in str(caught.value)
    assert not events
    assert all(not path.exists() for path in paths)
