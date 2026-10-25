import asyncio
import hashlib
import json
import threading
from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from pydantic import TypeAdapter

from dbt_metricflow_service.application.errors import ServiceError
from dbt_metricflow_service.engines.models import DataQueryResult, DimensionQueryResult, NativeDimension
from dbt_metricflow_service.execution.embedded import ENGINE_LOCK, run_serialized
from dbt_metricflow_service.execution.query import QueryExecutor
from dbt_metricflow_service.models.queries import QueryRequest


def test_dimension_query_uses_one_context_and_one_client(monkeypatch, settings):
    build = SimpleNamespace(build_id="00000000-0000-0000-0000-000000000001",
                            config_snapshot=SimpleNamespace(schema_name="analytics"))
    option_id = hashlib.sha256(json.dumps([build.build_id, ["revenue"], "order__status"],
                                          separators=(",", ":")).encode()).hexdigest()
    request = TypeAdapter(QueryRequest).validate_python({
        "mode": "QUERY", "metrics": ["revenue"], "groupBy": [{"optionId": option_id}],
    })
    calls = []

    @contextmanager
    def context(*args, **kwargs):
        calls.append("enter")
        try:
            yield b"manifest", settings.temp_root, settings.temp_root
        finally:
            calls.append("exit")

    class Client:
        def __init__(self, *args, **kwargs):
            calls.append("client")

        def available_dimensions(self, metrics):
            calls.append("discover")
            return DimensionQueryResult(dimensions=[NativeDimension(token="order__status", name="status")])

        def query(self, request):
            calls.append("execute")
            assert request.group_by == ["order__status"]
            assert not hasattr(request, "run_id")
            return DataQueryResult(rows=[["paid"]])

    executor = QueryExecutor(settings, object())
    monkeypatch.setattr(executor, "_engine_context", context)
    monkeypatch.setattr("dbt_metricflow_service.execution.query.MetricFlowClient", Client)
    assert executor.execute_query(build, request).rows == [["paid"]]
    assert calls == ["enter", "client", "discover", "execute", "exit"]


@pytest.mark.asyncio
async def test_serialized_exception_propagates_without_result_wrapper():
    error = ServiceError("TEST_ERROR", "expected", 422)

    def invoke():
        raise error

    with pytest.raises(ServiceError) as caught:
        await run_serialized(invoke)
    assert caught.value is error
    assert not ENGINE_LOCK.locked()


@pytest.mark.asyncio
async def test_cancellation_waits_for_engine_thread_before_unlocking():
    started, release, exited = threading.Event(), threading.Event(), threading.Event()

    def invoke():
        started.set()
        try:
            assert release.wait(5)
        finally:
            exited.set()

    task = asyncio.create_task(run_serialized(invoke))
    try:
        assert await asyncio.to_thread(started.wait, 2)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert ENGINE_LOCK.locked()
        assert not exited.is_set()
        assert not task.done()
    finally:
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert exited.is_set()
    assert not ENGINE_LOCK.locked()


def test_workspace_damage_is_a_service_error_without_private_details(monkeypatch, settings):
    @contextmanager
    def broken_workspace(*args):
        raise ValueError("private database path")
        yield

    monkeypatch.setattr("dbt_metricflow_service.execution.query.materialized_workspace", broken_workspace)
    build = SimpleNamespace(build_id="00000000-0000-0000-0000-000000000001", output_set_id=None)
    executor = QueryExecutor(settings, object())
    with pytest.raises(ServiceError) as caught:
        executor.execute_query(build, TypeAdapter(QueryRequest).validate_python({"mode": "PREVIEW", "selector": "x"}))
    assert caught.value.status == 503
    assert "private" not in caught.value.error.model_dump_json()
    assert not ENGINE_LOCK.locked()
