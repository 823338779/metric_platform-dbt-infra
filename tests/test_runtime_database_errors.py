"""Framework database failures retain the runtime API and worker contract."""

import asyncio
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import event
from sqlalchemy.exc import DBAPIError, IntegrityError, TimeoutError

from dbt_metricflow_service.api.app import create_app
from dbt_metricflow_service.runtime.worker import Worker
from tests.test_runtime_api import runtime_pair as runtime_pair


@pytest.mark.parametrize(
    "error", [DBAPIError(None, None, Exception("private-password")), TimeoutError("private-password")]
)
async def test_database_errors_return_503(runtime_pair, monkeypatch, error):
    runtimes, _ = runtime_pair
    app = create_app(runtimes[0].settings)

    def unavailable(*args, **kwargs):
        raise error

    monkeypatch.setattr(app.state.runtime.jobs, "get", unavailable)
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
                                    base_url="http://test") as client:
            response = await client.get("/v3/queries/" + str(uuid4()) + "/status")
        assert response.status_code == 503
        assert response.json()["error"]["code"] == "STORAGE_UNAVAILABLE"
        assert "private-password" not in response.text
    finally:
        app.state.runtime.db.close()


@pytest.mark.parametrize("error", [DBAPIError(None, None, Exception("offline")), TimeoutError("busy")])
async def test_worker_survives_database_error(runtime_pair, monkeypatch, error):
    runtimes, _ = runtime_pair
    worker = Worker(runtimes[0])
    calls = 0

    def claim(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise error
        worker._stopping = True
        return None

    monkeypatch.setattr(runtimes[0].jobs, "claim", claim)
    await asyncio.wait_for(worker._slot(), timeout=3)
    assert calls == 2


@pytest.mark.parametrize("error", [DBAPIError(None, None, Exception("offline")), TimeoutError("busy")])
async def test_heartbeat_failure_cancels_unconfirmed_execution(runtime_pair, monkeypatch, error):
    runtimes, _ = runtime_pair
    worker = Worker(runtimes[0])
    monkeypatch.setattr(worker.runtime, "settings", SimpleNamespace(heartbeat_seconds=0.01, lease_seconds=0.02))

    def offline(*args):
        raise error

    monkeypatch.setattr(worker.runtime.jobs, "heartbeat", offline)
    execution = asyncio.create_task(asyncio.sleep(10))
    try:
        await asyncio.wait_for(worker._heartbeat({"job_id": str(uuid4()), "lease_token": str(uuid4())}, execution), 1)
        await asyncio.gather(execution, return_exceptions=True)
        assert execution.cancelled()
    finally:
        execution.cancel()
        await asyncio.gather(execution, return_exceptions=True)


def test_gc_only_swallows_foreign_key_violation(runtime_pair):
    runtimes, project = runtime_pair
    runtime = runtimes[0]
    source_id = runtime.jobs.project(project)["source_set_id"]
    unique_violation = False

    def inject_failure(connection, cursor, statement, parameters, context, executemany):
        # 模拟引用检查后的竞争，DELETE 仍由真实 PostgreSQL 外键拒绝。
        if statement.startswith("SELECT") and "EXISTS" in statement and "runtime_project" in statement:
            return "SELECT false", ()
        if unique_violation and statement.startswith("DELETE FROM runtime_artifact_set"):
            return "INSERT INTO engine_change_counter(singleton,value) VALUES (true,0)", ()
        return statement, parameters

    event.listen(runtime.db.engine, "before_cursor_execute", inject_failure, retval=True)
    try:
        assert runtime.artifacts.delete_unreferenced(source_id) is False
        assert runtime.artifacts.metadata(source_id)["state"] == "SEALED"
        # 无关的唯一键冲突仍必须向外传播，且整个删除事务应回滚。
        unique_violation = True
        with pytest.raises(IntegrityError) as failure:
            runtime.artifacts.delete_unreferenced(source_id)
        assert failure.value.orig.pgcode == "23505"
        assert runtime.artifacts.metadata(source_id)["state"] == "SEALED"
    finally:
        event.remove(runtime.db.engine, "before_cursor_execute", inject_failure)
