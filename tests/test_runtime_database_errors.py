"""Framework database failures retain the runtime API and worker contract."""

import asyncio
from uuid import uuid4

import httpx
import pytest
from sqlalchemy.exc import DBAPIError, IntegrityError, TimeoutError

from dbt_metricflow_service.api.runtime import create_runtime_app
from dbt_metricflow_service.runtime.worker import Worker
from tests.test_runtime_api import runtime_pair as runtime_pair


@pytest.mark.parametrize(
    "error", [DBAPIError(None, None, Exception("private-password")), TimeoutError("private-password")]
)
async def test_database_errors_return_503(runtime_pair, monkeypatch, error):
    runtimes, _ = runtime_pair
    app = create_runtime_app(runtimes[0].settings)

    def unavailable(*args, **kwargs):
        raise error

    monkeypatch.setattr(app.state.runtime.jobs, "get", unavailable)
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
                                    base_url="http://test") as client:
            response = await client.get("/v1/jobs/" + str(uuid4()))
        assert response.status_code == 503
        assert response.json() == {"detail": {"code": "runtime_unavailable"}}
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


def test_gc_only_swallows_foreign_key_violation(runtime_pair, monkeypatch):
    from dbt_metricflow_service.storage import artifacts

    runtimes, project = runtime_pair
    runtime = runtimes[0]
    source_id = runtime.jobs.project(project)["source_set_id"]
    # Simulate a reference racing the eligibility check; retain real DELETE/FK behavior.
    monkeypatch.setattr(artifacts, "SQL_GC_ELIGIBLE",
                        "SELECT true AS eligible FROM (SELECT " + ",".join(["%s"] * 8) + ") AS inputs")
    assert runtime.artifacts.delete_unreferenced(source_id) is False
    assert runtime.artifacts.metadata(source_id)["state"] == "SEALED"
    # An unrelated integrity violation must not be mistaken for a protected reference.
    monkeypatch.setattr(artifacts, "SQL_DELETE_SET",
                        "INSERT INTO runtime_schema_version(version) SELECT 5 WHERE %s IS NOT NULL")
    with pytest.raises(IntegrityError) as failure:
        runtime.artifacts.delete_unreferenced(source_id)
    assert failure.value.orig.pgcode == "23505"
    assert runtime.artifacts.metadata(source_id)["state"] == "SEALED"
