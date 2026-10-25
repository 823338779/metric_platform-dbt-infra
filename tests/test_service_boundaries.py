from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from dbt_metricflow_service.api.auth import mutation_guard
from dbt_metricflow_service.api.errors import register_exception_handlers
from dbt_metricflow_service.application.catalog import CatalogService
from dbt_metricflow_service.execution.dbt import dbt_execution
from dbt_metricflow_service.execution.errors import ExecutionError


def test_authentication_preserves_public_error_code(settings):
    app = FastAPI()
    register_exception_handlers(app)
    guard = mutation_guard(settings)
    dependency = Depends(guard)

    @app.post("/protected")
    def protected(auth=dependency):
        return {"ok": True}

    response = TestClient(app).post("/protected")
    assert response.status_code == 401
    assert response.json()["error"]["code"] == "service_credential_required"
    assert TestClient(app).post("/protected", headers={"Authorization": "Bearer test-token"}).status_code == 200


def test_lineage_passes_one_manifest_read_to_executor():
    build_id = "00000000-0000-0000-0000-000000000001"
    build = SimpleNamespace(build_id=build_id, build_status="SUCCEEDED", output_set_id="artifact")
    artifacts = SimpleNamespace(read_file=Mock(return_value=b"manifest"))
    execute = Mock(return_value=[{"unique_id": "model.orders"}])
    service = CatalogService(SimpleNamespace(get=lambda identity: build), artifacts, execute, 1024)
    assert service.lineage(build_id, "orders") == [{"unique_id": "model.orders"}]
    artifacts.read_file.assert_called_once()
    execute.assert_called_once_with(build, "orders", b"manifest")


def test_build_failure_diagnostic_does_not_invent_another_job(monkeypatch):
    @contextmanager
    def environment(values, output, sink):
        yield lambda event: output.feed(event.message.encode())

    monkeypatch.setattr("dbt_metricflow_service.execution.dbt.engine_environment", environment)
    with pytest.raises(ExecutionError) as caught:
        with dbt_execution("project", {}, 1024) as context:
            context.check(SimpleNamespace(success=False, exception=ValueError("engine failed")))
    detail = caught.value.payload
    assert "engine failed" in detail.stdout
    assert detail.stderr == "ValueError"
    assert detail.exit_code == 2
    assert not {"id", "submitted_at", "status"}.intersection(detail.model_fields_set)
