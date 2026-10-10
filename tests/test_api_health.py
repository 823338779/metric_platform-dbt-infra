from importlib.metadata import version
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from dbt_metricflow_service import __version__
from dbt_metricflow_service.api.app import VERSION_DISTRIBUTIONS, create_app
from dbt_metricflow_service.settings import Settings

# 基础接口保留原有开放响应契约，避免函数注解触发 FastAPI 响应推断。
INFRASTRUCTURE_ROUTES = ("/health/live", "/health/ready", "/v1/versions")
OPENAPI_RESPONSE_SCHEMA_PATH = ("get", "responses", "200", "content", "application/json", "schema")
OPENAPI_PATHS_KEY = "paths"


@pytest.fixture
def application(monkeypatch, tmp_path):
    import dbt_metricflow_service.api.app as module

    (tmp_path / "profiles.yml").write_text("fixture: {}")
    settings = Settings(tmp_path, 30, 1024, database_url="postgresql://unused/test", temp_root=tmp_path)
    runtime = SimpleNamespace(
        settings=settings,
        db=SimpleNamespace(check=lambda: None),
        jobs=SimpleNamespace(db=None),
        artifacts=None,
        toolchain="test",
        started=False,
        closed=False,
    )

    async def start():
        runtime.started = True

    async def close():
        runtime.closed = True

    runtime.start, runtime.close = start, close
    monkeypatch.setattr(module, "Runtime", lambda settings: runtime)
    return create_app(settings), runtime


def test_live_does_not_depend_on_cli(application, monkeypatch):
    app, _ = application
    monkeypatch.setattr("dbt_metricflow_service.api.app.shutil.which", lambda name: None)
    with TestClient(app) as client:
        assert client.get("/health/live").json() == {"status": "ok"}
        assert client.get("/health/ready").status_code == 503


def test_ready_accepts_installed_clis_and_readable_profiles(application, monkeypatch):
    app, _ = application
    monkeypatch.setattr("dbt_metricflow_service.api.app.shutil.which", lambda name: name)
    with TestClient(app) as client:
        assert client.get("/health/ready").json() == {"status": "ready"}


def test_versions_and_runtime_lifecycle(application):
    app, runtime = application
    with TestClient(app) as client:
        assert runtime.started
        assert client.get("/v1/versions").json() == {
            "service": __version__,
            **{name: version(name) for name in VERSION_DISTRIBUTIONS},
        }
    assert runtime.closed


@pytest.mark.parametrize("path", INFRASTRUCTURE_ROUTES)
def test_infrastructure_response_schema_is_not_inferred(application, path):
    # 类型注解仅服务于开发检查，不额外收紧这些现有 HTTP 响应。
    app, _ = application
    schema = app.openapi()[OPENAPI_PATHS_KEY][path]
    for key in OPENAPI_RESPONSE_SCHEMA_PATH:
        schema = schema[key]
    assert schema == {}
