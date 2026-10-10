"""错误保持稳定语义，禁止向调用方泄露内部异常正文。"""

from fastapi import FastAPI
from fastapi.testclient import TestClient

from dbt_metricflow_service.api.errors import register_exception_handlers
from dbt_metricflow_service.application.errors import ServiceError


def test_structured_error_preserves_code_and_phase():
    app = FastAPI()
    register_exception_handlers(app)

    @app.get("/reject")
    def reject():
        raise ServiceError("INVALID_DIMENSION_OPTION", "选项已过期，请重新获取。", phase="QUERYING")

    response = TestClient(app).get("/reject")
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "INVALID_DIMENSION_OPTION"
    assert response.json()["error"]["phase"] == "QUERYING"
    assert response.json()["error"]["retryable"] is False


def test_unknown_error_does_not_expose_internal_message():
    app = FastAPI()
    register_exception_handlers(app)

    @app.get("/reject")
    def reject():
        raise ValueError("private connection information")

    response = TestClient(app, raise_server_exceptions=False).get("/reject")
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "INVALID_REQUEST"
    assert "private" not in response.text
