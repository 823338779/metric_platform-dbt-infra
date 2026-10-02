"""验证路由使用独立契约，回执返回后输入已经耐久。"""

import importlib

from fastapi import FastAPI
from fastapi.testclient import TestClient

from tests.test_draft_validation_storage import runtime as runtime
from tests.test_draft_validation_storage import setup
from tests.test_publication_storage import store as store


def test_admit_and_poll_is_project_scoped(runtime):
    spec = importlib.util.find_spec("dbt_metricflow_service.draft_validation_api")
    assert spec is not None, "draft validation routes are not implemented"
    app = FastAPI()
    app.include_router(importlib.import_module(spec.name).create_draft_validation_router(runtime))
    project, request = setup(runtime)
    with TestClient(app) as client:
        path = f"/v2/projects/{project}/validations"
        response = client.post(path, json=request.model_dump(mode="json", by_alias=True))
        assert response.status_code == 202
        validation = response.json()["validationId"]
        assert client.get(path + "/" + validation).json()["state"] == "QUEUED"
        assert client.get("/v2/projects/wrong/validations/" + validation).status_code == 404
