"""分支写 API 要求部署凭据，响应不包含绑定或凭据。"""

from importlib import import_module

from fastapi import FastAPI
from fastapi.testclient import TestClient

from tests.test_branch_lifecycle import context as context
from tests.test_platform_bindings import repository as repository
from tests.test_publication_storage import store as store

MODULE = "dbt_metricflow_service.branch_api"
PREFIX = "/v2/projects/"
BRANCHES = "/branches"
HEADER = "Authorization"
TOKEN = "test-only-branch-service-token"
BEARER = "Bearer "
JSON = "json"
EXPECTED_VERSION = "expectedVersion"


def test_mutation_requires_service_credential_and_returns_public_fields(context):
    # 写入口默认拒绝无凭据请求；列表可读，不暴露内部执行配置。
    service, project, request = context
    service.runtime.settings.branch_service_token = TOKEN
    app = FastAPI()
    app.include_router(import_module(MODULE).create_branch_router(service.runtime))
    with TestClient(app) as client:
        path = PREFIX + project + BRANCHES
        body = request.model_dump(mode=JSON, by_alias=True)
        assert client.post(path, json=body).status_code == 401
        assert client.post(path, json=body, headers={HEADER: BEARER + TOKEN + "bad"}).status_code == 401
        response = client.post(path, json=body, headers={HEADER: BEARER + TOKEN})
        assert response.status_code == 202
        branch = response.json()
        assert branch["gitRef"] == "refs/heads/feature"
        assert "bindingConfig" not in branch
        assert TOKEN not in response.text
        assert len(client.get(path).json()) == 2
        assert client.get(path + "/" + branch["branchId"]).status_code == 200
        assert client.delete(path + "/" + branch["branchId"],
                             params={EXPECTED_VERSION: branch["version"]}).status_code == 401


def test_unconfigured_mutation_is_disabled(context):
    service, project, request = context
    service.runtime.settings.branch_service_token = None
    app = FastAPI()
    app.include_router(import_module(MODULE).create_branch_router(service.runtime))
    with TestClient(app) as client:
        assert client.post(PREFIX + project + BRANCHES, json=request.model_dump(mode=JSON, by_alias=True),
                           headers={HEADER: BEARER + TOKEN}).status_code == 503
