"""旧验证操作收敛为构建，保留固定提交、重试及凭据约束。"""
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest

from dbt_metricflow_service.application.errors import ServiceError
from tests.test_publication_storage import store as store
from tests.test_v3_api import AUTH
from tests.test_v3_api import client as client
from tests.test_v3_build_acceptance import CALLER, request, service
from tests.test_v3_contract import build_body


def test_commit_validation_pins_request_and_recovers_after_config_change(store):
    app = service(store)
    body = request()
    first = app.submit(body, CALLER)
    assert app.get(first.build_id).commit_sha == body.commit_sha
    row = app.store.get(first.build_id)
    assert "changes" not in row["request_json"]
    with pytest.raises(ServiceError):
        app.get(uuid4())
    app.store.register_binding(body.repository, body.execution_binding, "2",
        {"profileBindingId": "other", "environments": ["PREVIEW"]})
    assert app.submit(body, CALLER).build_id == first.build_id
    with pytest.raises(ServiceError):
        app.submit(body.model_copy(update={"commit_sha": "b" * 40}), CALLER)


def test_validation_api_requires_service_token_and_rejects_drafts(client):
    path = "/v3/builds"
    request = build_body(idempotencyKey=uuid4().hex)
    assert client.post(path, json=request).status_code == 401
    assert client.post(path, json={**request, "changes": []}, headers=AUTH).status_code == 422
    accepted = client.post(path, json=request, headers=AUTH)
    assert accepted.status_code == 202
    result = client.get(path + "/" + accepted.json()["buildId"])
    assert result.status_code == 200
    assert result.json()["commitSha"] == request["commitSha"]
    assert client.post("/v2/projects/removed/validations", json=request, headers=AUTH).status_code == 404



def test_concurrent_retry_recovers_before_checking_changed_config(store):
    app = service(store)
    body = request()
    first = app.submit(body, CALLER)
    # 原配置下线后，已提交请求仍在同一个受理事务中优先恢复。
    with store.db.transaction() as connection:
        connection.exec_driver_sql("DELETE FROM engine_execution_binding WHERE repository=%s", (body.repository,))
    with ThreadPoolExecutor(max_workers=2) as pool:
        views = list(pool.map(lambda _: app.submit(body, CALLER), range(2)))
    assert all(view.build_id == first.build_id for view in views)
