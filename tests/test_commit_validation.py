"""验证任务只绑定固定提交，鉴权和幂等在 v2 边界生效。"""

from types import SimpleNamespace
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from dbt_metricflow_service.publications.models import FixedCommitRequest
from dbt_metricflow_service.storage.jobs import JobStore, StoreConflict
from dbt_metricflow_service.validation.api import create_commit_validation_router
from dbt_metricflow_service.validation.service import CommitValidationService
from tests.test_publication_storage import store as store


@pytest.fixture
def runtime(store, tmp_path):
    jobs = JobStore(store.db)
    project = "validate-" + uuid4().hex
    jobs.register_project(project, {"remote": "controlled", "projectSubdir": ".", "profileBindingId": "postgres"})
    return SimpleNamespace(db=store.db, jobs=jobs, toolchain="test", project=project,
                           settings=SimpleNamespace(config_version="1", command_timeout_seconds=60,
                                                    temp_root=tmp_path, service_token="fixture"))


def test_commit_validation_pins_request_and_recovers_after_config_change(runtime):
    service = CommitValidationService(runtime)
    request = FixedCommitRequest(commitSha="a" * 40, idempotencyKey=uuid4().hex)
    first = service.submit(runtime.project, request)
    row = runtime.jobs.get(str(first.validation_id))
    assert row["request_json"]["commitSha"] == request.commit_sha
    assert "changes" not in row["request_json"]
    assert service.get(runtime.project, str(first.validation_id)).commit_sha == request.commit_sha
    with pytest.raises(KeyError):
        service.get("other-project", str(first.validation_id))
    runtime.jobs.register_project(runtime.project, config_version="2")
    assert service.submit(runtime.project, request) == first
    with pytest.raises(StoreConflict):
        service.submit(runtime.project, request.model_copy(update={"commit_sha": "b" * 40}))


def test_validation_api_requires_service_token_and_rejects_drafts(runtime):
    app = FastAPI()
    app.include_router(create_commit_validation_router(runtime))
    with TestClient(app) as client:
        path = f"/v2/projects/{runtime.project}/validations"
        request = {"commitSha": "a" * 40, "idempotencyKey": uuid4().hex}
        assert client.post(path, json=request).status_code == 401
        headers = {"Authorization": "Bearer fixture"}
        assert client.post(path, json={**request, "changes": []}, headers=headers).status_code == 422
        accepted = client.post(path, json=request, headers=headers)
        assert accepted.status_code == 202
        result = client.get(path + "/" + accepted.json()["validationId"])
        assert result.status_code == 200
        assert result.json()["commitSha"] == request["commitSha"]


def test_concurrent_retry_recovers_before_checking_changed_config(runtime, monkeypatch):
    service = CommitValidationService(runtime)
    request = FixedCommitRequest(commitSha="a" * 40, idempotencyKey=uuid4().hex)
    original = runtime.jobs.by_key
    accepted = []
    first_lookup = True

    def lookup_before_other_request_commits(*args):
        nonlocal first_lookup
        if not first_lookup:
            return original(*args)
        first_lookup = False
        assert original(*args) is None
        accepted.append(service.submit(runtime.project, request))
        runtime.jobs.register_project(runtime.project, config_version="2")
        return None

    monkeypatch.setattr(runtime.jobs, "by_key", lookup_before_other_request_commits)
    assert service.submit(runtime.project, request) == accepted[0]
