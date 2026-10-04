"""Forgejo 签名覆盖原始字节，事件只提交受控仓库的分支信号。"""

import hashlib
import hmac
import json
from importlib import import_module

from fastapi import FastAPI
from fastapi.testclient import TestClient

from tests.test_branch_lifecycle import context as context
from tests.test_platform_bindings import repository as repository
from tests.test_publication_storage import store as store

MODULE = "dbt_metricflow_service.branch_events"
PATH = "/internal/git-branch-events"
SECRET = "test-only-event-secret"
SIGNATURE = "X-Forgejo-Signature"
EVENT = "X-Forgejo-Event"
PUSH = "push"
UTF8 = "utf-8"


def test_bad_signature_or_other_repository_is_rejected(context, repository):
    service, project, request = context
    branch = service.create(project, request)
    runtime = service.runtime
    runtime.settings.branch_events_enabled = True
    runtime.settings.branch_event_secret = SECRET
    app = FastAPI()
    app.include_router(import_module(MODULE).create_branch_event_router(runtime))
    body = json.dumps({"ref": branch.git_ref, "repository": {"clone_url": str(repository)}}).encode(UTF8)

    def headers(raw):
        return {EVENT: PUSH, SIGNATURE: hmac.new(SECRET.encode(UTF8), raw, hashlib.sha256).hexdigest()}

    with TestClient(app) as client:
        assert client.post(PATH, content=body, headers={EVENT: PUSH, SIGNATURE: "wrong"}).status_code == 401
        other = json.dumps({"ref": branch.git_ref, "repository": {"clone_url": "https://other.invalid/repo"}}).encode()
        assert client.post(PATH, content=other, headers=headers(other)).status_code == 403
        before = service.store.get(project, str(branch.branch_id))["signal_version"]
        assert client.post(PATH, content=body, headers=headers(body)).status_code == 202
        assert service.store.get(project, str(branch.branch_id))["signal_version"] == before + 1
