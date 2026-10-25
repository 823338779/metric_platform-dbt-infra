"""唯一快照协议覆盖所有入口，旧写入不能留下副作用。"""

import pytest

from tests.test_api import AUTH
from tests.test_environment_api import client as client
from tests.test_execution_snapshot_storage import request
from tests.test_publication_storage import store as store
from tests.test_request_limits import _invoke

API_ROOT = ""
OLD_BUILD = "/v3/builds"
OLD_VERSION = "/v1/versions"
LEGACY_FIELDS = ("environment", "executionBinding", "configVersion", "schemaName")
WRITE_PATHS = ("/builds", "/query", "/query/available-dimensions")


def test_only_unversioned_business_routes_are_registered(client):
    paths = client.get("/openapi.json").json()["paths"]
    assert not any(path.startswith(("/v1/", "/v2/", "/v3/", "/v4/")) for path in paths)
    assert API_ROOT + "/versions" in paths
    assert API_ROOT + "/query" in paths
    assert API_ROOT + "/builds/{build_id}/catalog" in paths
    assert client.post(OLD_BUILD, json={}, headers=AUTH).status_code == 404
    assert client.get(OLD_VERSION).status_code == 404


@pytest.mark.parametrize("field", LEGACY_FIELDS)
def test_rejects_legacy_build_fields_before_acceptance(client, field):
    body = request().model_dump(mode="json", by_alias=True)
    body[field] = "old"
    assert client.post(API_ROOT + "/builds", json=body, headers=AUTH).status_code == 422
    assert (
        client.get(API_ROOT + "/builds", params={"environmentId": body["execution"]["environmentId"]}).json()["items"]
        == []
    )


@pytest.mark.parametrize("path", WRITE_PATHS)
async def test_body_limit_precedes_application(path):
    called, sent, _ = await _invoke(
        [{"type": "http.request", "body": b"oversized", "more_body": False}],
        path=API_ROOT + path,
    )
    assert called is False
    assert sent[0]["status"] == 413


def test_register_bindings_command_is_removed():
    from dbt_metricflow_service.admin import main

    with pytest.raises(SystemExit) as failure:
        main(["register-bindings", "unused.json"])
    assert failure.value.code == 2
