"""目录历史读取使用固定 buildId，后续构建不使旧目录失效。"""

from uuid import uuid4

from tests.test_publication_storage import store as store
from tests.test_v3_api import client as client
from tests.test_v3_catalog_queries import ready_build


def test_active_catalog_and_historical_release_access(store, tmp_path, client):
    _, first, _, _ = ready_build(store, tmp_path)
    path = "/v3/builds/" + str(first.build_id) + "/catalog"
    assert client.get(path).json()["items"]
    assert client.get(path + "?limit=0").status_code == 422
    assert client.get(path.replace(str(first.build_id), str(uuid4()))).status_code == 404
    ready_build(store, tmp_path)
    assert client.get(path).status_code == 200
    assert client.get("/v3/builds/" + str(first.build_id)).status_code == 200
