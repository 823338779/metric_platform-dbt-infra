"""目录历史读取使用固定 buildId，后续构建不使旧目录失效。"""

from uuid import uuid4

from tests.test_api import client as client
from tests.test_catalog_queries import ready_build
from tests.test_execution_profiles import secrets as secrets
from tests.test_publication_storage import store as store


def test_active_catalog_and_historical_release_access(store, tmp_path, client):
    _, first, _, _ = ready_build(store, tmp_path)
    path = "/builds/" + str(first.build_id) + "/catalog"
    assert client.get(path).json()["metrics"]
    assert client.get(path.replace(str(first.build_id), str(uuid4()))).status_code == 404
    ready_build(store, tmp_path)
    assert client.get(path).status_code == 200
    assert client.get("/builds/" + str(first.build_id)).status_code == 200


def test_catalog_http_preserves_native_keys_and_nested_dimensions(store, tmp_path, client, secrets):
    builds, build, _, artifacts = ready_build(store, tmp_path)
    base = "/builds/" + str(build.build_id)
    response = client.get(base + "/catalog")
    assert response.status_code == 200
    catalog = response.json()
    assert set(catalog) >= {"metadata", "metrics", "semantic_models", "nodes", "sources", "parent_map", "child_map"}
    metric = catalog["metrics"]["metric.revenue"]
    assert "type_params" in metric
    assert "attributes" not in metric
    assert "resourceId" not in metric
    assert catalog["semantic_models"]["semantic_model.sample.orders"]["dimensions"][0]["name"] == "region"
    assert catalog["semantic_models"]["semantic_model.sample.returns"]["dimensions"][0]["name"] == "region"
    resource_path = base + "/resources/metric.revenue"
    assert client.get(resource_path).json() == metric
    assert client.get(resource_path + "/native-details").json() == metric
    lineage_path = base + "/lineage"
    response = client.get(lineage_path, params={"selector": "1+metric:revenue+1"})
    assert response.status_code == 200, response.text
    assert {node["unique_id"] for node in response.json()} == {"metric.revenue", "semantic_model.sample.orders"}
    assert all("depends_on" in node for node in response.json())
    assert client.get(lineage_path, params={"selector": "missing_model"}).json() == []
    invalid = client.get(lineage_path, params={"selector": "bogus_method:orders"})
    assert invalid.status_code == 422
    assert invalid.json()["error"]["code"] == "INVALID_QUERY"
    assert client.get(lineage_path).status_code == 422
    assert client.get(resource_path + "/lineage").status_code == 422
    assert client.get(lineage_path, params={"selector": "+orders+"}).status_code == 200
    assert client.get(resource_path + "/unknown").status_code == 422
