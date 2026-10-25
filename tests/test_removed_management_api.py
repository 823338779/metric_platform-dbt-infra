"""退役管理入口不再暴露契约，也不能改变构建和自动部署事实。"""

from tests.test_api import AUTH
from tests.test_environment_api import client as client
from tests.test_environment_api import submit
from tests.test_publication_storage import store as store


def test_removed_management_contracts_are_absent(client):
    document = client.get("/openapi.json").json()
    paths = document["paths"]
    assert "post" not in paths["/deployments"]
    assert "/builds/{build_id}/cancel" not in paths
    assert "/builds/{build_id}/resources/{resource_id}/{view}" not in paths
    assert "/builds/{build_id}/resources/{resource_id}/source" in paths
    assert "DeploymentRequest" not in document["components"]["schemas"]
    assert "cancelRequested" not in document["components"]["schemas"]["SnapshotBuildView"]["properties"]


def test_removed_management_requests_preserve_queued_build_and_initial_deployment(client):
    build = submit(client, deploymentPolicy="ON_SUCCESS")
    path = "/builds/" + build["buildId"]
    assert "cancelRequested" not in build
    assert client.post(path + "/cancel", headers=AUTH).status_code == 404
    assert client.post("/deployments", headers=AUTH, json={
        "buildId": build["buildId"], "branchName": build["branchName"],
        "expectedTargetVersion": 0, "idempotencyKey": "retired-deployment",
    }).status_code == 405
    assert client.get(path + "/resources/metric.orders/native-details").status_code == 404
    after = client.get(path).json()
    assert after == build
    history = client.get("/deployments", params={
        "repository": build["repository"], "environmentId": build["execution"]["environmentId"],
        "branchName": build["branchName"],
    }).json()["items"]
    assert history == [build["initialDeployment"]]


def test_retired_writes_skip_body_buffering(client):
    # 已退役路径不再进入写入体缓冲；无需提供认证或可解析的 JSON。
    for path, expected in (("/deployments", 405), ("/builds/retired/cancel", 404)):
        response = client.post(path, content=b"invalid", headers={"content-length": str(9 * 1024 * 1024)})
        assert response.status_code == expected
