"""通过公开 HTTP 契约验证真实 StarRocks 构建与固定快照查询。

仅显式启用时运行；仓库、实例、凭据来自外部环境，不修改源码仓库。
"""

import os
import tempfile
import time
from pathlib import Path
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from dbt_metricflow_service.api.app import create_app
from dbt_metricflow_service.settings import Settings

TIMEOUT = 600


def test_real_environment_isolation(tmp_path):
    if os.getenv("ENVIRONMENT_ISOLATION_E2E") != "1":
        pytest.skip("需要显式启用真实仓库、服务数据库与 StarRocks")
    import mysql.connector

    # 独立工具链身份避免领取其他测试或本地服务的任务。
    token = uuid4().hex
    host = os.environ.get("ENVIRONMENT_TEST_HOST", "127.0.0.1")
    port = int(os.environ.get("ENVIRONMENT_TEST_PORT", "9030"))
    repository = os.environ["ENVIRONMENT_TEST_REPOSITORY"]
    branch = os.environ.get("ENVIRONMENT_TEST_BRANCH", "main")
    settings = Settings(
        profiles_dir=tmp_path, command_timeout_seconds=TIMEOUT, max_output_bytes=1048576,
        database_url=os.environ["SERVICE_TEST_DATABASE_URL"], service_token=token,
        temp_root=Path(tempfile.gettempdir()) / ("dbtenv-" + token[:8]), toolchain_version="isolation-" + token,
        execution_instances={"isolation": {"type": "starrocks", "hosts": [host], "port": port}},
        execution_credentials={"isolation": {"userEnv": "DBT_ENV_SECRET_ISOLATION_USER",
                                            "passwordEnv": "DBT_ENV_SECRET_ISOLATION_PASSWORD"}},
    )
    schemas = ["env_e2e_" + uuid4().hex for _ in range(2)]
    snapshots = [{"environmentId": str(uuid4()), "configVersion": 7,
                  "connection": {"type": "starrocks", "instanceId": "isolation", "host": host,
                                 "port": port, "path": None},
                  "credentialRef": "isolation", "catalog": "default_catalog", "schema": schema,
                  "threads": 2, "businessTimezone": "Asia/Shanghai"} for schema in schemas]
    headers = {"Authorization": "Bearer " + token}
    database = mysql.connector.connect(host=host, port=port, user=os.environ["DBT_ENV_SECRET_ISOLATION_USER"],
                               password=os.environ["DBT_ENV_SECRET_ISOLATION_PASSWORD"], autocommit=True)

    # 所有引擎操作都经过 API；数据库连接仅核查本次创建的物理对象。
    with TestClient(create_app(settings)) as client, database:
        def completed(snapshot, expected="SUCCEEDED"):
            response = client.post("/builds", headers=headers, json={"repository": repository,
                "branchName": branch, "execution": snapshot, "idempotencyKey": uuid4().hex,
                "deploymentPolicy": "ON_SUCCESS"})
            assert response.status_code == 202, response.text
            build_id = response.json()["buildId"]
            deadline = time.monotonic() + TIMEOUT
            while time.monotonic() < deadline:
                value = client.get("/builds/" + build_id).json()
                if value["buildStatus"] not in ("QUEUED", "RUNNING"):
                    assert value["buildStatus"] == expected, (
                        value, client.get(f"/builds/{build_id}/execution-logs").json())
                    return value
                time.sleep(0.5)
            pytest.fail("build timeout: " + build_id)

        def contents(schema):
            with database.cursor() as cursor:
                cursor.execute(f"SHOW FULL TABLES FROM `{schema}`")
                names = [row[0] for row in cursor.fetchall()]
                assert names
                result = {}
                for name in names:
                    cursor.execute(f"SELECT * FROM `{schema}`.`{name}`")
                    result[name] = sorted(repr(row) for row in cursor.fetchall())
                return result

        def query(build):
            catalog = client.get(f"/builds/{build['buildId']}/catalog", params={"kind": "METRIC"})
            assert catalog.status_code == 200, catalog.text
            metric = catalog.json()["items"][0]["resourceId"]
            response = client.post("/query", params={"build_id": build["buildId"]}, headers=headers,
                json={"mode": "QUERY", "metricResourceIds": [metric], "limit": 10})
            assert response.status_code == 200, response.text
            assert response.json()["rows"]
            explained = client.post("/query", params={"build_id": build["buildId"]}, headers=headers,
                json={"mode": "EXPLAIN", "metricResourceIds": [metric], "limit": 10})
            assert explained.status_code == 200, explained.text
            assert build["execution"]["schema"] in explained.json()["sql"]
            return response.json()["rows"]

        first, second = completed(snapshots[0]), completed(snapshots[1])
        before = contents(schemas[1])
        rows = query(second)
        first_rows = query(first)
        # 修改 A 的配置版本并重建，B 的物理内容、固定查询和部署身份保持不变。
        changed = dict(snapshots[0], configVersion=8, threads=1, schema="env_e2e_" + uuid4().hex)
        third = completed(changed)
        assert first["commitSha"] == second["commitSha"] == third["commitSha"]
        assert contents(schemas[1]) == before
        assert query(second) == rows
        assert query(first) == first_rows
        assert client.get("/builds/" + first["buildId"]).json()["execution"] == snapshots[0]
        history = client.get("/builds", params={"environmentId": snapshots[0]["environmentId"]}).json()
        assert {row["buildId"] for row in history["items"]} == {first["buildId"], third["buildId"]}
        target = client.get("/deployments/current", params={"repository": repository,
            "branchName": branch, "environmentId": snapshots[1]["environmentId"]})
        assert target.status_code == 200, target.text
        assert target.json()["activeBuildId"] == second["buildId"]
        # 不存在的 catalog 必须失败，不能退回默认 catalog 覆盖 B 的同名 schema。
        invalid = dict(snapshots[1], environmentId=str(uuid4()), catalog="missing_" + uuid4().hex)
        failed = completed(invalid, expected="FAILED")
        assert failed["queryAvailable"] is False
        assert contents(schemas[1]) == before
        print("isolation evidence", [{key: build[key] for key in ("buildId", "commitSha", "execution")}
                                     for build in (first, second, third)])

    # 全新 runtime 从持久快照恢复固定历史查询，不需要重新登记环境。
    with TestClient(create_app(settings)) as client:
        assert query(first) == first_rows
        assert query(second) == rows
