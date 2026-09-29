"""两个独立 HTTP 进程和真实 dbt/MetricFlow/PostgreSQL 的验收。"""

import os
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path
from uuid import uuid4

import httpx
import pytest

from tests.integration.test_postgres_platform_flow import FIXTURE, PROFILES, digest, git


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def poll(client, url, timeout=150):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        response = client.get(url)
        assert response.status_code == 200, response.text
        value = response.json()
        if value["state"] in {"READY", "FAILED"}:
            return value
        time.sleep(0.25)
    pytest.fail("runtime task did not finish")


def test_real_build_and_queries_survive_receiving_process_exit(tmp_path):
    if os.getenv("SERVICE_RUNTIME_E2E") != "1":
        pytest.skip("设置 SERVICE_RUNTIME_E2E=1、服务测试 DSN 和目标 PostgreSQL 参数")
    from dbt_metricflow_service.storage.jobs import JobStore
    from dbt_metricflow_service.storage.postgres import Database

    database = Database(os.environ["SERVICE_TEST_DATABASE_URL"])
    database.migrate()
    project_id = "e2e_" + uuid4().hex
    toolchain = uuid4().hex
    repo = tmp_path / "repo"
    shutil.copytree(FIXTURE, repo)
    git(repo, "init", "-b", "main")
    git(repo, "config", "user.email", "test@example.invalid")
    git(repo, "config", "user.name", "Test")
    git(repo, "add", ".")
    git(repo, "commit", "-m", "fixture")
    sha = git(repo, "rev-parse", "HEAD")
    JobStore(database).register_project(project_id, {
        "projectId": project_id, "remote": str(repo), "projectSubdir": ".",
        "profileBindingId": "postgres",
    })
    profiles = tmp_path / "profiles"
    profiles.mkdir()
    (profiles / "profiles.yml").write_text(PROFILES, encoding="utf-8")
    # 工厂脚本在测试临时目录生成，不依赖本地项目挂载或持久产物目录。
    factory = tmp_path / "server.py"
    factory.write_text(
        "from dbt_metricflow_service.runtime_api import create_runtime_app\n"
        "from dbt_metricflow_service.settings import Settings\n"
        "app=create_runtime_app(Settings.from_environment())\n", encoding="utf-8",
    )
    processes, logs, urls = [], [], []
    try:
        for index in range(2):
            port = free_port()
            urls.append(f"http://127.0.0.1:{port}")
            environment = {**os.environ, "SERVICE_DATABASE_URL": os.environ["SERVICE_TEST_DATABASE_URL"],
                           "SERVICE_TOOLCHAIN_VERSION": toolchain, "DBT_PROFILES_DIR": str(profiles),
                           "SERVICE_TEMP_ROOT": str(tmp_path / f"node-{index}"),
                           "PYTHONUTF8": "1",
                           "PATH": str(Path(sys.executable).parent) + os.pathsep + os.environ["PATH"]}
            output = (tmp_path / f"node-{index}.log").open("w", encoding="utf-8")
            logs.append(output)
            processes.append(subprocess.Popen(
                [sys.executable, "-m", "uvicorn", "server:app", "--host", "127.0.0.1", "--port", str(port)],
                cwd=tmp_path, env=environment, stdout=output, stderr=output,
                creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
            ))
        with httpx.Client(timeout=45) as client:
            for url in urls:
                deadline = time.monotonic() + 30
                while True:
                    try:
                        if client.get(url + "/health/ready").status_code == 200:
                            break
                    except httpx.HTTPError:
                        pass
                    if time.monotonic() > deadline:
                        pytest.fail("runtime HTTP process did not become ready")
                    time.sleep(0.2)
            accepted = client.post(urls[0] + "/v1/project-runs", json={
                "projectId": project_id, "commitSha": sha, "projectDigest": digest(repo, sha),
                "profileBindingId": "postgres", "configVersion": "1", "idempotencyKey": toolchain,
            })
            assert accepted.status_code == 202, accepted.text
            run_id = accepted.json()["runId"]
            snapshot = poll(client, urls[1] + "/v1/project-runs/" + run_id)
            assert snapshot["state"] == "READY", snapshot
            # 停掉受理节点，后续读取与查询只能依靠另一进程和 PostgreSQL。
            processes[0].terminate()
            processes[0].wait(timeout=15)
            catalog = client.get(urls[1] + "/v1/project-runs/" + run_id + "/catalog")
            assert catalog.status_code == 200, catalog.text
            assert catalog.json()["resources"]
            options = client.get(urls[1] + "/v1/project-runs/" + run_id + "/query-options",
                                 params={"metrics": "revenue"})
            assert options.status_code == 200, options.text
            invalid = client.get(urls[1] + "/v1/project-runs/" + run_id + "/query-options",
                                 params={"metrics": "missing_metric"})
            assert invalid.status_code == 422, invalid.text
            response = client.post(urls[1] + "/v1/query-jobs", json={
                "runId": run_id, "idempotencyKey": "query-" + toolchain, "mode": "QUERY",
                "metrics": ["revenue"], "groupBy": ["metric_time__month"], "limit": 10,
            })
            assert response.status_code == 202, response.text
            result = poll(client, urls[1] + "/v1/query-jobs/" + response.json()["queryId"])
            assert result["state"] == "READY", result
            assert result["rows"]
            cleanup = client.post(urls[1] + "/v1/project-runs/" + run_id + ":cleanup")
            assert cleanup.status_code == 200, cleanup.text
            assert cleanup.json() == {"state": "CLEANED"}
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=15)
        for output in logs:
            output.close()
        database.close()
