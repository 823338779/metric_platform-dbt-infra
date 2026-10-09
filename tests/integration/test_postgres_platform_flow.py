from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from dbt_metricflow_service.api.app import create_app
from dbt_metricflow_service.execution.runner import JobRunner
from dbt_metricflow_service.projects import ProjectRegistry
from dbt_metricflow_service.settings import Settings

FIXTURE = Path(__file__).parents[1] / "fixtures" / "postgres_platform"
PROFILES = """postgres_platform:
  target: decoy
  outputs:
    decoy:
      type: postgres
      host: "{{ env_var('PLATFORM_TEST_PGHOST') }}"
      port: "{{ env_var('PLATFORM_TEST_PGPORT') | int }}"
      user: "{{ env_var('PLATFORM_TEST_PGUSER') }}"
      password: "{{ env_var('PLATFORM_TEST_PGPASSWORD') }}"
      dbname: "{{ env_var('PLATFORM_TEST_PGDATABASE') }}"
      schema: decoy_schema
      threads: 2
    postgres:
      type: postgres
      host: \"{{ env_var('PLATFORM_TEST_PGHOST') }}\"
      port: \"{{ env_var('PLATFORM_TEST_PGPORT') | int }}\"
      user: \"{{ env_var('PLATFORM_TEST_PGUSER') }}\"
      password: \"{{ env_var('PLATFORM_TEST_PGPASSWORD') }}\"
      dbname: \"{{ env_var('PLATFORM_TEST_PGDATABASE') }}\"
      schema: \"{{ env_var('DBT_PLATFORM_SCHEMA') }}\"
      threads: 2
"""


def git(repo: Path, *args: str) -> str:
    return subprocess.check_output(["git", "-C", str(repo), *args], text=True).strip()


def digest(repo: Path, sha: str) -> str:
    result = hashlib.sha256()
    for line in git(repo, "ls-tree", "-r", sha).splitlines():
        metadata, path = line.split("\t", 1)
        if path not in {"dbt_project.yml", "packages.yml", "package-lock.yml"} and not path.startswith(
            ("models/", "macros/")
        ):
            continue
        mode, _kind, blob = metadata.split()
        result.update(path.encode() + b"\0" + f"{mode} {blob}".encode() + b"\0")
    return result.hexdigest()


def wait_for(client: TestClient, path: str, *, timeout: float = 180) -> dict[str, object]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = client.get(path)
        assert result.status_code == 200, result.text
        snapshot = result.json()
        if snapshot["state"] in {"READY", "FAILED"}:
            return snapshot
        time.sleep(0.5)
    pytest.fail(f"task did not finish: {path}")


def test_postgres_full_build_catalog_query_and_dimension_values(tmp_path: Path) -> None:
    if os.getenv("PLATFORM_TEST_POSTGRES") != "1":
        pytest.skip("需要配置 PLATFORM_TEST_POSTGRES=1 及独立 PostgreSQL 测试库")
    repo = tmp_path / "repo"
    shutil.copytree(FIXTURE, repo)
    git(repo, "init", "-b", "main")
    git(repo, "config", "user.email", "test@example.invalid")
    git(repo, "config", "user.name", "Test")
    git(repo, "add", ".")
    git(repo, "commit", "-m", "fixture")
    sha = git(repo, "rev-parse", "HEAD")
    profile_dir = tmp_path / "profiles"
    profile_dir.mkdir()
    (profile_dir / "profiles.yml").write_text(PROFILES, encoding="utf-8")
    bindings_file = tmp_path / "bindings.json"
    bindings_file.write_text(json.dumps([{
        "projectId": "postgres_platform", "remote": str(repo),
        "projectSubdir": ".", "profileBindingId": "postgres",
    }]), encoding="utf-8")
    settings = Settings(
        projects_root=tmp_path / "projects", profiles_dir=profile_dir,
        command_timeout_seconds=1800, max_output_bytes=1024,
        job_artifacts_root=tmp_path / "artifacts", platform_bindings_file=bindings_file,
        platform_db_path=tmp_path / "jobs.sqlite",
    )
    app = create_app(settings, ProjectRegistry(settings.projects_root), JobRunner(1800, 1024))
    with TestClient(app) as client:
        response = client.post("/v1/project-runs", json={
            "projectId": "postgres_platform", "commitSha": sha, "projectDigest": digest(repo, sha),
            "profileBindingId": "postgres", "configVersion": "1", "idempotencyKey": "first",
        })
        assert response.status_code == 202, response.text
        run_id = response.json()["runId"]
        snapshot = wait_for(client, f"/v1/project-runs/{run_id}")
        assert snapshot["state"] == "READY", snapshot
        catalog = client.get(f"/v1/project-runs/{run_id}/catalog").json()
        assert {item["kind"] for item in catalog["resources"]} >= {"METRIC", "DIMENSION", "TABLE"}
        table_id = next(item["resourceId"] for item in catalog["resources"] if item["kind"] == "TABLE")
        preview = client.post("/v1/query-jobs", json={
            "runId": run_id, "idempotencyKey": "preview-one", "mode": "PREVIEW",
            "datasetResourceId": table_id, "limit": 1,
        })
        assert preview.status_code == 202, preview.text
        preview_result = wait_for(client, f"/v1/query-jobs/{preview.json()['queryId']}")
        assert preview_result["state"] == "READY", preview_result
        assert len(preview_result["rows"]) == 1
        options_response = client.get(f"/v1/project-runs/{run_id}/query-options", params={"metrics": "revenue"})
        assert options_response.status_code == 200, options_response.text
        options = options_response.json()
        assert "metric_time__month" in {item["token"] for item in options["timeDimensions"]}
        query = client.post("/v1/query-jobs", json={
            "runId": run_id, "idempotencyKey": "query-one", "mode": "QUERY",
            "metrics": ["revenue"], "groupBy": ["metric_time__month"],
        })
        assert query.status_code == 202, query.text
        result = wait_for(client, f"/v1/query-jobs/{query.json()['queryId']}")
        assert result["state"] == "READY", result
        assert len(result["rows"]) == 2
        region = next(item["token"] for item in options["dimensions"] if "region" in item["token"])
        filtered = client.post("/v1/query-jobs", json={
            "runId": run_id, "idempotencyKey": "filtered-one", "mode": "QUERY",
            "metrics": ["revenue"], "groupBy": [],
            "filters": [{"field": region, "operator": "=", "value": "A"}],
        })
        assert filtered.status_code == 202, filtered.text
        filtered_result = wait_for(client, f"/v1/query-jobs/{filtered.json()['queryId']}")
        assert filtered_result["state"] == "READY", filtered_result
        assert str(filtered_result["rows"][0][0]) == "10.25"
        explain = client.post("/v1/query-jobs", json={
            "runId": run_id, "idempotencyKey": "explain-one", "mode": "EXPLAIN",
            "metrics": ["revenue"], "groupBy": ["metric_time__month"],
        })
        assert explain.status_code == 202, explain.text
        explanation = wait_for(client, f"/v1/query-jobs/{explain.json()['queryId']}")
        assert explanation["state"] == "READY", explanation
        assert "SELECT" in explanation["sql"].upper()
        dimension = next(item["token"] for item in options["dimensions"] if "ordered_at" in item["token"])
        values = client.post("/v1/query-jobs", json={
            "runId": run_id, "idempotencyKey": "dimension-one", "mode": "DIMENSION_VALUES",
            "metrics": ["revenue"], "dimension": dimension, "limit": 10,
        })
        assert values.status_code == 202, values.text
        value_result = wait_for(client, f"/v1/query-jobs/{values.json()['queryId']}")
        assert value_result["state"] == "READY", value_result
        assert len(value_result["rows"]) == 2

        # 新提交重新构建全部定义，两个 run 的独立 schema 互不覆盖。
        (repo / "models" / "orders.sql").write_text(
            "select 1 as order_id, date '2024-03-01' as ordered_at, 30.5::numeric as revenue, 'A' as region\n",
            encoding="utf-8",
        )
        git(repo, "commit", "-am", "second")
        second_sha = git(repo, "rev-parse", "HEAD")
        second_response = client.post("/v1/project-runs", json={
            "projectId": "postgres_platform", "commitSha": second_sha,
            "projectDigest": digest(repo, second_sha), "profileBindingId": "postgres",
            "configVersion": "1", "idempotencyKey": "second",
        })
        assert second_response.status_code == 202, second_response.text
        second_id = second_response.json()["runId"]
        second_snapshot = wait_for(client, f"/v1/project-runs/{second_id}")
        assert second_snapshot["state"] == "READY", second_snapshot
        assert snapshot["schemaName"] != second_snapshot["schemaName"]
        cleanup = client.post(f"/v1/project-runs/{run_id}:cleanup")
        assert cleanup.status_code == 200, cleanup.text
        assert client.get(f"/v1/project-runs/{second_id}").json()["state"] == "READY"
