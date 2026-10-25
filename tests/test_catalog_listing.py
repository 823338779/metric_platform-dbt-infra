from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from dbt.cli.main import dbtRunner
from fastapi import FastAPI
from fastapi.testclient import TestClient
from test_sdk_integration import write_project

from dbt_metricflow_service.api.catalog import router
from dbt_metricflow_service.application.catalog import CatalogService
from dbt_metricflow_service.execution.query import QueryExecutor


def test_catalog_lists_saved_manifest_resources_without_parsing_sources(tmp_path, settings, monkeypatch):
    write_project(tmp_path)
    (tmp_path / "profiles.yml").write_text(
        "execution:\n  target: test\n  outputs:\n    test:\n      type: duckdb\n      path: ':memory:'\n",
        encoding="utf-8",
    )
    parsed = dbtRunner().invoke([
        "parse", "--quiet", "--project-dir", str(tmp_path), "--profiles-dir", str(tmp_path),
    ])
    assert parsed.success, parsed.exception
    manifest_path = tmp_path / "target/manifest.json"
    saved_manifest = manifest_path.read_bytes()
    manifest_path.unlink()
    (tmp_path / "models/semantic.yml").write_text("invalid: [", encoding="utf-8")
    (tmp_path / "models/orders.sql").unlink()

    def forbidden(*args, **kwargs):
        pytest.fail("catalog must use the saved Manifest without parsing project sources")

    monkeypatch.setattr("dbt.cli.requires.parse_manifest", forbidden)
    build_id = "00000000-0000-0000-0000-000000000001"
    build = SimpleNamespace(build_id=build_id, build_status="SUCCEEDED", output_set_id="sealed-output")
    reads = []

    def read_file(set_id, path):
        reads.append((set_id, path))
        return saved_manifest

    artifacts = SimpleNamespace(read_file=read_file)
    executor = QueryExecutor(settings, artifacts)

    @contextmanager
    def context(build, manifest):
        yield manifest, tmp_path, tmp_path

    monkeypatch.setattr(executor, "_engine_context", context)
    service = CatalogService(SimpleNamespace(get=lambda _: build), artifacts, executor.lineage, 1024 * 1024)
    app = FastAPI()
    app.include_router(router(service))
    response = TestClient(app).get(f"/builds/{build_id}/catalog")
    assert response.status_code == 200, response.text
    resources = response.json()
    assert isinstance(resources, list)
    by_id = {row["unique_id"]: row for row in resources}
    assert by_id["model.api_review.orders"]["resource_type"] == "model"
    assert by_id["metric.api_review.revenue"]["resource_type"] == "metric"
    assert by_id["semantic_model.api_review.orders"]["resource_type"] == "semantic_model"
    assert reads == [("sealed-output", "target/manifest.json")]
