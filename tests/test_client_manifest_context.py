import ast
import json
from pathlib import Path

import pytest
from mashumaro.exceptions import MissingField
from test_sdk_integration import write_project


@pytest.fixture
def built_context(tmp_path):
    from dbt_metricflow_service.engines.dbt import DbtClient

    write_project(tmp_path)
    (tmp_path / "profiles.yml").write_text(
        "execution:\n  target: test\n  outputs:\n    test:\n      type: duckdb\n"
        f"      path: '{(tmp_path / 'warehouse.duckdb').as_posix()}'\n", encoding="utf-8",
    )
    result = DbtClient().build(tmp_path, tmp_path)
    assert result.success, result.exception
    from dbt_metricflow_service.platform.manifest_capture import ManifestCapture

    capture = ManifestCapture(64 * 1024 * 1024)
    capture(result.manifest)
    return tmp_path, capture.content


def forbid_parse_and_disk_context(project, monkeypatch):
    for filename in ("manifest.json", "semantic_manifest.json", "partial_parse.msgpack"):
        (project / "target" / filename).unlink(missing_ok=True)
    (project / "models/orders.sql").unlink()
    (project / "models/semantic.yml").write_text("invalid: [", encoding="utf-8")

    def forbidden(*args, **kwargs):
        pytest.fail("supplied context must not parse project sources")

    monkeypatch.setattr("dbt.cli.requires.parse_manifest", forbidden)


def test_dbt_client_uses_snapshot_for_resources_listing_and_preview(built_context, monkeypatch):
    from dbt_metricflow_service.engines.dbt import DbtClient

    project, content = built_context
    forbid_parse_and_disk_context(project, monkeypatch)
    client = DbtClient(content)
    assert client.resource("model.api_review.orders")["name"] == "orders"
    assert client.source_path("model.api_review.orders") == "models/orders.sql"
    assert client.lineage("orders", project=project, profiles=project)
    result = client.preview("orders", project=project, profiles=project, limit=1)
    assert len(result.rows) == 1 and result.truncated


def test_metricflow_client_uses_snapshot_for_offline_and_execution(built_context, monkeypatch):
    from dbt_metricflow_service.engines.metricflow import MetricFlowClient
    from dbt_metricflow_service.engines.models import CompiledQuery, QueryMode

    project, content = built_context
    forbid_parse_and_disk_context(project, monkeypatch)
    client = MetricFlowClient(content, project=project, profiles=project)
    dimensions = client.available_dimensions(("revenue",))
    assert any(item.name == "status" for item in dimensions.dimensions)
    request = CompiledQuery(mode=QueryMode.EXPLAIN, metrics=["revenue"])
    assert "sum" in client.query(request).sql.lower()
    result = client.query(request.model_copy(update={"mode": QueryMode.QUERY}))
    assert float(result.rows[0][0]) == 14.34
    assert not (project / "target/semantic_manifest.json").exists()
    # 冲突的磁盘产物也不能覆盖传入上下文；使用新 Client 验证不是旧引擎缓存遮蔽了读取。
    (project / "target/semantic_manifest.json").write_text("invalid", encoding="utf-8")
    fresh = MetricFlowClient(content, project=project, profiles=project)
    assert float(fresh.query(request.model_copy(update={"mode": QueryMode.QUERY})).rows[0][0]) == 14.34
    assert (project / "target/semantic_manifest.json").read_text(encoding="utf-8") == "invalid"
    changed = json.loads(content)
    changed["metrics"] = {}
    with pytest.raises(ValueError):
        MetricFlowClient(json.dumps(changed).encode()).available_dimensions(("revenue",))
    assert client.available_dimensions(("revenue",)).dimensions


@pytest.mark.parametrize("content, error", [(b"not json", json.JSONDecodeError), (b"{}", MissingField)])
def test_invalid_manifest_does_not_fall_back(content, error):
    from dbt_metricflow_service.engines.dbt import DbtClient
    from dbt_metricflow_service.engines.metricflow import MetricFlowClient

    with pytest.raises(error):
        DbtClient(content).resource("model.api_review.orders")
    with pytest.raises(error):
        MetricFlowClient(content).available_dimensions(("revenue",))


def test_manifest_sdk_access_stays_in_clients_and_capture():
    root = Path(__file__).resolve().parents[1]
    forbidden = (
        "dbt.contracts.graph.manifest", "dbt.artifacts.schemas.manifest",
        "dbt.contracts.graph.semantic_manifest", "metricflow_semantics.model.semantic_manifest_lookup",
        "metricflow_semantics.model.dbt_manifest_parser",
    )
    for path in (root / "src/dbt_metricflow_service").rglob("*.py"):
        if path.relative_to(root / "src/dbt_metricflow_service").as_posix() in {
            "engines/dbt.py", "engines/metricflow.py", "platform/manifest_capture.py",
        }:
            continue
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8-sig"))):
            if isinstance(node, ast.ImportFrom):
                assert node.module not in forbidden, path
    for package in ("vendor/dbt/core/dbt", "vendor/metricflow/dbt-metricflow/dbt_metricflow"):
        for path in (root / package).rglob("*.py"):
            assert "dbt_metricflow_service" not in path.read_text(encoding="utf-8-sig"), path
