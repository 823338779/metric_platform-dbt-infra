import json

import pytest
from dbt.cli.main import dbtRunner
from test_sdk_integration import write_project

from dbt_metricflow_service.engines import InvalidDimensionsRequest
from dbt_metricflow_service.engines.metricflow import MetricFlowClient


def test_dimensions_use_only_supplied_manifest(tmp_path, monkeypatch):
    # 先生成真实 dbt 构建产物，执行维度发现时禁止重新加载项目或写入产物。
    write_project(tmp_path)
    (tmp_path / "profiles.yml").write_text(
        "execution:\n  target: test\n  outputs:\n    test:\n      type: duckdb\n      path: ':memory:'\n",
        encoding="utf-8",
    )
    parsed = dbtRunner().invoke([
        "parse", "--quiet", "--project-dir", str(tmp_path), "--profiles-dir", str(tmp_path),
    ])
    assert parsed.success, parsed.exception
    content = (tmp_path / "target/manifest.json").read_bytes()

    def forbidden(*args, **kwargs):
        pytest.fail("dimension discovery must not load project files or write a semantic manifest")

    monkeypatch.setattr("dbt_metricflow_service.engines.metricflow.dbtProjectMetadata.load_from_paths", forbidden)
    monkeypatch.setattr("dbt.contracts.graph.semantic_manifest.SemanticManifest.write_json_to_file", forbidden)
    # 不同请求传入不同快照，不能复用磁盘上的旧 Manifest 或上一请求的引擎。
    client = MetricFlowClient(content)
    result = client.available_dimensions(("revenue",))
    assert any(d.name == "status" for d in result.dimensions)
    assert any(d.granularity == "day" for d in result.time_dimensions)
    changed = json.loads(content)
    changed["metrics"] = {}
    with pytest.raises(InvalidDimensionsRequest):
        MetricFlowClient(json.dumps(changed).encode()).available_dimensions(("revenue",))
