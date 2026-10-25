"""历史 Manifest 由原生 SDK 消费，禁止重新解析业务定义。"""

import shutil
from uuid import uuid4

import pytest

from dbt_metricflow_service.delegates.clients import InvalidDimensionsRequest
from dbt_metricflow_service.delegates.clients.dbt_client import DbtClient
from dbt_metricflow_service.delegates.clients.metric_flow_client import MetricFlowClient
from dbt_metricflow_service.execution.embedded import create_output, engine_environment
from dbt_metricflow_service.platform.manifest_capture import ManifestCapture
from dbt_metricflow_service.platform.models import PlatformQueryRequest
from dbt_metricflow_service.runtime.dbt_executor import build_engine_environment
from tests.resource_helpers import make_resource_project


def test_native_sdks_use_saved_manifest_without_source_parse(tmp_path, monkeypatch):
    project, profiles, _ = make_resource_project(tmp_path)
    environment = build_engine_environment(project, profiles, "main", "test")
    output = create_output(4096)
    with engine_environment(environment, output):
        result = DbtClient().build(project, profiles)
        assert result.success, result.exception
        capture = ManifestCapture(10 * 1024 * 1024)
        capture(result.manifest)
        content = capture.content
        normalized = content.decode().replace("\\\\", "\\").replace("\\", "/")
        assert project.as_posix() not in normalized
        assert profiles.as_posix() not in normalized
    output.finish()
    project_config = (project / "dbt_project.yml").read_bytes()
    profile_config = (profiles / "profiles.yml").read_bytes()
    # 构建目录彻底销毁，历史查询只使用原文字节和重新准备的执行上下文。
    assert project.resolve().is_relative_to(tmp_path.resolve())
    assert profiles.resolve().is_relative_to(tmp_path.resolve())
    shutil.rmtree(project)
    shutil.rmtree(profiles)
    project, profiles = tmp_path / "restored-project", tmp_path / "restored-profiles"
    project.mkdir()
    profiles.mkdir()
    (project / "target").mkdir()
    (project / "dbt_project.yml").write_bytes(project_config)
    (profiles / "profiles.yml").write_bytes(profile_config)
    environment = build_engine_environment(project, profiles, "main", "test")
    output = create_output(4096)
    with engine_environment(environment, output):
        dbt = DbtClient(content)
        catalog = dbt.catalog()
        metric = next(iter(catalog["metrics"].values()))["name"]
        model_id, model = next((key, node) for key, node in catalog["nodes"].items()
                              if node["resource_type"] == "model" and node["name"] == "orders")
        (project / "target/semantic_manifest.json").write_text("stale semantic artifact")
        def forbid(*args, **kwargs):
            raise AssertionError("historical requests must not parse source YAML")
        monkeypatch.setattr("dbt.cli.requires.parse_manifest", forbid)
        monkeypatch.setattr("dbt.parser.manifest.ManifestLoader.get_full_manifest", forbid)
        assert dbt.resource(model_id)["name"] == "orders"
        with monkeypatch.context() as metadata_only:
            def no_connection(*args, **kwargs):
                pytest.fail("dbt ls must not connect to the warehouse")
            metadata_only.setattr("dbt.adapters.base.impl.BaseAdapter.acquire_connection", no_connection)
            lineage = dbt.lineage("1+orders+1", project=project, profiles=profiles)
            assert model_id in {node["unique_id"] for node in lineage}
            assert any(node["depends_on"]["nodes"] for node in lineage)
            assert dbt.lineage("missing_model", project=project, profiles=profiles) == []
        client = MetricFlowClient(content, project=project, profiles=profiles)
        # 内存原文是依据，不能再次读取 target 中的 Manifest。
        (project / "target/manifest.json").write_text("stale manifest artifact")
        dimensions = client.available_dimensions((metric,))
        assert dimensions.metrics[0].name == metric
        for mode in ("QUERY", "EXPLAIN"):
            query = client.query(PlatformQueryRequest(
                runId=uuid4(), mode=mode, metrics=[metric]))
            assert query.sql if mode == "EXPLAIN" else query.rows
            with pytest.raises(InvalidDimensionsRequest):
                client.query(PlatformQueryRequest(runId=uuid4(), mode=mode, metrics=["missing_metric"]))
        preview = dbt.preview(model["name"], project=project, profiles=profiles, limit=1)
        assert preview.rows
        assert len(preview.rows) == 1
    output.finish()
