"""Build dbt artifacts, load native MetricFlow, and execute through the service."""

import json
from uuid import uuid4

import pytest
from dbt.cli.main import dbtRunner
from dbt_metricflow.cli.cli_configuration import CLIConfiguration
from metricflow.engine.metricflow_engine import MetricFlowQueryRequest
from metricflow_semantics.errors.error_classes import DimensionValueMappingError

from dbt_metricflow_service.platform.metricflow import (
    execute_available_dimensions,
    execute_programmatic,
)
from dbt_metricflow_service.platform.models import PlatformQueryRequest
from tests.dimension_mapping_helpers import write_mapping_project


@pytest.fixture(scope="module")
def mapping_project(tmp_path_factory):
    project = tmp_path_factory.mktemp("dimension_mapping")
    write_mapping_project(project)
    result = dbtRunner().invoke(["build", "--project-dir", str(project), "--profiles-dir", str(project)])
    assert result.success, result.exception
    return project


def test_dbt_to_service_mapping_and_raw_dimension_values(mapping_project):
    project = mapping_project
    manifest = json.loads((project / "target/manifest.json").read_text(encoding="utf-8"))
    semantic = json.loads((project / "target/semantic_manifest.json").read_text(encoding="utf-8"))
    source = manifest["semantic_models"]["semantic_model.mapping_test.requests"]
    semantic_source = next(model for model in semantic["semantic_models"] if model["name"] == "requests")
    assert source["dimensions"][0]["value_mapping"] == semantic_source["dimensions"][0]["value_mapping"]
    dimensions = execute_available_dimensions(project, project, ("requests_count",))
    assert {"raw_value", "display_value"}.isdisjoint(item.name for item in dimensions.dimensions)
    assert next(item for item in dimensions.dimensions if item.name == "status").semantic_model_name == "requests"
    args = {
        "runId": uuid4(),
        "metrics": ["requests_count"],
        "groupBy": ["request__status"],
        "orderBy": ["request__status"],
        "limit": 3,
    }
    result = execute_programmatic(project, project, PlatformQueryRequest(mode="QUERY", **args))
    assert result.rows == [["成功", 100], ["失败", 20], ["失败", 10]]
    assert result.columns[0].type == "str"
    assert result.truncated
    raw = execute_programmatic(
        project, project, PlatformQueryRequest(mode="DIMENSION_VALUES", dimension="request__status", **args)
    )
    assert raw.rows == [[200], [404], [502]]
    assert raw.columns[0].type != "str"
    explained = execute_programmatic(project, project, PlatformQueryRequest(mode="EXPLAIN", **args))
    assert explained.sql.count("LEFT OUTER JOIN") == 1
    multiple = execute_programmatic(
        project,
        project,
        PlatformQueryRequest(
            mode="EXPLAIN", **{**args, "groupBy": ["request__status", "request__target_status", "request__channel"]}
        ),
    )
    assert multiple.sql.count("LEFT OUTER JOIN") == 3
    multiple_result = execute_programmatic(
        project,
        project,
        PlatformQueryRequest(
            mode="QUERY",
            **{**args, "groupBy": ["request__status", "request__target_status", "request__channel"]},
        ),
    )
    assert multiple_result.rows == [
        ["成功", "失败", "web", 100],
        ["失败", "失败", "web", 20],
        ["失败", "成功", "mobile", 10],
    ]


def test_saved_query_and_native_mapping_validator_use_native_configuration(mapping_project):
    cfg = CLIConfiguration()
    cfg.setup(dbt_project_path=mapping_project, dbt_profiles_path=mapping_project, configure_file_logging=False)
    result = cfg.mf.query(MetricFlowQueryRequest.create(saved_query_name="status_summary"))
    assert sorted(result.result_df.rows) == sorted([("成功", 100), ("失败", 20), ("失败", 10), ("599", 4)])
    adapter = cfg.dbt_artifacts.adapter
    with adapter.connection_named("bad_mapping_fixture"):
        adapter.execute("CREATE OR REPLACE VIEW status_codes AS SELECT NULL::INTEGER AS code, 'bad' AS label")
    with pytest.raises(DimensionValueMappingError, match="NULL"):
        from dbt_metricflow.cli.dbt_connectors.value_mapping_validation import validate_value_mapping_data
        validate_value_mapping_data(cfg.semantic_manifest_lookup, adapter)
