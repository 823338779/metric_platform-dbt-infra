"""Display configuration survives publication without changing query input."""

from dbt_metricflow_service.models.native import NativeNode
from dbt_metricflow_service.platform.dimensions import map_available_dimensions
from dbt_metricflow_service.platform.models import DimensionQueryResult, NativeDimension, PlatformQueryRequest
from tests.test_publication_catalog import build, package


def test_mapping_survives_manifest_catalog():
    mapping = {"semantic_model": "returns", "value_dimension": "region", "display_dimension": "region"}
    manifest = package()
    manifest["semantic_models"]["semantic_model.sample.orders"]["dimensions"][0]["value_mapping"] = mapping
    published = build(manifest)
    dimension = published["semantic_models"]["semantic_model.sample.orders"]["dimensions"][0]
    assert dimension["value_mapping"] == mapping
    assert (
        NativeNode.model_validate({"name": "region", "value_mapping": mapping}).native_details()["value_mapping"]
        == mapping
    )
    assert "valueMappings" not in PlatformQueryRequest.model_json_schema()["properties"]


def test_available_dimension_uses_model_identity():
    native = DimensionQueryResult(
        dimensions=[NativeDimension(token="order__region", name="region", semantic_model_name="orders")]
    )
    options, _ = map_available_dimensions("build", ["metric.sample.orders"], native)
    assert options.dimensions[0].semantic_model_name == "orders"
    assert options.dimensions[0].name == "region"
