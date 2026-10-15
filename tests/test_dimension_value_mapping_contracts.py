"""Display configuration survives publication without changing query input."""

from dbt_metricflow_service.models.artifacts import DimensionAttributes
from dbt_metricflow_service.models.native import NativeNode
from dbt_metricflow_service.platform.dimensions import map_available_dimensions
from dbt_metricflow_service.platform.models import DimensionQueryResult, NativeDimension, PlatformQueryRequest
from tests.test_publication_catalog import build, package


def test_mapping_survives_native_and_published_catalog():
    mapping = {"semantic_model": "returns", "value_dimension": "region", "display_dimension": "region"}
    native, semantic, bindings = package()
    native["resources"][1]["definition"]["dimensions"][0]["value_mapping"] = mapping
    semantic["semantic_models"][0]["dimensions"][0]["value_mapping"] = mapping
    published = build(native, semantic, bindings)
    dimension = next(item for item in published.resources if item.resource_id.endswith("orders.dimension.region"))
    assert dimension.attributes.value_mapping.semantic_model == "returns"
    assert (
        NativeNode.model_validate({"name": "region", "value_mapping": mapping}).native_details()["value_mapping"]
        == mapping
    )
    assert DimensionAttributes(datasetId="orders", dimensionType="categorical").value_mapping is None
    assert "valueMappings" not in PlatformQueryRequest.model_json_schema()["properties"]


def test_available_dimension_uses_model_identity():
    published = build()
    native = DimensionQueryResult(
        dimensions=[NativeDimension(token="order__region", name="region", semantic_model_name="orders")]
    )
    options, _ = map_available_dimensions("build", ["metric.sample.orders"], published, native)
    assert options.dimensions[0].resource_id == "semantic_model.sample.orders.dimension.region"
