"""Owned payloads reject unknown fields while vendor projections remain lossless."""
import pytest
from pydantic import ValidationError

from dbt_metricflow_service.models.artifacts import MetricTimeConfig
from dbt_metricflow_service.models.catalog import NativeCatalog
from dbt_metricflow_service.models.native import NativeNode
from dbt_metricflow_service.models.payloads import ResultColumn
from dbt_metricflow_service.platform.models import EngineRequest, EngineResult


@pytest.mark.parametrize("model,payload", [
    (NativeCatalog, {"resources": [], "dependencies": []}),
    (EngineRequest, {"mode": "PROBE"}),
    (EngineResult, {}),
    (ResultColumn, {"name": "total", "type": "Decimal"}),
    (MetricTimeConfig, {}),
])
def test_owned_payloads_reject_extra_fields(model, payload):
    with pytest.raises(ValidationError):
        model.model_validate({**payload, "misspelled": True})


def test_native_projection_preserves_nulls_extensions_and_both_hook_spellings():
    payload = {
        "name": "orders", "config": {"pre_hook": [], "pre-hook": ["select 1"], "custom": {"x": 1}},
        "type_params": {"metrics": None, "vendor_option": {"enabled": True}},
        "vendor_details": ["preserved"],
    }
    node = NativeNode.model_validate(payload)
    assert node.config.pre_hook == ["select 1"]
    assert node.config.pre_hook_legacy == []
    assert node.type_params.metrics is None
    assert node.native_details() == payload


def test_query_engine_request_requires_query_body():
    with pytest.raises(ValidationError):
        EngineRequest(mode="QUERY")
