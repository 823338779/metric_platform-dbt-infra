"""Proof and write-policy flags must not gain truth through type coercion."""

import pytest
from pydantic import ValidationError

from dbt_metricflow_service.models.native import NativeNode
from dbt_metricflow_service.platform.models import EngineResult


@pytest.mark.parametrize("value", ["true", "false", 1, 0])
def test_query_proof_requires_an_actual_boolean(value):
    with pytest.raises(ValidationError):
        EngineResult.model_validate({"queryCapability": value})


@pytest.mark.parametrize("value", ["true", "false", 1, 0])
def test_test_write_policy_requires_an_actual_boolean(value):
    with pytest.raises(ValidationError):
        NativeNode.model_validate({"resource_type": "test", "config": {"store_failures": value}})
