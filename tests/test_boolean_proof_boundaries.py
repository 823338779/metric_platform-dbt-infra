"""Native boolean fields reject ambiguous string and integer values."""

import pytest
from pydantic import ValidationError

from dbt_metricflow_service.models.native import NativeNode


@pytest.mark.parametrize("value", ["true", "false", 1, 0])
def test_test_write_policy_requires_an_actual_boolean(value):
    with pytest.raises(ValidationError):
        NativeNode.model_validate({"resource_type": "test", "config": {"store_failures": value}})
