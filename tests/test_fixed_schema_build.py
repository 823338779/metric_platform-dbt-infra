"""固定 schema 保留模型表名，清理构建不能删除共用物理对象。"""


import pytest
from pydantic import ValidationError

from dbt_metricflow_service.models.execution import SnapshotBuildRequest as BuildRequest


@pytest.mark.parametrize("extra", [{}, {"schema": None}, {"schema": ""}])
def test_request_requires_fixed_schema(extra):
    from tests.test_build_contract import build_body

    body = build_body()
    body["execution"].pop("schema", None)
    with pytest.raises(ValidationError):
        BuildRequest.model_validate({**body, "execution": {**body["execution"], **extra}})




def test_caller_supplies_fixed_schema():
    from tests.test_build_contract import build_body

    body = build_body()
    body["execution"]["schema"] = "external_analytics"
    assert BuildRequest.model_validate(body).execution.schema_name == "external_analytics"
