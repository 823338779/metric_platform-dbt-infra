"""固定 schema 保留模型表名，清理构建不能删除共用物理对象。"""

from types import SimpleNamespace
from uuid import uuid4

import pytest
from pydantic import ValidationError

from dbt_metricflow_service.models.execution import SnapshotBuildRequest as BuildRequest
from dbt_metricflow_service.runtime.executor import RuntimeExecutor
from tests.data_helpers import leased_job


@pytest.mark.parametrize("extra", [{}, {"schema": None}, {"schema": ""}])
def test_request_requires_fixed_schema(extra):
    from tests.test_build_contract import build_body

    body = build_body()
    body["execution"].pop("schema", None)
    with pytest.raises(ValidationError):
        BuildRequest.model_validate({**body, "execution": {**body["execution"], **extra}})


@pytest.mark.parametrize("input_set", [None, "source-snapshot"])
async def test_cleanup_never_materializes_or_invokes_engine(tmp_path, monkeypatch, input_set):
    def forbidden(*args, **kwargs):
        raise AssertionError("cleanup must preserve shared tables")

    executor = RuntimeExecutor(SimpleNamespace(temp_root=tmp_path), None,
                               SimpleNamespace(materialize=forbidden))
    monkeypatch.setattr(executor, "_dbt", forbidden)
    job = leased_job(kind="RUN_CLEANUP", parent_run_id=str(uuid4()), input_set_id=input_set, schema_name="analytics")
    result = await executor.execute(job)
    assert result.output_set_id is None


def test_caller_supplies_fixed_schema():
    from tests.test_build_contract import build_body

    body = build_body()
    body["execution"]["schema"] = "external_analytics"
    assert BuildRequest.model_validate(body).execution.schema_name == "external_analytics"
