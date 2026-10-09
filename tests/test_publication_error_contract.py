"""业务拒绝公开安全的修正信息，并保留原一级错误码。"""

import importlib

import pytest
from fastapi import HTTPException

from dbt_metricflow_service.publications.api import call


async def test_structured_error_keeps_legacy_code_and_recovery():
    spec = importlib.util.find_spec("dbt_metricflow_service.publications.errors")
    assert spec is not None, "structured publication errors are not implemented"
    error_type = importlib.import_module(spec.name).PublicationError

    def reject():
        raise error_type("invalid_query_selection", "invalid_dimension_option", "groupBy[0].optionId",
                         "选项已过期，请重新获取。", False, "reload_query_options")

    with pytest.raises(HTTPException) as raised:
        await call(reject)
    error = raised.value
    assert error.status_code == 422
    assert error.detail["code"] == "invalid_query_selection"
    assert error.detail["reason"] == "invalid_dimension_option"
    assert error.detail["field"] == "groupBy[0].optionId"
    assert error.detail["recovery"] == "reload_query_options"
    assert error.detail["requestId"]


async def test_unknown_error_does_not_expose_internal_message():
    def reject():
        raise ValueError("private connection information")

    with pytest.raises(HTTPException) as raised:
        await call(reject)
    assert raised.value.detail["code"] == "invalid_query_selection"
    assert raised.value.detail["reason"] == "invalid_query_selection"
    assert raised.value.detail["recovery"] == "fix_query_selection"
    assert "private" not in str(raised.value.detail)
