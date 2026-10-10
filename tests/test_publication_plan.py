"""选择性构建必须完整覆盖实际下游并保留直接绑定。"""


import pytest

from dbt_metricflow_service.platform.build_plan import validate_publication_evidence


def state():
    return {"nodes": {
        "model.p.a": {"raw_code": "select 1", "config": {"materialized": "table"},
                      "resource_type": "model", "depends_on": {"nodes": []}},
        "model.p.b": {"raw_code": "select * from {{ ref('a') }}", "config": {"materialized": "view"},
                      "resource_type": "model", "depends_on": {"nodes": ["model.p.a"]}},
        "model.p.c": {"raw_code": "select 3", "config": {"materialized": "table"},
                      "resource_type": "model", "depends_on": {"nodes": []}},
    }, "macros": {}, "sources": {}, "context": {"configVersion": "1"}, "relationBindings": []}


def test_full_build_includes_every_physical_model():
    import dbt_metricflow_service.platform.build_plan as planner
    assert hasattr(planner, "full_build_plan")
    current = state()
    current["nodes"]["model.p.transient"] = {"resource_type": "model", "config": {"materialized": "ephemeral"}}
    plan = planner.full_build_plan(current)
    assert plan.build_mode == "FULL_BUILD"
    assert plan.selected_native_ids == ["model.p.a", "model.p.b", "model.p.c"]
    assert plan.reuse_native_ids == []


def test_full_build_evidence_cannot_omit_build_or_models():
    import dbt_metricflow_service.platform.build_plan as planner
    assert hasattr(planner, "full_build_plan")
    plan = planner.full_build_plan(state())
    valid = {"build": "PASSED", "tests": "PASSED", "semanticValidation": "PASSED",
             "relationVerification": "PASSED", "queryProbe": "PASSED",
             "coveredNativeIds": list(state()["nodes"])}
    assert validate_publication_evidence(plan, valid) == valid
    for change in ({"build": "NOT_REQUIRED"}, {"coveredNativeIds": []}, {"tests": "FAILED"}):
        with pytest.raises(ValueError):
            validate_publication_evidence(plan, {**valid, **change})
