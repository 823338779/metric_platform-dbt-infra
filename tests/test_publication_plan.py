"""选择性构建必须完整覆盖实际下游并保留直接绑定。"""

from copy import deepcopy

import pytest

from dbt_metricflow_service.publications.plan import plan_publication, validate_publication_evidence


def state():
    return {"nodes": {
        "model.p.a": {"raw_code": "select 1", "config": {"materialized": "table"},
                      "resource_type": "model", "depends_on": {"nodes": []}},
        "model.p.b": {"raw_code": "select * from {{ ref('a') }}", "config": {"materialized": "view"},
                      "resource_type": "model", "depends_on": {"nodes": ["model.p.a"]}},
        "model.p.c": {"raw_code": "select 3", "config": {"materialized": "table"},
                      "resource_type": "model", "depends_on": {"nodes": []}},
    }, "macros": {}, "sources": {}, "context": {"configVersion": "1"}, "relationBindings": []}


def test_build_modes_and_downstream_closure():
    previous = state()
    assert plan_publication(previous, None, previous["context"]).build_mode == "FULL_BUILD"
    assert plan_publication(previous, previous, previous["context"]).build_mode == "SEMANTIC_ONLY"
    changed = deepcopy(previous)
    changed["nodes"]["model.p.a"]["raw_code"] = "select 2"
    plan = plan_publication(changed, previous, previous["context"])
    assert plan.build_mode == "SELECTIVE_BUILD"
    assert plan.selected_native_ids == ["model.p.a", "model.p.b"]
    assert plan.reuse_native_ids == ["model.p.c"]


@pytest.mark.parametrize("field", ["macros", "sources", "context"])
def test_uncertain_environment_requires_full_build(field):
    previous, changed = state(), state()
    changed[field] = {"changed": True}
    assert plan_publication(changed, previous, changed["context"]).build_mode == "FULL_BUILD"


def test_semantic_evidence_cannot_fabricate_build_results():
    current = state()
    plan = plan_publication(current, current, current["context"])
    valid = {"build": "NOT_REQUIRED", "tests": "PASSED", "semanticValidation": "PASSED",
             "relationVerification": "PASSED", "queryProbe": "PASSED",
             "coveredNativeIds": list(current["nodes"])}
    assert validate_publication_evidence(plan, valid) == valid
    with pytest.raises(ValueError):
        validate_publication_evidence(plan, {**valid, "coveredNativeIds": []})
    with pytest.raises(ValueError):
        validate_publication_evidence(plan, {**valid, "tests": "FAILED"})
