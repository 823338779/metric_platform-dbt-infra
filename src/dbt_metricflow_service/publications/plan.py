"""构建模式与完整覆盖证明。"""
from .models import BuildMode, BuildPlan

MODEL = "model"
EPHEMERAL = "ephemeral"
PASSED = "PASSED"
VALIDATION_STEPS = ("tests", "semanticValidation", "relationVerification", "queryProbe")


def full_build_plan(manifest: dict) -> BuildPlan:
    physical = [key for key, node in manifest["nodes"].items() if node.get("resource_type") == MODEL
                and node.get("config", {}).get("materialized") != EPHEMERAL]
    return BuildPlan(build_mode=BuildMode.FULL_BUILD, selected_native_ids=sorted(physical),
                     reuse_native_ids=[], reasons=["FULL_BUILD_POLICY"])


def validate_publication_evidence(plan: BuildPlan, evidence: dict) -> dict:
    # 无构建步骤必须显式证明 NOT_REQUIRED，测试/语义/关系/查询证明不能省略。
    expected = PASSED
    if evidence.get("build") != expected or any(evidence.get(key) != PASSED for key in VALIDATION_STEPS):
        raise ValueError("发布步骤证明不完整")
    if set(evidence.get("coveredNativeIds", [])) != set(plan.selected_native_ids + plan.reuse_native_ids):
        raise ValueError("发布物理覆盖集合不完整")
    return evidence
