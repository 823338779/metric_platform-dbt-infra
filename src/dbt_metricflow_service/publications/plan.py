"""构建模式与完整覆盖证明。"""

from .models import BuildMode, BuildPlan

MODEL = "model"
EPHEMERAL = "ephemeral"
NOT_REQUIRED = "NOT_REQUIRED"
PASSED = "PASSED"
PHYSICAL_FIELDS = ("raw_code", "config", "unrendered_config", "depends_on", "contract", "constraints")
VALIDATION_STEPS = ("tests", "semanticValidation", "relationVerification", "queryProbe")
INCOMPATIBLE = "BASELINE_INCOMPATIBLE"
CHANGED = "PHYSICAL_DEFINITION_CHANGED"
SEMANTIC = "PHYSICAL_DEFINITIONS_UNCHANGED"


def plan_publication(current: dict, baseline: dict | None, context: dict) -> BuildPlan:
    # 比较逻辑解析结果，不比较候选物理前缀、解析时间或运行 invocation。
    nodes = current["nodes"]
    physical = {key for key, node in nodes.items() if node.get("resource_type") == MODEL
                and node.get("config", {}).get("materialized") != EPHEMERAL}
    if baseline is None or any(current.get(key) != baseline.get(key) for key in ("macros", "sources")) or (
        context != baseline.get("context")
    ):
        return BuildPlan(build_mode=BuildMode.FULL_BUILD, selected_native_ids=sorted(physical),
                         reuse_native_ids=[], reasons=[INCOMPATIBLE])
    previous = baseline["nodes"]
    changed = {key for key, node in nodes.items() if key not in previous or any(
        node.get(field) != previous[key].get(field) for field in PHYSICAL_FIELDS)}
    # 变更的 ephemeral 也进入闭包，确保下游不会复用已失效的 SQL 定义。
    changed.update(set(previous) - set(nodes))
    while True:
        downstream = {key for key, node in nodes.items()
                      if changed.intersection(node.get("depends_on", {}).get("nodes", []))}
        if downstream <= changed:
            break
        changed.update(downstream)
    selected = physical & changed
    return BuildPlan(
        build_mode=BuildMode.SELECTIVE_BUILD if selected else BuildMode.SEMANTIC_ONLY,
        selected_native_ids=sorted(selected), reuse_native_ids=sorted(physical - selected),
        reasons=[CHANGED if selected else SEMANTIC], relation_bindings=baseline.get("relationBindings", []),
    )


def validate_publication_evidence(plan: BuildPlan, evidence: dict) -> dict:
    # 无构建步骤必须显式证明 NOT_REQUIRED，测试/语义/关系/查询证明不能省略。
    expected = NOT_REQUIRED if plan.build_mode == BuildMode.SEMANTIC_ONLY else PASSED
    if evidence.get("build") != expected or any(evidence.get(key) != PASSED for key in VALIDATION_STEPS):
        raise ValueError("发布步骤证明不完整")
    if set(evidence.get("coveredNativeIds", [])) != set(plan.selected_native_ids + plan.reuse_native_ids):
        raise ValueError("发布物理覆盖集合不完整")
    return evidence
