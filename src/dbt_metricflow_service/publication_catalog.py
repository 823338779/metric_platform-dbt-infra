"""发布前生成展示目录；未知关键语义必须失败，不能留给平台解释。"""

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

from .publication_models import PUBLISHED_CATALOG_FILE, BindingMode, PublishedCatalog, RelationBinding, ResourceKind

SIMPLE = "simple"
DERIVED = "derived"
RATIO = "ratio"
CUMULATIVE = "cumulative"
CONVERSION = "conversion"
METRIC_TYPES = frozenset((SIMPLE, DERIVED, RATIO, CUMULATIVE, CONVERSION))
PREVIEW = "PREVIEW"
QUERY = "QUERY"
DIMENSION_VALUES = "DIMENSION_VALUES"
DEPENDENCY = "DBT_DEPENDS_ON"
MEMBERSHIP = "SEMANTIC_DIMENSION"
METRIC_INPUT = "METRIC_INPUT"
METRIC_DATASET = "METRIC_DATASET"
MODEL_DATASET = "MODEL_DATASET"
SEMANTIC_FIELDS = ("dimensions", "entities", "measures", "defaults")
METRIC_FIELDS = ("type", "type_params", "filter", "time_granularity")
SEMANTIC_FILE = "semantic_manifest.json"
UTF8 = "utf-8"
JSON_MODE = "json"
SOURCE_TYPE = "source"


def _canonical(value):
    # 两种原生产物中的空默认值不影响语义比较；数组保持定义顺序。
    if isinstance(value, dict):
        return {key: _canonical(item) for key, item in value.items() if item not in (None, [], {})}
    if isinstance(value, list):
        return [_canonical(item) for item in value]
    return value


def _validate_semantics(resources: list[dict], semantic: dict) -> None:
    # 名称必须唯一且集合一致；不能把缺少语义产物误判为无指标。
    for kind, section, fields in (
        (ResourceKind.SEMANTIC_MODEL, "semantic_models", SEMANTIC_FIELDS),
        (ResourceKind.METRIC, "metrics", METRIC_FIELDS),
    ):
        definitions = semantic.get(section)
        if not isinstance(definitions, list):
            raise ValueError("缺少完整语义产物")
        indexed = {item["name"]: item for item in definitions}
        candidates = [item for item in resources if item["kind"] == kind]
        expected = {item["name"]: item["definition"] for item in candidates}
        if (len(indexed) != len(definitions) or len(expected) != len(candidates)
                or indexed.keys() != expected.keys()):
            raise ValueError("原生产物语义集合不一致")
        for name, definition in expected.items():
            if any(_canonical(definition.get(key)) != _canonical(indexed[name].get(key)) for key in fields):
                raise ValueError("原生产物语义定义不一致")


def build_published_catalog(
    *, project_id: str, release_id: UUID, native_catalog: dict,
    semantic_manifest: dict, relation_bindings: list[RelationBinding],
) -> PublishedCatalog:
    # 输入必须包含完整目录结构，并确保逻辑 ID 与原生身份一致。
    resources = native_catalog.get("resources")
    if not isinstance(resources, list) or not isinstance(native_catalog.get("dependencies"), list):
        raise ValueError("缺少完整原生目录")
    indexed = {item["resourceId"]: item for item in resources}
    if len(indexed) != len(resources) or any(item["nativeId"] != key for key, item in indexed.items()):
        raise ValueError("目录身份重复或不一致")
    _validate_semantics(resources, semantic_manifest)
    bindings = [RelationBinding.model_validate(item) for item in relation_bindings]
    binding_by_id = {item.native_id: item for item in bindings}
    if len(binding_by_id) != len(bindings):
        raise ValueError("物理绑定重复")
    metric_ids = {item["name"]: key for key, item in indexed.items() if item["kind"] == ResourceKind.METRIC}
    dataset_ids = {item["name"]: key for key, item in indexed.items() if item["kind"] == ResourceKind.SEMANTIC_MODEL}
    relations = set()
    ephemeral = native_catalog.get("ephemeralDependencies", {})

    def edge(upstream, downstream, kind, visited=frozenset()):
        # ephemeral 没有独立物理关系，展示血缘连接到其真实上游，仍检测循环与悬空引用。
        if upstream in ephemeral:
            if upstream in visited:
                raise ValueError("ephemeral 依赖存在环")
            for parent in ephemeral[upstream]:
                edge(parent, downstream, kind, visited | {upstream})
            return
        if upstream not in indexed or downstream not in indexed:
            raise ValueError("目录存在悬空引用")
        relations.add((upstream, downstream, kind))

    # 补齐 dbt 依赖，并只为已知语义类型生成平台展示关系。
    for item in native_catalog["dependencies"]:
        edge(item["upstreamResourceId"], item["downstreamResourceId"], item["kind"])
    for key, item in indexed.items():
        for upstream in item["definition"].get("depends_on", {}).get("nodes", []):
            edge(upstream, key, DEPENDENCY)

    computed, visiting = {}, set()

    def metric_attributes(key):
        if key in visiting:
            raise ValueError("指标引用存在环")
        if key in computed:
            return computed[key]
        visiting.add(key)
        definition = indexed[key]["definition"]
        metric_type = definition.get("type")
        if metric_type not in METRIC_TYPES:
            raise ValueError("不支持的指标类型")
        params = definition.get("type_params") or {}
        inputs = list(params.get("metrics") or [])
        inputs.extend(params[field] for field in ("numerator", "denominator") if params.get(field))
        cumulative = params.get("cumulative_type_params") or {}
        conversion = params.get("conversion_type_params") or {}
        if cumulative.get("metric"):
            inputs.append(cumulative["metric"])
        inputs.extend(conversion[field] for field in ("base_metric", "conversion_metric") if conversion.get(field))
        input_ids = []
        datasets = set()
        for entry in inputs:
            name = entry if isinstance(entry, str) else entry["name"]
            if name not in metric_ids:
                raise ValueError("指标引用不存在")
            parent = metric_ids[name]
            input_ids.append(parent)
            datasets.update(metric_attributes(parent)["datasetIds"])
            edge(parent, key, METRIC_INPUT)
        # 新版 aggregation 与旧版 input_measures 都映射为明确的数据集归属。
        aggregation = params.get("metric_aggregation_params") or {}
        if aggregation.get("semantic_model"):
            name = aggregation["semantic_model"]
            if name not in dataset_ids:
                raise ValueError("指标数据集不存在")
            datasets.add(dataset_ids[name])
        measures = list(params.get("input_measures") or [])
        measures.extend(params[field] for field in ("measure",) if params.get(field))
        measures.extend(conversion[field] for field in ("base_measure", "conversion_measure") if conversion.get(field))
        for measure in measures:
            name = measure if isinstance(measure, str) else measure["name"]
            owners = [dataset for dataset in dataset_ids.values() if any(
                item["name"] == name for item in indexed[dataset]["definition"].get("measures", [])
            )]
            if len(owners) != 1:
                raise ValueError("指标度量归属不存在或不唯一")
            datasets.update(owners)
        if not datasets:
            raise ValueError("指标缺少数据集归属")
        for dataset in datasets:
            edge(dataset, key, METRIC_DATASET)
        summary = params.get("expr") or f"{metric_type}: {definition['name']}"
        result = {"metricType": metric_type, "definitionSummary": summary,
                  "inputMetricIds": sorted(set(input_ids)), "datasetIds": sorted(datasets),
                  "filters": definition.get("filter"), "timeConfig": {
                      "timeGranularity": definition.get("time_granularity"),
                      "window": params.get("window"), "cumulative": cumulative, "conversion": conversion}}
        visiting.remove(key)
        computed[key] = result
        return result

    output = []
    for key, item in sorted(indexed.items()):
        definition, kind = item["definition"], ResourceKind(item["kind"])
        capabilities = []
        if kind == ResourceKind.METRIC:
            attributes = metric_attributes(key)
            capabilities = [QUERY] if item.get("queryable") else []
        elif kind == ResourceKind.DIMENSION:
            dataset = item.get("semanticModelId")
            if dataset not in dataset_ids.values():
                raise ValueError("维度缺少所属数据集")
            edge(dataset, key, MEMBERSHIP)
            attributes = {"datasetId": dataset, "dimensionType": definition["type"],
                          "expression": definition.get("expr"),
                          "timeGranularity": (definition.get("type_params") or {}).get("time_granularity")}
            capabilities = [DIMENSION_VALUES] if item.get("queryable") else []
        elif kind == ResourceKind.SEMANTIC_MODEL:
            models = [node for node in definition.get("depends_on", {}).get("nodes", []) if node in binding_by_id]
            if len(models) != 1:
                raise ValueError("语义模型缺少唯一物理来源")
            semantic = next(node for node in semantic_manifest["semantic_models"] if node["name"] == item["name"])
            relation = semantic.get("node_relation")
            binding = binding_by_id[models[0]].relation
            if relation and (relation.get("database"), relation.get("schema_name"), relation.get("alias")) != (
                    binding.database, binding.schema_name, binding.identifier):
                raise ValueError("语义模型物理关系与发布绑定不一致")
            edge(models[0], key, MODEL_DATASET)
            attributes = {"modelResourceId": models[0], "dimensionIds": sorted(
                node for node, value in indexed.items()
                if value["kind"] == ResourceKind.DIMENSION and value.get("semanticModelId") == key),
                "entities": definition.get("entities", []), "measures": definition.get("measures", []),
                "defaultTimeDimension": (definition.get("defaults") or {}).get("agg_time_dimension")}
        else:
            binding = binding_by_id.get(key)
            if binding is None:
                raise ValueError("物理资源缺少经过验证的绑定")
            attributes = {"origin": binding.mode, "relation": binding.relation, "columns": item.get("columns", {})}
            capabilities = [PREVIEW] if item.get("queryable") else []
        output.append({"resourceId": key, "nativeId": item["nativeId"], "kind": kind, "name": item["name"],
                       "displayName": definition.get("label") or item["name"],
                       "description": item.get("description", definition.get("description")),
                       "sourcePath": item.get("sourcePath"), "capabilities": capabilities,
                       "attributes": attributes, "nativeDetails": definition})

    # 检查整个依赖图，物理环也不能因指标递归校验已经通过而漏掉。
    incoming = {key: set() for key in indexed}
    for upstream, downstream, _ in relations:
        incoming[downstream].add(upstream)
    while incoming:
        ready = {key for key, parents in incoming.items() if not parents}
        if not ready:
            raise ValueError("目录依赖存在环")
        incoming = {key: parents - ready for key, parents in incoming.items() if key not in ready}
    return PublishedCatalog.model_validate({
        "projectId": project_id, "releaseId": release_id, "resources": output, "relationBindings": bindings,
        "relations": [{"upstreamResourceId": up, "downstreamResourceId": down, "kind": kind}
                      for up, down, kind in sorted(relations)],
    })


def write_publication_catalog(target: Path, *, project_id: str, release_id: UUID,
                              run_id: UUID, native_catalog: dict, reused_bindings: list | None = None) -> dict:
    """全构建的实际关系形成直接绑定，完整目录文件必须在 capture 之前写入。"""
    bindings = []
    reused = {item.native_id: item for item in (reused_bindings or [])}
    for item in native_catalog["resources"]:
        if item["kind"] not in (ResourceKind.TABLE, ResourceKind.VIEW):
            continue
        definition = item["definition"]
        if item["nativeId"] in reused:
            bindings.append(reused[item["nativeId"]])
            continue
        external = definition.get("resource_type") == SOURCE_TYPE
        bindings.append({
            "nativeId": item["nativeId"],
            "relation": {"database": definition.get("database"), "schema": definition.get("schema"),
                         "identifier": definition.get("identifier") if external else definition.get("alias")},
            "creatorRunId": None if external else run_id,
            "mode": BindingMode.EXTERNAL if external else BindingMode.BUILT,
            "definitionDigest": hashlib.sha256(json.dumps(definition, sort_keys=True).encode(UTF8)).hexdigest(),
            "verifiedAt": datetime.now(UTC),
        })
    catalog = build_published_catalog(
        project_id=project_id, release_id=release_id, native_catalog=native_catalog,
        semantic_manifest=json.loads((target / SEMANTIC_FILE).read_text(encoding=UTF8)), relation_bindings=bindings,
    )
    (target / PUBLISHED_CATALOG_FILE).write_text(catalog.model_dump_json(by_alias=True), encoding=UTF8)
    return catalog.model_dump(mode=JSON_MODE, by_alias=True)
