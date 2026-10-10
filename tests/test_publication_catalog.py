"""发布前展示投影的身份、引用与失败边界。"""

from copy import deepcopy
from uuid import UUID

import pytest

from dbt_metricflow_service.platform.sealed_catalog import build_published_catalog, write_publication_catalog

PROJECT = "catalog-test"
RELEASE = UUID("00000000-0000-0000-0000-000000000001")


def package():
    # 两个模型中的同名维度必须分别保留；派生指标引用两个数据集。
    resources, bindings, semantic = [], [], {"semantic_models": [], "metrics": []}
    for name in ("orders", "returns"):
        model_id, dataset_id = f"model.sample.{name}", f"semantic_model.sample.{name}"
        model = {"name": name, "resource_type": "model"}
        dataset = {"name": name, "dimensions": [{"name": "region", "type": "categorical"}],
                   "entities": [], "measures": [], "depends_on": {"nodes": [model_id]}}
        metric = {"name": name, "label": f"展示 {name}", "type": "simple",
                  "type_params": {"metric_aggregation_params": {"semantic_model": name, "agg": "sum"}},
                  "depends_on": {"nodes": [dataset_id]}}
        for native_id, kind, definition in (
            (model_id, "TABLE", model), (dataset_id, "SEMANTIC_MODEL", dataset),
            (f"{dataset_id}.dimension.region", "DIMENSION", dataset["dimensions"][0]),
            (f"metric.sample.{name}", "METRIC", metric),
        ):
            resources.append({"resourceId": native_id, "nativeId": native_id, "kind": kind,
                              "name": definition["name"], "definition": definition,
                              "semanticModelId": dataset_id, "queryable": True, "columns": {}})
        bindings.append({"nativeId": model_id, "relation": {"database": "db", "schema": "s", "identifier": name},
                         "creatorRunId": str(RELEASE), "mode": "BUILT", "definitionDigest": "a" * 64,
                         "verifiedAt": "2026-09-30T00:00:00Z"})
        semantic["semantic_models"].append(deepcopy(dataset))
        semantic["metrics"].append(deepcopy(metric))
    derived = {"name": "net", "type": "derived", "type_params": {
        "expr": "orders - returns", "metrics": [{"name": "orders"}, {"name": "returns"}]}}
    resources.append({"resourceId": "metric.sample.net", "nativeId": "metric.sample.net", "kind": "METRIC",
                      "name": "net", "definition": derived, "queryable": True})
    semantic["metrics"].append(deepcopy(derived))
    return {"resources": resources, "dependencies": []}, semantic, bindings


def build(native=None, semantic=None, bindings=None):
    defaults = package()
    return build_published_catalog(project_id=PROJECT, release_id=RELEASE,
                                   native_catalog=defaults[0] if native is None else native,
                                   semantic_manifest=defaults[1] if semantic is None else semantic,
                                   relation_bindings=defaults[2] if bindings is None else bindings)


def test_typed_catalog_is_complete():
    result = build()
    data = result.model_dump(mode="json", by_alias=True)
    resources = {item["resourceId"]: item for item in data["resources"]}
    assert data["schemaVersion"] == 1
    assert data["releaseId"] == str(RELEASE)
    assert len([item for item in resources.values() if item["kind"] == "DIMENSION"]) == 2
    assert resources["metric.sample.orders"]["displayName"] == "展示 orders"
    net = resources["metric.sample.net"]["attributes"]
    assert net["inputMetricIds"] == ["metric.sample.orders", "metric.sample.returns"]
    assert net["datasetIds"] == ["semantic_model.sample.orders", "semantic_model.sample.returns"]
    assert "DATASET" not in {item["kind"] for item in resources.values()}
    assert type(result).model_validate_json(result.model_dump_json()) == result
    assert any(edge["upstreamResourceId"] == "metric.sample.orders" and
               edge["downstreamResourceId"] == "metric.sample.net" for edge in data["relations"])


@pytest.mark.parametrize("damage", ["duplicate", "missing-binding", "unknown-type", "dangling", "cycle", "mismatch"])
def test_invalid_catalog_cannot_publish(damage):
    native, semantic, bindings = package()
    if damage == "duplicate":
        native["resources"].append(native["resources"][0])
    elif damage == "missing-binding":
        bindings.pop()
    elif damage == "unknown-type":
        native["resources"][-1]["definition"]["type"] = "unknown"
        semantic["metrics"][-1]["type"] = "unknown"
    elif damage == "dangling":
        native["resources"][-1]["definition"]["type_params"]["metrics"] = [{"name": "missing"}]
        semantic["metrics"][-1] = deepcopy(native["resources"][-1]["definition"])
    elif damage == "cycle":
        native["resources"][-1]["definition"]["type_params"]["metrics"] = [{"name": "net"}]
        semantic["metrics"][-1] = deepcopy(native["resources"][-1]["definition"])
    else:
        semantic["semantic_models"][0]["dimensions"] = []
    with pytest.raises(ValueError):
        build(native, semantic, bindings)


def test_empty_catalog_requires_complete_native_structure():
    empty = build({"resources": [], "dependencies": []}, {"semantic_models": [], "metrics": []}, [])
    assert empty.resources == []
    with pytest.raises(ValueError):
        build({}, {}, [])


def test_write_catalog_requires_matching_physical_binding(tmp_path):
    import json

    native, semantic, _ = package()
    for resource in native["resources"]:
        if resource["kind"] == "TABLE":
            resource["definition"].update({"database": "db", "schema": "s", "alias": resource["name"]})
    (tmp_path / "semantic_manifest.json").write_text(json.dumps(semantic))
    result = write_publication_catalog(tmp_path, project_id=PROJECT, release_id=RELEASE, run_id=RELEASE,
                                       native_catalog=native)
    assert (tmp_path / "published_catalog.json").exists()
    assert result["projectId"] == PROJECT
    assert len(result["relationBindings"]) == 2
