"""原生 Manifest 字典，供查询映射测试使用。"""


def resource(resource_id, **changes):
    return {"unique_id": resource_id, "name": resource_id.split(".")[-1],
            "resource_type": "metric" if resource_id.startswith("metric.") else "model",
            "config": {"materialized": "table"}, **changes}


def catalog(resources=()):
    return {
        "metrics": {item["unique_id"]: item for item in resources if item["resource_type"] == "metric"},
        "nodes": {item["unique_id"]: item for item in resources if item["resource_type"] != "metric"},
    }
