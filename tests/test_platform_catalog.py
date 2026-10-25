"""目录保留原生 SDK 对物化方式、声明字段和源码路径的解释。"""

import pytest
from dbt.artifacts.schemas.manifest import WritableManifest

from tests.test_publication_catalog import build, package


@pytest.mark.parametrize("materialized", ["table", "incremental", "ephemeral"])
def test_native_nodes_are_serialized_by_dbt(materialized):
    manifest = package()
    node = manifest["nodes"]["model.sample.orders"]
    node["config"]["materialized"] = materialized
    node["original_file_path"] = "models\\orders.sql"
    assert build(manifest) == WritableManifest.from_dict(manifest).to_dict(
        omit_none=False, context={"artifact": True})
