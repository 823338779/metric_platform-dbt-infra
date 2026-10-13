"""外部配置版本不参与 worker 版本匹配，历史查询依赖固定快照。"""

from dbt_metricflow_service.application.builds import BuildService
from dbt_metricflow_service.storage.builds import BuildStore
from tests.test_execution_snapshot_storage import accept, request
from tests.test_publication_storage import store as store


def test_external_version_does_not_make_build_unavailable(store):
    row = accept(store, request())
    row.build_status = "SUCCEEDED"
    row.output_set_id = "artifact"
    row.run_lifecycle = "ACTIVE"
    app = BuildService(BuildStore(store.db), "test-toolchain", 600, "runtime-v1")
    assert app.query_unavailable_reason(row) is None
    row.toolchain_version = "incompatible"
    assert app.query_unavailable_reason(row) == "EXECUTION_ENVIRONMENT_UNAVAILABLE"
