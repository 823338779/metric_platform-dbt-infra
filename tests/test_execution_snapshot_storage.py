"""外部执行快照不依赖配置注册，升级保留历史身份。"""

from uuid import uuid4

import pytest
from sqlalchemy import inspect

from dbt_metricflow_service.models.execution import ExecutionSnapshot, SnapshotBuildRequest
from dbt_metricflow_service.storage.builds import BuildStore
from dbt_metricflow_service.storage.jobs import JobStore, StoreConflict
from tests.test_publication_storage import store as store


def snapshot(**changes):
    return ExecutionSnapshot.model_validate({
        "environmentId": str(uuid4()), "configVersion": 7,
        "connection": {"type": "starrocks", "instanceId": "warehouse", "host": "localhost", "port": 9030},
        "credentialRef": "warehouse-test", "catalog": "default_catalog", "schema": "env_" + uuid4().hex,
        "threads": 2, "businessTimezone": "Asia/Shanghai", **changes,
    })


def request(**changes):
    return SnapshotBuildRequest.model_validate({
        "repository": "https://example.test/metrics.git", "branchName": "main",
        "execution": snapshot(), "idempotencyKey": uuid4().hex, **changes,
    })


def accept(store, body):
    return BuildStore(store.db).accept_snapshot(body, "environment-tests", "test-toolchain", "runtime-v1")


def test_snapshot_acceptance_does_not_read_execution_binding(store):
    body = request()
    row = accept(store, body)
    assert row.environment_id == str(body.execution.environment_id)
    assert "protocol_version" not in type(row).model_fields
    assert "execution_binding" not in type(row).model_fields
    assert row.config_snapshot == body.execution
    job = JobStore(store.db).get(row.run_id)
    assert job.config_version == "runtime-v1"
    assert row.config_version == "7"
    assert job.schema_name == body.execution.schema_name
    assert "engine_execution_binding" not in inspect(store.db.engine).get_table_names()


def test_snapshot_idempotency(store):
    body = request()
    first = accept(store, body)
    assert accept(store, body).build_id == first.build_id
    changed = body.model_copy(update={"execution": body.execution.model_copy(update={"catalog": "other"})})
    with pytest.raises(StoreConflict):
        accept(store, changed)


def test_initialize_creates_only_snapshot_schema():
    from tests.schema_helpers import isolated_database

    with isolated_database() as database:
        database.initialize()
        database.check()
        with database.transaction() as connection:
            columns = {column["name"] for column in inspect(connection).get_columns("engine_build")}
            assert {"environment_id", "physical_target_key", "config_snapshot"} <= columns
            assert not {"execution_binding", "protocol_version"} & columns
            assert "engine_execution_binding" not in inspect(connection).get_table_names()


def test_repeated_initialize_preserves_new_builds(store):
    row = accept(store, request())
    before = BuildStore(store.db).get(row.build_id)
    store.db.initialize()
    assert BuildStore(store.db).get(row.build_id) == before


def test_environment_has_no_source_binding():
    body = request()
    assert body.branch_name == "main"
    with pytest.raises(ValueError):
        snapshot(repository="https://example.test/wrong.git")
    with pytest.raises(ValueError):
        request(repository="https://user:password@example.test/metrics.git")
