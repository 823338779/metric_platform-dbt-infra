"""外部执行快照不依赖配置注册，升级保留历史身份。"""

from uuid import uuid4

import pytest
from sqlalchemy import select

from dbt_metricflow_service.models.execution import ExecutionSnapshot, SnapshotBuildRequest
from dbt_metricflow_service.storage.builds import BuildStore
from dbt_metricflow_service.storage.entities import ExecutionBinding
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
    return BuildStore(store.db).accept_snapshot(body, "environment-tests", "test-toolchain", 600, "runtime-v1")


def test_snapshot_acceptance_does_not_read_execution_binding(store):
    body = request()
    row = accept(store, body)
    assert row["environment_id"] == str(body.execution.environment_id)
    assert row["protocol_version"] == 4
    assert row["execution_binding"] is None
    assert row["config_snapshot"] == body.execution.model_dump(mode="json", by_alias=True)
    job = JobStore(store.db).get(row["run_id"])
    assert job["config_version"] == "runtime-v1"
    assert row["config_version"] == "7"
    assert job["schema_name"] == body.execution.schema_name
    with store.db.session() as session:
        assert session.scalar(select(ExecutionBinding).where(ExecutionBinding.repository == body.repository)) is None


def test_snapshot_idempotency(store):
    body = request()
    first = accept(store, body)
    assert accept(store, body)["build_id"] == first["build_id"]
    changed = body.model_copy(update={"execution": body.execution.model_copy(update={"catalog": "other"})})
    with pytest.raises(StoreConflict):
        accept(store, changed)


def test_upgrade_from_actual_v3_schema_preserves_rows():
    from dbt_metricflow_service.storage.schema import SCHEMA_FILE, upgrade
    from tests.schema_helpers import isolated_database

    with isolated_database() as database:
        build_id, job_id = str(uuid4()), str(uuid4())
        with database.transaction() as connection:
            connection.exec_driver_sql(SCHEMA_FILE.read_text(encoding="utf-8"),
                                       execution_options={"no_parameters": True})
            connection.exec_driver_sql("INSERT INTO runtime_project(project_id) VALUES('legacy')")
            connection.exec_driver_sql(
                "INSERT INTO runtime_job(job_id,kind,project_id,request_fingerprint,config_version,"
                "toolchain_version,deadline_at) VALUES(%s,'BUILD_RUN','legacy','digest','1','old',clock_timestamp())",
                (job_id,),
            )
            connection.exec_driver_sql(
                "INSERT INTO engine_build(build_id,run_id,repository,environment,execution_binding,config_version,"
                "toolchain_version,caller,idempotency_key,request_digest,request_json,config_snapshot) "
                "VALUES(%s,%s,'legacy','PREVIEW','old','1','old','test','key','digest','{}','{}')", (build_id, job_id),
            )
            before = dict(connection.exec_driver_sql("SELECT * FROM engine_build").mappings().one())
        with pytest.raises(RuntimeError, match="upgrade-db"):
            database.check()
        with database.transaction() as connection:
            upgrade(connection)
            upgrade(connection)
            after = dict(connection.exec_driver_sql("SELECT * FROM engine_build").mappings().one())
        assert {key: after[key] for key in before} == before
        assert after["protocol_version"] == 3
        assert after["environment_id"] is None
        database.check()


def test_upgrade_preserves_existing_builds(store):
    from dbt_metricflow_service.storage.schema import upgrade
    from tests.test_v3_build_acceptance import request as old_request
    from tests.test_v3_build_acceptance import service

    old = service(store).submit(old_request(), "legacy-test")
    before = BuildStore(store.db).get(old.build_id)
    with store.db.transaction() as connection:
        upgrade(connection)
        upgrade(connection)
    assert BuildStore(store.db).get(old.build_id) == before


def test_environment_has_no_source_binding():
    body = request()
    assert body.branch_name == "main"
    with pytest.raises(ValueError):
        snapshot(repository="https://example.test/wrong.git")
    with pytest.raises(ValueError):
        request(repository="https://user:password@example.test/metrics.git")
