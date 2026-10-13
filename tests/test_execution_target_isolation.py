"""物理目标互斥覆盖多个 worker，未知外部结果不释放保护。"""

from uuid import uuid4

import pytest

from dbt_metricflow_service.storage.execution_targets import ExecutionTargetStore
from dbt_metricflow_service.storage.jobs import JobStore, StoreConflict
from tests.test_execution_snapshot_storage import accept, request, snapshot
from tests.test_publication_storage import store as store


def leased(store, body):
    build = accept(store, body)
    jobs = JobStore(store.db)
    return jobs.claim(str(uuid4()), toolchain_version="test-toolchain", config_version="runtime-v1",
                      job_id=build.run_id)


def test_two_workers_cannot_write_same_target(store):
    body = request()
    first = leased(store, body)
    second = leased(store, request(execution=body.execution))
    targets = ExecutionTargetStore(store.db)
    assert targets.claim(first.job_id, first.lease_token)
    assert not targets.claim(second.job_id, second.lease_token)
    JobStore(store.db).fail(first.job_id, first.lease_token, "TEST_FAILURE", stopped=True)
    targets.release(first.job_id, first.lease_token, stopped=True)
    assert targets.claim(second.job_id, second.lease_token)


def test_other_environment_cannot_claim_historical_target(store):
    original = snapshot()
    accept(store, request(execution=original))
    other = original.model_copy(update={"environment_id": uuid4()})
    with pytest.raises(StoreConflict, match="EXECUTION_TARGET_CONFLICT"):
        accept(store, request(execution=other))


def test_same_environment_version_is_immutable(store):
    original = snapshot()
    accept(store, request(execution=original))
    changed = original.model_copy(update={"schema_name": "changed"})
    with pytest.raises(StoreConflict, match="EXECUTION_SNAPSHOT_CONFLICT"):
        accept(store, request(execution=changed))


def test_unknown_outcome_keeps_target_blocked(store):
    body = request()
    first = leased(store, body)
    second = leased(store, request(execution=body.execution))
    targets = ExecutionTargetStore(store.db)
    assert targets.claim(first.job_id, first.lease_token)
    JobStore(store.db).fail(first.job_id, first.lease_token, "EXECUTION_OUTCOME_UNKNOWN", stopped=False)
    targets.release(first.job_id, first.lease_token, stopped=True)
    assert not targets.claim(second.job_id, second.lease_token)


def test_external_config_version_does_not_block_worker(store):
    job = leased(store, request(execution=snapshot(configVersion=99)))
    assert job is not None
    assert job.config_version == "runtime-v1"
