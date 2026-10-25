"""物理目标互斥覆盖多个 worker，未知外部结果不释放保护。"""

from types import SimpleNamespace
from uuid import uuid4

import pytest

from dbt_metricflow_service.storage.execution_targets import ExecutionTargetStore
from dbt_metricflow_service.storage.jobs import JobStore, StoreConflict
from tests.dbt_execution_helpers import invoke_job_for_test
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


@pytest.mark.parametrize("interrupted", [False, True])
async def test_worker_releases_target_after_completed_dbt_failure(store, tmp_path, monkeypatch, interrupted):
    from dbt.cli.main import dbtRunner

    from dbt_metricflow_service.runtime.worker import Worker
    from dbt_metricflow_service.storage.builds import BuildStore
    from dbt_metricflow_service.storage.entities import RuntimeAttempt, RuntimeJob
    from tests.dbt_execution_helpers import EngineCall

    body = request()
    first = leased(store, body)
    second = leased(store, request(execution=body.execution))
    jobs = JobStore(store.db)
    targets = ExecutionTargetStore(store.db)
    assert targets.claim(first.job_id, first.lease_token)
    assert not targets.claim(second.job_id, second.lease_token)
    worker = Worker(SimpleNamespace(
        db=store.db, jobs=jobs, artifacts=None,
        settings=SimpleNamespace(max_output_bytes=1024, heartbeat_seconds=15),
    ))
    monkeypatch.setattr(dbtRunner, "invoke", lambda *_: SimpleNamespace(
        success=False, exception=RuntimeError("interrupted") if interrupted else None,
    ))

    async def execute(job):
        await invoke_job_for_test(worker.executor, job, EngineCall(("dbt", "build"), tmp_path, {}, True), "BUILDING")

    monkeypatch.setattr(worker.executor, "execute", execute)
    await worker._execute(first)
    build = BuildStore(store.db).get(first.request_json.build_id)
    assert build.build_status == ("OUTCOME_UNKNOWN" if interrupted else "FAILED")
    assert build.error_code == ("EXECUTION_OUTCOME_UNKNOWN" if interrupted else "COMMAND_FAILED")
    with store.db.session() as session:
        attempt = session.get(RuntimeAttempt, first.attempt_id)
        assert attempt.state == ("EXPIRED_UNCONFIRMED" if interrupted else "FAILED")
        assert (attempt.stop_confirmed_at is None) is interrupted
        assert session.get(RuntimeJob, first.job_id).target_claimed is interrupted
    assert targets.claim(second.job_id, second.lease_token) is not interrupted


def test_external_config_version_does_not_block_worker(store):
    job = leased(store, request(execution=snapshot(configVersion=99)))
    assert job is not None
    assert job.config_version == "runtime-v1"
