"""真实 PostgreSQL 上验证队列竞争、租约 fencing 和清理引用保护。"""

import os
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest

from dbt_metricflow_service.storage.jobs import JobStore, ProjectBusy, StoreConflict
from dbt_metricflow_service.storage.postgres import Database
from dbt_metricflow_service.storage.records import ArtifactMetadata, JobRequest
from tests.test_execution_snapshot_storage import request, snapshot

DSN_ENV = "SERVICE_TEST_DATABASE_URL"
PROJECT_PREFIX = "storage-test-"
BUILD = "BUILD_RUN"
VERSION = "storage-test"


@pytest.fixture
def store():
    # 独立测试数据库由环境注入；随机项目避免不同测试进程互相清表。
    dsn = os.getenv(DSN_ENV)
    if not dsn:
        pytest.skip("SERVICE_TEST_DATABASE_URL 未设置")
    db = Database(dsn)
    db.initialize()
    db.check()
    jobs = JobStore(db)
    global VERSION
    VERSION = str(uuid4())
    jobs.register_project(PROJECT_PREFIX + str(uuid4()))
    yield jobs
    db.close()


def project(store):
    name = PROJECT_PREFIX + str(uuid4())
    store.register_project(name)
    return name


def reserve(store, name, **kwargs):
    return store.reserve(name, JobRequest(), toolchain_version=VERSION, **kwargs)


def claim(store):
    return store.claim(str(uuid4()), toolchain_version=VERSION)


def expire(store, job):
    with store.db.transaction() as connection:
        connection.exec_driver_sql(
            "UPDATE runtime_attempt SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE attempt_id=%s",
            (job.attempt_id,),
        )


def test_concurrent_idempotency_and_claim(store):
    name = project(store)
    key = str(uuid4())
    with ThreadPoolExecutor(max_workers=8) as pool:
        rows = list(
            pool.map(
                lambda _: reserve(store, name, fingerprint="one", idempotency_scope=BUILD, idempotency_key=key),
                range(16),
            )
        )
    assert len({row.job_id for row in rows}) == 1
    with pytest.raises(StoreConflict):
        reserve(store, name, fingerprint="two", idempotency_scope=BUILD, idempotency_key=key)
    with ThreadPoolExecutor(max_workers=8) as pool:
        claims = list(pool.map(lambda _: claim(store), range(8)))
    assert sum(row is not None for row in claims) == 1


def test_expired_write_cannot_renew_or_publish_or_release_lock(store):
    name = project(store)
    row = reserve(store, name, write=True)
    job = claim(store)
    assert store.phase(job.job_id, job.lease_token, "BUILDING", external=True)
    expire(store, job)
    assert not store.heartbeat(job.job_id, job.lease_token)
    assert not store.finish(job.job_id, job.lease_token)
    store.recover()
    assert store.get(row.job_id).error_code == "EXECUTION_OUTCOME_UNKNOWN"
    with pytest.raises(ProjectBusy):
        reserve(store, name, write=True)
    assert store.confirm_stopped(job.attempt_id, job.lease_token)
    assert reserve(store, name, write=True).status == "QUEUED"



def test_preparation_retry_keeps_stale_attempt_fenced(store):
    name = project(store)
    row = reserve(store, name)
    first = claim(store)
    expire(store, first)
    store.recover()
    assert store.get(row.job_id).status == "QUEUED"
    with store.db.transaction() as connection:
        connection.exec_driver_sql(
            "UPDATE runtime_job SET available_at=clock_timestamp() WHERE job_id=%s", (row.job_id,)
        )
    second = claim(store)
    assert second.attempt_id != first.attempt_id
    assert not store.finish(row.job_id, first.lease_token, succeeded=False)
    assert store.finish(row.job_id, second.lease_token, succeeded=False, error_code="BUILD_FAILED")
    assert store.get(row.job_id).error_code == "BUILD_FAILED"




def test_build_without_output_cannot_be_published(store):
    name = project(store)
    row = store.reserve(name, JobRequest(), toolchain_version=VERSION)
    attempt = claim(store)
    with pytest.raises(ValueError, match="output"):
        store.finish(row.job_id, attempt.lease_token)
    assert store.get(row.job_id).status == "RUNNING"


def test_recovery_preserves_healthy_build(store):
    name = project(store)
    worker = str(uuid4())
    row = reserve(store, name)
    assert store.recover() == 0
    attempt = store.claim(worker, toolchain_version=VERSION)
    assert attempt.job_id == row.job_id
    assert store.phase(row.job_id, attempt.lease_token, "BUILDING", external=True)
    assert store.recover() == 0
    assert store.finish(row.job_id, attempt.lease_token, succeeded=False, error_code="BUILD_FAILED")
    assert store.get(row.job_id).error_code == "BUILD_FAILED"



@pytest.mark.parametrize("failure", ["lease_lost", "seal_error", "wrong_attempt", "missing_manifest"])
def test_finish_rolls_back_artifacts_and_job_on_failure(store, tmp_path, failure):
    from dbt_metricflow_service.storage.artifacts import ArtifactStore

    name = project(store)
    row = reserve(store, name)
    from dbt_metricflow_service.storage.entities import ArtifactSet, Build
    build_id = str(uuid4())
    from dbt_metricflow_service.models.execution import physical_target_key
    execution = snapshot()
    with store.db.fact_session() as session:
        session.add(Build(
            build_id=build_id, run_id=row.job_id, repository="https://example.test/project.git",
            environment=str(execution.environment_id), environment_id=str(execution.environment_id),
            physical_target_key=physical_target_key(execution), config_version="1",
            toolchain_version=VERSION, caller="test", idempotency_key=str(uuid4()),
            request_digest="test", request_json=request(execution=execution).model_dump(mode="json", by_alias=True),
            config_snapshot=execution.model_dump(mode="json", by_alias=True),
        ))
    attempt = claim(store)
    (tmp_path / "dbt_project.yml").write_text("name: lease_test\n", encoding="utf-8")
    artifacts = ArtifactStore(store.db)
    target = tmp_path / "target"
    target.mkdir()
    for filename in ("manifest.json", "semantic_manifest.json", "catalog.json"):
        (target / filename).write_text("{}", encoding="utf-8")
    if failure == "missing_manifest":
        (target / "manifest.json").unlink()
    output = artifacts.capture(
        name, tmp_path, producer_attempt_id=attempt.attempt_id, kind="EXECUTION",
        metadata=ArtifactMetadata(),
    )

    def broken_seal(set_id, connection):
        artifacts.seal(set_id, connection)
        if failure == "seal_error":
            raise ValueError("injected after sealing")
        connection.connection().exec_driver_sql(
            "UPDATE runtime_attempt SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE attempt_id=%s",
            (attempt.attempt_id,),
        )

    if failure == "lease_lost":
        assert not store.finish(row.job_id, attempt.lease_token, output_set_id=output, seal=broken_seal)
    elif failure == "seal_error":
        with pytest.raises(ValueError, match="injected"):
            store.finish(row.job_id, attempt.lease_token, output_set_id=output, seal=broken_seal)
    elif failure == "missing_manifest":
        with pytest.raises(ValueError, match="missing required native artifacts"):
            store.finish(row.job_id, attempt.lease_token, output_set_id=output)
    else:
        reserve(store, name)
        other = claim(store)
        with pytest.raises(ValueError, match="authorized attempt"):
            store.finish(other.job_id, other.lease_token, output_set_id=output)
        assert store.get(other.job_id).output_set_id is None
    assert store.get(row.job_id).status == "RUNNING"
    assert store.get(row.job_id).output_set_id is None
    with store.db.session() as session:
        assert session.get(ArtifactSet, output).state == "STAGING"
        build = session.get(Build, build_id)
        assert build.build_status == "RUNNING"
        assert build.output_set_id is None




def test_sealed_set_cannot_be_demoted_or_rewritten(store, tmp_path):
    from sqlalchemy.exc import IntegrityError

    from dbt_metricflow_service.storage.artifacts import ArtifactStore

    name = project(store)
    (tmp_path / "model.sql").write_text("select 1", encoding="utf-8")
    (tmp_path / "dbt_project.yml").write_text("name: immutable_test\n", encoding="utf-8")
    set_id = ArtifactStore(store.db).capture(name, tmp_path)
    for change in ("state='STAGING'", "project_digest='changed'"):
        with pytest.raises(IntegrityError), store.db.transaction() as connection:
            connection.exec_driver_sql("UPDATE runtime_artifact_set SET " + change + " WHERE set_id=%s", (set_id,))


def test_idempotency_cannot_reuse_another_project(store):
    first, second = project(store), project(store)
    key = str(uuid4())
    reserve(store, first, idempotency_scope=BUILD, idempotency_key=key, fingerprint="same-body")
    with pytest.raises(StoreConflict):
        reserve(store, second, idempotency_scope=BUILD, idempotency_key=key, fingerprint="same-body")


def test_build_admission_rejects_project_import_after_input_validation(store):
    name = project(store)
    checked = store.project(name)
    store.register_project(name, snapshot())
    with pytest.raises(StoreConflict, match="changed"):
        reserve(store, name, expected_revision=checked.revision)
    assert store.project(name).busy_job_id is None
