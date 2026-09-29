"""真实 PostgreSQL 上验证队列竞争、租约 fencing 和清理引用保护。"""

import os
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest

from dbt_metricflow_service.storage.jobs import CleanupBlocked, JobStore, ProjectBusy, StoreConflict
from dbt_metricflow_service.storage.postgres import Database

DSN_ENV = "SERVICE_TEST_DATABASE_URL"
PROJECT_PREFIX = "storage-test-"
DBT = "DBT_COMMAND"
BUILD = "BUILD_RUN"
QUERY = "METRIC_QUERY"
VERSION = "storage-test"


@pytest.fixture
def store():
    # 独立测试数据库由环境注入；随机项目避免不同测试进程互相清表。
    dsn = os.getenv(DSN_ENV)
    if not dsn:
        pytest.skip("SERVICE_TEST_DATABASE_URL 未设置")
    db = Database(dsn)
    db.migrate()
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
    return store.reserve(DBT, name, {}, toolchain_version=VERSION, **kwargs)


def claim(store):
    return store.claim(str(uuid4()), toolchain_version=VERSION)


def expire(store, job):
    with store.db.transaction() as cursor:
        cursor.execute(
            "UPDATE runtime_attempt SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE attempt_id=%s",
            (job["attempt_id"],),
        )


def test_concurrent_idempotency_and_claim(store):
    name = project(store)
    key = str(uuid4())
    with ThreadPoolExecutor(max_workers=8) as pool:
        rows = list(
            pool.map(
                lambda _: reserve(store, name, fingerprint="one", idempotency_scope=DBT, idempotency_key=key), range(16)
            )
        )
    assert len({row["job_id"] for row in rows}) == 1
    with pytest.raises(StoreConflict):
        reserve(store, name, fingerprint="two", idempotency_scope=DBT, idempotency_key=key)
    with ThreadPoolExecutor(max_workers=8) as pool:
        claims = list(pool.map(lambda _: claim(store), range(8)))
    assert sum(row is not None for row in claims) == 1


def test_expired_write_cannot_renew_or_publish_or_release_lock(store):
    name = project(store)
    row = reserve(store, name, write=True)
    job = claim(store)
    assert store.phase(job["job_id"], job["lease_token"], "BUILDING", external=True)
    expire(store, job)
    assert not store.heartbeat(job["job_id"], job["lease_token"])
    assert not store.finish(job["job_id"], job["lease_token"], {"wrong": True})
    store.recover()
    assert store.get(row["job_id"])["error_code"] == "EXECUTION_OUTCOME_UNKNOWN"
    with pytest.raises(ProjectBusy):
        reserve(store, name, write=True)
    assert store.confirm_stopped(job["attempt_id"], job["lease_token"])
    assert reserve(store, name, write=True)["status"] == "QUEUED"


def test_queued_volatile_input_expires_without_claim(store):
    name = project(store)
    row = reserve(store, name, input_mode="VOLATILE", pinned_instance_id=str(uuid4()), write=True)
    with store.db.transaction() as cursor:
        cursor.execute(
            "UPDATE runtime_job SET input_lease_expires_at=clock_timestamp()-interval '1 second' WHERE job_id=%s",
            (row["job_id"],),
        )
    store.recover()
    assert store.get(row["job_id"])["error_code"] == "INPUT_LOST"
    assert store.project(name)["busy_job_id"] is None


def test_readonly_retry_retains_unconfirmed_attempt_and_cleanup_gate(store):
    name = project(store)
    parent = store.reserve(BUILD, name, {}, toolchain_version=VERSION)
    run = claim(store)
    # 引用保护测试直接提供已发布父任务；发布证据在单独测试验证。
    with store.db.transaction() as cursor:
        cursor.execute("UPDATE runtime_job SET status='SUCCEEDED' WHERE job_id=%s", (run["job_id"],))
    # 测试夹具只提供空封存集合；产物完整性由独立产物测试验证。
    set_id = str(uuid4())
    with store.db.transaction() as cursor:
        cursor.execute(
            "INSERT INTO runtime_artifact_set(set_id,project_id,kind,state) VALUES(%s,%s,'EXECUTION','SEALED')",
            (set_id, name),
        )
        cursor.execute("UPDATE runtime_job SET output_set_id=%s WHERE job_id=%s", (set_id, parent["job_id"]))
    query = store.reserve(
        QUERY, name, {}, parent_run_id=parent["job_id"], retry_policy="READ_ONLY", toolchain_version=VERSION
    )
    with pytest.raises(CleanupBlocked):
        store.reserve_cleanup(parent["job_id"])
    attempt = claim(store)
    store.phase(attempt["job_id"], attempt["lease_token"], "BUILDING", external=True)
    expire(store, attempt)
    store.recover()
    assert store.get(query["job_id"])["status"] == "QUEUED"
    with store.db.transaction() as cursor:
        cursor.execute("UPDATE runtime_job SET available_at=clock_timestamp() WHERE job_id=%s", (query["job_id"],))
    next_attempt = claim(store)
    assert store.finish(next_attempt["job_id"], next_attempt["lease_token"], {"rows": [["123456789.00001"]]})
    with pytest.raises(CleanupBlocked):
        store.reserve_cleanup(parent["job_id"])
    store.confirm_stopped(attempt["attempt_id"], attempt["lease_token"])
    cleanup = store.reserve_cleanup(parent["job_id"])
    assert cleanup["kind"] == "RUN_CLEANUP"
    assert store.result(query["job_id"])["payload_json"]["rows"] == [["123456789.00001"]]


def test_build_without_output_cannot_be_published(store):
    name = project(store)
    row = store.reserve(BUILD, name, {}, toolchain_version=VERSION)
    attempt = claim(store)
    with pytest.raises(ValueError, match="output"):
        store.finish(row["job_id"], attempt["lease_token"])
    assert store.get(row["job_id"])["status"] == "RUNNING"
    assert store.result(row["job_id"]) is None


def test_deadline_expires_queued_job_and_inputs_cannot_revive(store):
    name = project(store)
    worker = str(uuid4())
    row = reserve(store, name, input_mode="VOLATILE", pinned_instance_id=worker, timeout_seconds=-1)
    assert store.heartbeat_inputs(worker, [row["job_id"]]) == 0
    assert store.claim(worker, toolchain_version=VERSION) is None
    store.recover()
    assert store.get(row["job_id"])["error_code"] == "TASK_TIMEOUT"


def test_renew_only_inputs_present_in_memory(store):
    name = project(store)
    worker = str(uuid4())
    first = reserve(store, name, input_mode="VOLATILE", pinned_instance_id=worker)
    reserve(store, name, input_mode="VOLATILE", pinned_instance_id=worker)
    assert store.heartbeat_inputs(worker, [first["job_id"]]) == 1
    assert store.heartbeat_inputs(worker, []) == 0


def test_finish_rechecks_lease_after_artifact_sealing(store, tmp_path):
    from dbt_metricflow_service.storage.artifacts import ArtifactStore

    name = project(store)
    row = reserve(store, name)
    attempt = claim(store)
    (tmp_path / "dbt_project.yml").write_text("name: lease_test\n", encoding="utf-8")
    artifacts = ArtifactStore(store.db)
    output = artifacts.capture(name, tmp_path, producer_attempt_id=attempt["attempt_id"], kind="EXECUTION")

    def seal_then_expire(set_id, cursor):
        artifacts.seal(set_id, cursor)
        cursor.execute(
            "UPDATE runtime_attempt SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE attempt_id=%s",
            (attempt["attempt_id"],),
        )

    assert not store.finish(row["job_id"], attempt["lease_token"], output_set_id=output, seal=seal_then_expire)
    assert store.get(row["job_id"])["status"] == "RUNNING"
    assert store.result(row["job_id"]) is None
    assert artifacts.metadata(output)["state"] == "STAGING"


def test_cleanup_uses_parent_versions_and_can_clean_failed_build(store):
    name = project(store)
    parent = store.reserve(BUILD, name, {}, toolchain_version=VERSION, schema_name="run_schema")
    attempt = claim(store)
    assert store.fail(parent["job_id"], attempt["lease_token"], "BUILD_FAILED")
    cleanup = store.reserve_cleanup(parent["job_id"])
    assert cleanup["toolchain_version"] == VERSION
    assert claim(store)["job_id"] == cleanup["job_id"]


def test_sealed_set_cannot_be_demoted_or_rewritten(store, tmp_path):
    from psycopg2 import IntegrityError

    from dbt_metricflow_service.storage.artifacts import ArtifactStore

    name = project(store)
    (tmp_path / "model.sql").write_text("select 1", encoding="utf-8")
    (tmp_path / "dbt_project.yml").write_text("name: immutable_test\n", encoding="utf-8")
    set_id = ArtifactStore(store.db).capture(name, tmp_path)
    for change in ("state='STAGING'", "metadata='{\"changed\":true}'::jsonb"):
        with pytest.raises(IntegrityError), store.db.transaction() as cursor:
            cursor.execute("UPDATE runtime_artifact_set SET " + change + " WHERE set_id=%s", (set_id,))


def test_idempotency_cannot_reuse_another_project(store):
    first, second = project(store), project(store)
    key = str(uuid4())
    reserve(store, first, idempotency_scope=DBT, idempotency_key=key, fingerprint="same-body")
    with pytest.raises(StoreConflict):
        reserve(store, second, idempotency_scope=DBT, idempotency_key=key, fingerprint="same-body")


def test_cli_admission_rejects_project_import_after_manifest_validation(store):
    name = project(store)
    checked = store.project(name)
    store.register_project(name, {"binding": "updated"})
    with pytest.raises(StoreConflict, match="changed"):
        reserve(store, name, expected_revision=checked["revision"])
    assert store.project(name)["busy_job_id"] is None


def test_options_retry_is_bounded_and_preserves_deadline(store):
    name = project(store)
    row = store.reserve("QUERY_OPTIONS", name, {}, toolchain_version=VERSION)
    deadline = row["deadline_at"]
    for attempt_number in range(1, 4):
        attempt = claim(store)
        assert attempt["attempt_no"] == attempt_number
        store.fail(row["job_id"], attempt["lease_token"], "TRANSIENT_FAILURE")
        assert store.requeue_options(row["job_id"]) is (attempt_number < 3)
        assert store.get(row["job_id"])["deadline_at"] == deadline


def test_options_retry_rejects_unconfirmed_external_execution(store):
    name = project(store)
    row = store.reserve("QUERY_OPTIONS", name, {}, toolchain_version=VERSION)
    attempt = claim(store)
    store.phase(row["job_id"], attempt["lease_token"], "VALIDATING", external=True)
    store.fail(row["job_id"], attempt["lease_token"], "EXECUTION_OUTCOME_UNKNOWN", stopped=False)
    assert not store.requeue_options(row["job_id"])


def test_options_retry_rejects_elapsed_deadline(store):
    name = project(store)
    row = store.reserve("QUERY_OPTIONS", name, {}, toolchain_version=VERSION)
    attempt = claim(store)
    store.fail(row["job_id"], attempt["lease_token"], "TRANSIENT_FAILURE")
    with store.db.transaction() as cursor:
        cursor.execute("UPDATE runtime_job SET deadline_at=clock_timestamp()-interval '1 second' WHERE job_id=%s",
                       (row["job_id"],))
    assert not store.requeue_options(row["job_id"])


def test_cleanup_releases_artifact_references_but_preserves_tombstone(store, tmp_path):
    from dbt_metricflow_service.storage.artifacts import ArtifactStore
    name = project(store)
    artifacts = ArtifactStore(store.db)
    (tmp_path / "dbt_project.yml").write_text("name: cleanup\n", encoding="utf-8")
    source = artifacts.capture(name, tmp_path)
    parent = store.reserve(BUILD, name, {}, toolchain_version=VERSION)
    attempt = claim(store)
    store.attach_input(parent["job_id"], attempt["lease_token"], source)
    store.fail(parent["job_id"], attempt["lease_token"], "PREPARATION_FAILED")
    cleanup = store.reserve_cleanup(parent["job_id"])
    execution = claim(store)
    assert execution["job_id"] == cleanup["job_id"]
    assert store.finish(execution["job_id"], execution["lease_token"], {"cleaned": True})
    assert store.get(parent["job_id"])["run_lifecycle"] == "CLEANED"
    assert artifacts.delete_unreferenced(source)


def test_missing_source_cannot_report_clean_after_known_external_execution(store):
    name = project(store)
    parent = store.reserve(BUILD, name, {}, toolchain_version=VERSION)
    attempt = claim(store)
    store.phase(parent["job_id"], attempt["lease_token"], "BUILDING", external=True)
    store.fail(parent["job_id"], attempt["lease_token"], "IMPORTED_FAILED", stopped=True)
    with pytest.raises(CleanupBlocked):
        store.reserve_cleanup(parent["job_id"])
