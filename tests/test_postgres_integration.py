from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from uuid import uuid4

import pytest
from sqlalchemy import func, select

from dbt_metricflow_service.application.builds import BuildService
from dbt_metricflow_service.application.errors import ServiceError
from dbt_metricflow_service.models.builds import DeploymentPolicy
from dbt_metricflow_service.models.deployments import DeploymentKey
from dbt_metricflow_service.storage.artifacts import ArtifactStore
from dbt_metricflow_service.storage.builds import BuildStore
from dbt_metricflow_service.storage.deployments import DeploymentStore
from dbt_metricflow_service.storage.entities import Build, RuntimeAttempt
from dbt_metricflow_service.storage.jobs import JobStore
from dbt_metricflow_service.storage.schema import SCHEMA_FILE


def test_detach_runtime_project_preserves_existing_builds(database, settings, build_request):
    builds = BuildStore(database)
    service = BuildService(builds, "test-toolchain", settings)
    original = service.submit(build_request, "service")
    with database.transaction() as connection:
        connection.exec_driver_sql("""
            CREATE TABLE runtime_job AS SELECT build_id AS job_id, project_id FROM engine_build;
            ALTER TABLE runtime_job ADD PRIMARY KEY (job_id);
            CREATE TABLE runtime_project (project_id text PRIMARY KEY,
                busy_job_id uuid CONSTRAINT runtime_project_busy_fk REFERENCES runtime_job,
                source_set_id uuid CONSTRAINT runtime_project_source_fk REFERENCES runtime_artifact_set);
            INSERT INTO runtime_project (project_id, busy_job_id) SELECT project_id, job_id FROM runtime_job;
            ALTER TABLE runtime_job ADD CONSTRAINT runtime_job_project_id_fkey
                FOREIGN KEY (project_id) REFERENCES runtime_project;
            ALTER TABLE runtime_artifact_set ADD CONSTRAINT runtime_artifact_set_project_id_fkey
                FOREIGN KEY (project_id) REFERENCES runtime_project;
        """)
    with pytest.raises(RuntimeError, match="detach_runtime_project"):
        database.check()
    with database.transaction() as connection:
        connection.exec_driver_sql(SCHEMA_FILE.with_name("detach_runtime_project.sql").read_text(encoding="utf-8"))
        connection.exec_driver_sql("DROP TABLE runtime_job")
    database.check()
    assert service.submit(build_request, "service").build_id == original.build_id
    new = service.submit(build_request.model_copy(update={"idempotency_key": uuid4()}), "service")
    assert new.build_id != original.build_id
    with database.transaction() as connection:
        assert connection.exec_driver_sql("SELECT busy_job_id FROM runtime_project").scalars().all() == [
            str(original.build_id)
        ]


def test_transactional_idempotency_after_config_removal(database, settings, build_request):
    store = BuildStore(database)
    service = BuildService(store, "test-toolchain", settings)
    other = build_request.model_copy(update={"repository": "https://other.invalid/repo", "commit_sha": "c" * 40})
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(service.submit, build_request, "service")
        second = pool.submit(service.submit, other, "another-caller")
        ids = [first.result().build_id, second.result().build_id]
    assert ids[0] == ids[1]
    revoked = BuildService(store, "different-toolchain", replace(settings, execution_instances={}))
    assert revoked.submit(build_request, "service").build_id == ids[0]
    assert revoked.submit(other, "another-caller").build_id == ids[0]
    new_request = build_request.model_copy(update={"idempotency_key": uuid4()})
    with pytest.raises(ServiceError) as invalid:
        revoked.submit(new_request, "service")
    assert invalid.value.error.code == "EXECUTION_TARGET_UNVERIFIED"
    with database.session() as session:
        assert session.scalar(select(func.count()).select_from(Build)) == 1
    assert service.submit(new_request, "service").build_id != ids[0]


def test_two_environments_can_enqueue_builds_for_the_same_schema(database, settings, build_request):
    service = BuildService(BuildStore(database), "test-toolchain", settings)
    first = service.submit(build_request, "service")
    other = build_request.model_copy(update={
        "idempotency_key": uuid4(),
        "execution": build_request.execution.model_copy(update={"environment_id": uuid4()}),
    })
    second = service.submit(other, "service")
    assert first.build_id != second.build_id
    with database.session() as session:
        assert session.scalar(select(func.count()).select_from(Build)) == 2


def test_expired_preparation_requeues_same_build_and_fences_old_lease(database, settings, build_request):
    builds = BuildStore(database)
    view = BuildService(builds, "test", settings).submit(build_request, "caller")
    queue = JobStore(database)
    first = queue.claim(uuid4(), toolchain_version="test")
    assert first.build_id == str(view.build_id)
    with database.session() as session:
        session.get(RuntimeAttempt, first.attempt_id).lease_expires_at = func.clock_timestamp()
    assert queue.recover() == 1
    assert builds.get(view.build_id).build_status == "QUEUED"
    with database.session() as session:
        session.get(Build, first.build_id).available_at = func.clock_timestamp()
    second = queue.claim(uuid4(), toolchain_version="test")
    assert second.build_id == first.build_id and second.attempt_id != first.attempt_id
    assert not queue.fail(first.build_id, first.lease_token, "CANCELLED")
    assert queue.fail(second.build_id, second.lease_token, "CANCELLED")
    assert builds.get(view.build_id).build_status == "CANCELLED"


def test_builds_in_one_environment_keep_independent_snapshots(database, settings, build_request):
    store = BuildStore(database)
    service = BuildService(store, "test-toolchain", settings)
    first = service.submit(build_request, "service")
    request = build_request.model_copy(update={
        "idempotency_key": uuid4(), "execution": build_request.execution.model_copy(update={"threads": 2}),
    })
    second = service.submit(request, "service")
    for view in (first, second):
        build = store.get(view.build_id)
        task = JobStore(database).claim(uuid4(), build_id=build.build_id, toolchain_version="test-toolchain")
        assert task.execution == view.execution
        assert task.build_id == build.build_id
    assert first.execution.threads != second.execution.threads


@pytest.mark.parametrize("old_head,head,state,status,reason", [
    ("b" * 40, "c" * 40, "UNDEPLOYED", "DEPLOYED", None),
    ("b" * 40, None, "UNKNOWN", "DEPLOYED", None),
    ("b" * 40, None, "MISSING", "DEPLOYED", None),
    ("c" * 40, "b" * 40, "UNDEPLOYED", "STALE", "SOURCE_HEAD_CHANGED"),
    ("c" * 40, None, "UNKNOWN", "STALE", "SOURCE_HEAD_CHANGED"),
])
def test_postgres_observation_interleaving(database, settings, build_request, tmp_path,
                                         old_head, head, state, status, reason):
    request = build_request.model_copy(update={"deployment_policy": DeploymentPolicy.ON_SUCCESS})
    builds = BuildStore(database)
    view = BuildService(builds, "test-toolchain", settings).submit(request, "service")
    build = builds.get(view.build_id)
    source = tmp_path / "source"
    source.mkdir()
    (source / "dbt_project.yml").write_text("name: test\nversion: '1'\n", encoding="utf-8")
    output = ArtifactStore(database).capture(build.project_id, source)
    # Seed a valid referenced artifact and completed build; SDK/finish behavior is covered separately.
    with database.fact_session() as session:
        row = session.get(Build, build.build_id)
        row.build_status = "SUCCEEDED"
        row.output_set_id = output
    store = DeploymentStore(database)
    key = DeploymentKey(repository=request.repository, environment=str(request.execution.environment_id),
                        branch_name=request.branch_name)
    initial = store.current(key)
    old = store.observe(key, old_head, "UNDEPLOYED", expected_version=initial.version,
                        expected_active_build_id=None)
    with ThreadPoolExecutor(max_workers=1) as pool:
        newer = pool.submit(store.observe, key, head, state, expected_version=old.version,
                            expected_active_build_id=None).result()
    result = store.settle(key, 1, status, reason, expected_version=old.version, head=old_head)
    current = store.current(key)
    assert current.version == newer.version
    assert current.active_build_id is None
    assert current.observed_head_sha == head
    assert result.deployment_status != "DEPLOYED"
    assert store.observe(key, request.commit_sha, "UNDEPLOYED", expected_version=old.version,
                         expected_active_build_id=None) is None
    if state != "MISSING":
        assert result.deployment_status == "WAITING_FOR_BUILD"
        fresh = store.observe(key, request.commit_sha, "UNDEPLOYED", expected_version=current.version,
                              expected_active_build_id=None)
        assert store.settle(key, 1, "DEPLOYED", expected_version=fresh.version,
                            head=request.commit_sha).deployment_status == "DEPLOYED"
