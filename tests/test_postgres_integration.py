from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace

import pytest
from sqlalchemy import func, select

from dbt_metricflow_service.application.builds import BuildService
from dbt_metricflow_service.application.errors import ServiceError
from dbt_metricflow_service.models.builds import DeploymentPolicy
from dbt_metricflow_service.models.deployments import DeploymentKey
from dbt_metricflow_service.storage.artifacts import ArtifactStore
from dbt_metricflow_service.storage.builds import BuildStore
from dbt_metricflow_service.storage.deployments import DeploymentStore
from dbt_metricflow_service.storage.entities import Build, RuntimeJob


def test_transactional_idempotency_after_config_removal(database, settings, build_request):
    store = BuildStore(database)
    service = BuildService(store, "test-toolchain", settings)
    with ThreadPoolExecutor(max_workers=2) as pool:
        ids = list(pool.map(lambda _: service.submit(build_request, "service").build_id, range(2)))
    assert ids[0] == ids[1]
    revoked = BuildService(store, "different-toolchain", replace(settings, execution_instances={}))
    assert revoked.submit(build_request, "service").build_id == ids[0]
    with pytest.raises(ServiceError) as conflict:
        revoked.submit(build_request.model_copy(update={"commit_sha": "c" * 40}), "service")
    assert conflict.value.status == 409
    with pytest.raises(ServiceError) as invalid:
        revoked.submit(build_request.model_copy(update={"idempotency_key": "new"}), "service")
    assert invalid.value.error.code == "EXECUTION_TARGET_UNVERIFIED"
    with database.session() as session:
        assert session.scalar(select(func.count()).select_from(Build)) == 1
        assert session.scalar(select(func.count()).select_from(RuntimeJob)) == 1


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
    job = builds.jobs.get(build.run_id)
    source = tmp_path / "source"
    source.mkdir()
    (source / "dbt_project.yml").write_text("name: test\nversion: '1'\n", encoding="utf-8")
    output = ArtifactStore(database).capture(job.project_id, source)
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
