"""部署顺序由受理意图决定，Git 观察失败不是分支删除。"""

from uuid import uuid4

from dbt_metricflow_service.application.catalog import CatalogService
from dbt_metricflow_service.storage.artifacts import ArtifactStore
from tests.test_publication_storage import store as store
from tests.test_v3_build_acceptance import CALLER, request, service


def deployment_service(store, head):
    from dbt_metricflow_service.application.deployments import DeploymentService
    from dbt_metricflow_service.storage.deployments import DeploymentStore

    builds = service(store)
    return DeploymentService(
        DeploymentStore(store.db),
        lambda repository, branch: head,
        builds=builds,
        catalogs=CatalogService(builds.store, ArtifactStore(store.db)),
    )


def test_newer_failed_intent_still_fences_older(store):
    app = service(store)
    branch = "feature/" + uuid4().hex
    first = app.submit(request(branchName=branch, deploymentPolicy="ON_SUCCESS"), CALLER)
    second = app.submit(request(branchName=branch, deploymentPolicy="ON_SUCCESS"), CALLER)
    deploy = deployment_service(store, "a" * 40)
    from dbt_metricflow_service.models.deployments import DeploymentKey

    key = DeploymentKey(repository=first.repository, environment=first.environment, branchName=branch)
    with store.db.transaction() as connection:
        connection.exec_driver_sql(
            "UPDATE runtime_job SET status='FAILED',error_code='TEST_FAILURE' WHERE job_id=%s",
            (app.store.get(second.build_id)["run_id"],),
        )
    assert deploy.reconcile(key, second.initial_deployment["generation"]).deployment_status == "FAILED"
    assert deploy.reconcile(key, first.initial_deployment["generation"]).deployment_status == "STALE"
    assert deploy.current(key).active_build_id is None


def test_missing_ref_disables_but_network_error_does_not(store):
    from dbt_metricflow_service.application.deployments import DeploymentService
    from dbt_metricflow_service.models.deployments import DeploymentKey
    from dbt_metricflow_service.storage.deployments import DeploymentStore

    app = service(store)
    build = app.submit(request(branchName="feature/" + uuid4().hex, deploymentPolicy="ON_SUCCESS"), CALLER)
    key = DeploymentKey(repository=build.repository, environment=build.environment, branchName=build.branch_name)
    with store.db.transaction() as connection:
        connection.exec_driver_sql(
            "UPDATE engine_deployment_target SET active_build_id=%s WHERE repository=%s AND branch_name=%s",
            (str(build.build_id), build.repository, build.branch_name),
        )

    def unavailable(repository, branch):
        raise ValueError("network unavailable")

    offline = DeploymentService(
        DeploymentStore(store.db), unavailable, builds=app, catalogs=CatalogService(app.store, ArtifactStore(store.db))
    ).current(key)
    assert offline.source_state == "UNKNOWN"
    assert offline.active_build_id == build.build_id
    missing = deployment_service(store, None).current(key)
    assert missing.active_build_id is None
    assert missing.source_state == "MISSING"


def test_independent_deploy_retry_and_recreated_branch_require_new_intent(store, tmp_path):
    from dbt_metricflow_service.application.deployments import DeploymentService
    from dbt_metricflow_service.models.deployments import DeploymentKey, DeploymentRequest
    from dbt_metricflow_service.storage.deployments import DeploymentStore
    from tests.test_v3_catalog_queries import ready_build

    app, build, jobs, artifacts = ready_build(store, tmp_path)
    observed = [build.commit_sha]
    deploy = DeploymentService(
        DeploymentStore(store.db), lambda *_: observed[0], builds=app, catalogs=CatalogService(app.store, artifacts)
    )
    key = DeploymentKey(repository=build.repository, environment=build.environment, branchName=build.branch_name)
    body = DeploymentRequest(
        buildId=build.build_id, branchName=build.branch_name, expectedTargetVersion=0, idempotencyKey=uuid4().hex
    )
    attempt = deploy.submit(body, CALLER)
    assert deploy.reconcile(key, attempt.generation).deployment_status == "DEPLOYED"
    assert deploy.submit(body, CALLER).generation == attempt.generation
    # GET current 的 head 观察不使业务 CAS 版本变化。
    version = deploy.current(key).version
    assert deploy.current(key).version == version
    observed[0] = None
    assert deploy.current(key).active_build_id is None
    observed[0] = build.commit_sha
    assert deploy.current(key).active_build_id is None
    assert deploy.reconcile(key, attempt.generation).deployment_status == "DEPLOYED"
    assert deploy.current(key).active_build_id is None
    renewed = deploy.submit(
        body.model_copy(
            update={"expected_target_version": deploy.current(key).version, "idempotency_key": uuid4().hex}
        ),
        CALLER,
    )
    assert deploy.reconcile(key, renewed.generation).deployment_status == "DEPLOYED"


def test_deploy_cannot_bypass_missing_execution_environment(store, tmp_path):
    from dbt_metricflow_service.application.deployments import DeploymentService
    from dbt_metricflow_service.models.deployments import DeploymentKey, DeploymentRequest
    from dbt_metricflow_service.storage.deployments import DeploymentStore
    from tests.test_v3_catalog_queries import ready_build

    app, build, jobs, artifacts = ready_build(store, tmp_path)
    app.toolchain = "unavailable"
    deploy = DeploymentService(
        DeploymentStore(store.db),
        lambda *_: build.commit_sha,
        builds=app,
        catalogs=CatalogService(app.store, artifacts),
    )
    attempt = deploy.submit(
        DeploymentRequest(
            buildId=build.build_id, branchName=build.branch_name, expectedTargetVersion=0, idempotencyKey=uuid4().hex
        ),
        CALLER,
    )
    key = DeploymentKey(repository=build.repository, environment=build.environment, branchName=build.branch_name)
    assert deploy.reconcile(key, attempt.generation).reason == "EXECUTION_ENVIRONMENT_UNAVAILABLE"
    assert deploy.current(key).active_build_id is None
