"""部署顺序由受理意图决定，Git 观察失败不是分支删除。"""

from uuid import uuid4

from dbt_metricflow_service.application.catalog import CatalogService
from dbt_metricflow_service.storage.artifacts import ArtifactStore
from tests.test_build_acceptance import CALLER, request, service
from tests.test_publication_storage import store as store


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
    first = app.submit_snapshot(request(branchName=branch, deploymentPolicy="ON_SUCCESS"), CALLER)
    second = app.submit_snapshot(request(branchName=branch, deploymentPolicy="ON_SUCCESS"), CALLER)
    deploy = deployment_service(store, "a" * 40)
    from dbt_metricflow_service.models.deployments import DeploymentKey

    key = DeploymentKey(repository=first.repository, environment=str(first.execution.environment_id), branchName=branch)
    with store.db.transaction() as connection:
        connection.exec_driver_sql(
            "UPDATE runtime_job SET status='FAILED',error_code='TEST_FAILURE' WHERE job_id=%s",
            (app.store.get(second.build_id).run_id,),
        )
    assert deploy.reconcile(key, second.initial_deployment.generation).deployment_status == "FAILED"
    assert deploy.reconcile(key, first.initial_deployment.generation).deployment_status == "STALE"
    assert deploy.current(key).active_build_id is None


def test_missing_ref_disables_but_network_error_does_not(store):
    from dbt_metricflow_service.application.deployments import DeploymentService
    from dbt_metricflow_service.models.deployments import DeploymentKey
    from dbt_metricflow_service.storage.deployments import DeploymentStore

    app = service(store)
    build = app.submit_snapshot(request(branchName="feature/" + uuid4().hex, deploymentPolicy="ON_SUCCESS"), CALLER)
    key = DeploymentKey(
        repository=build.repository, environment=str(build.execution.environment_id), branchName=build.branch_name
    )
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


def test_auto_deploy_retry_and_recreated_branch_require_new_build(store, tmp_path):
    from dbt_metricflow_service.application.deployments import DeploymentService
    from dbt_metricflow_service.models.deployments import DeploymentKey
    from dbt_metricflow_service.storage.deployments import DeploymentStore
    from tests.test_catalog_queries import ready_build

    app, build, jobs, artifacts = ready_build(store, tmp_path, deploymentPolicy="ON_SUCCESS")
    observed = [build.commit_sha]
    deploy = DeploymentService(
        DeploymentStore(store.db), lambda *_: observed[0], builds=app, catalogs=CatalogService(app.store, artifacts)
    )
    key = DeploymentKey(
        repository=build.repository, environment=str(build.execution.environment_id), branchName=build.branch_name
    )
    attempt = build.initial_deployment
    assert deploy.reconcile(key, attempt.generation).deployment_status == "DEPLOYED"
    body = app.store.get(build.build_id).request_json
    assert app.submit_snapshot(body, CALLER).initial_deployment.generation == attempt.generation
    # GET current 的 head 观察不使业务 CAS 版本变化。
    version = deploy.current(key).version
    assert deploy.current(key).version == version
    observed[0] = None
    assert deploy.current(key).active_build_id is None
    observed[0] = build.commit_sha
    assert deploy.current(key).active_build_id is None
    assert deploy.reconcile(key, attempt.generation).deployment_status == "DEPLOYED"
    assert deploy.current(key).active_build_id is None
    _, renewed_build, _, _ = ready_build(
        store, tmp_path, branchName=build.branch_name, execution=build.execution, deploymentPolicy="ON_SUCCESS",
    )
    renewed = renewed_build.initial_deployment
    assert renewed_build.build_id != build.build_id
    assert deploy.reconcile(key, renewed.generation).deployment_status == "DEPLOYED"


def test_deploy_allows_historical_toolchain(store, tmp_path):
    from dbt_metricflow_service.application.deployments import DeploymentService
    from dbt_metricflow_service.models.deployments import DeploymentKey
    from dbt_metricflow_service.storage.deployments import DeploymentStore
    from tests.test_catalog_queries import ready_build

    app, build, jobs, artifacts = ready_build(store, tmp_path, deploymentPolicy="ON_SUCCESS")
    app.toolchain = "unavailable"
    deploy = DeploymentService(
        DeploymentStore(store.db),
        lambda *_: build.commit_sha,
        builds=app,
        catalogs=CatalogService(app.store, artifacts),
    )
    attempt = build.initial_deployment
    key = DeploymentKey(
        repository=build.repository, environment=str(build.execution.environment_id), branchName=build.branch_name
    )
    assert deploy.reconcile(key, attempt.generation).deployment_status == "DEPLOYED"
    assert str(deploy.current(key).active_build_id) == str(build.build_id)


def test_legacy_independent_deployment_remains_readable(store, tmp_path):
    from dbt_metricflow_service.models.deployments import DeploymentKey
    from tests.test_catalog_queries import ready_build

    app, build, _, _ = ready_build(store, tmp_path, deploymentPolicy="ON_SUCCESS")
    # 模拟旧版已受理的独立部署；它不能被误认为构建附带的初始部署。
    with store.db.transaction() as connection:
        connection.exec_driver_sql(
            "UPDATE engine_deployment_attempt SET operation='DEPLOYMENT',idempotency_key='legacy-deploy' "
            "WHERE build_id=%s", (str(build.build_id),)
        )
    assert app.snapshot_view(build.build_id).initial_deployment is None
    deploy = deployment_service(store, build.commit_sha)
    key = DeploymentKey(
        repository=build.repository, environment=str(build.execution.environment_id), branchName=build.branch_name
    )
    assert deploy.reconcile(key, build.initial_deployment.generation).deployment_status == "DEPLOYED"
    assert deploy.current(key).active_build_id == build.build_id
    assert deploy.history(key, cursor=None, limit=20).items[0].build_id == build.build_id
