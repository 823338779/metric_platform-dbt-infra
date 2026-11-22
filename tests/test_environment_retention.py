"""环境内跨仓库、分支的部署切换与存量产物清理。"""

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import func

from dbt_metricflow_service.application.builds import BuildService
from dbt_metricflow_service.application.catalog import CatalogService
from dbt_metricflow_service.application.errors import ServiceError
from dbt_metricflow_service.execution.logs import EngineLog
from dbt_metricflow_service.execution.query import QueryExecutor
from dbt_metricflow_service.models.builds import DeploymentPolicy
from dbt_metricflow_service.models.deployments import DeploymentKey
from dbt_metricflow_service.models.queries import PreviewRequest
from dbt_metricflow_service.storage.artifacts import ArtifactStore
from dbt_metricflow_service.storage.builds import BuildStore
from dbt_metricflow_service.storage.deployments import DeploymentStore, natural
from dbt_metricflow_service.storage.entities import (
    ArtifactSet,
    Build,
    DeploymentAttempt,
    DeploymentTarget,
    RuntimeAttempt,
)
from dbt_metricflow_service.storage.execution_logs import ExecutionLogStore
from dbt_metricflow_service.storage.jobs import JobStore
from dbt_metricflow_service.storage.records import ArtifactMetadata


@pytest.mark.parametrize("existing", [False, True], ids=["successful-switch", "existing-environment"])
def test_environment_retention(database, settings, build_request, tmp_path, existing, monkeypatch):
    store = BuildStore(database)
    builds = BuildService(store, "test-toolchain", settings)
    deployments = DeploymentStore(database)
    artifacts = ArtifactStore(database)
    executor = QueryExecutor(settings, artifacts)
    catalogs = CatalogService(store, artifacts, executor.lineage, 1024)
    project = tmp_path / "source"
    project.mkdir()
    (project / "dbt_project.yml").write_text("name: test\nversion: '1'\n", encoding="utf-8")

    # 通过正式受理入口建立真实任务和部署意图，固定封存快照以隔离外部 Git 和引擎。
    def stage(branch, *, other_environment=False, shared_source=None):
        execution = build_request.execution
        if other_environment:
            execution = execution.model_copy(update={"environment_id": uuid4(), "schema_name": "other"})
        request = build_request.model_copy(update={
            "idempotency_key": uuid4(), "branch_name": branch, "execution": execution,
            "repository": f"https://example.invalid/{branch}.git", "deployment_policy": DeploymentPolicy.ON_SUCCESS,
        })
        build = builds.load(builds.submit(request, "service").build_id)
        job = JobStore(database).claim(uuid4(), toolchain_version="test-toolchain", build_id=build.build_id)
        ExecutionLogStore(database).append(job, "BUILD", "dbt", [EngineLog(datetime.now(UTC), "info", branch)])
        source = shared_source or artifacts.capture(job.project_id, project)
        output = artifacts.capture(job.project_id, project, kind="EXECUTION", metadata=ArtifactMetadata(
            source_set_id=source), memory_files={"target/manifest.json": b'{}'})
        with database.fact_session() as session:
            session.get(RuntimeAttempt, job.attempt_id).state = "SUCCEEDED"
            row = session.get(Build, job.build_id)
            row.build_status, row.input_set_id, row.output_set_id = "SUCCEEDED", source, output
        key = DeploymentKey(repository=request.repository, environment=str(execution.environment_id),
                            branch_name=branch)
        return key, build, source, output

    def settle(key):
        current = deployments.current(key)
        observation = deployments.observe(key, build_request.commit_sha, "UNDEPLOYED",
                                          expected_version=current.version, expected_active_build_id=None)
        assert deployments.settle(key, 1, "DEPLOYED", expected_version=observation.version,
                                  head=build_request.commit_sha).deployment_status == "DEPLOYED"

    old, old_build, old_source, old_output = stage("old")
    settle(old)
    current = deployments.current(old)
    missing = deployments.observe(old, None, "MISSING", expected_version=current.version,
                                  expected_active_build_id=old_build.build_id)
    assert missing.active_build_id == old_build.build_id
    other, other_build, _, _ = stage("other", other_environment=True)
    settle(other)
    assert catalogs.read(old_build.build_id)[1] == b'{}'
    failed, _, _, _ = stage("failed")
    deployments.settle(failed, 1, "FAILED", "test")
    assert deployments.current(old).active_build_id == old_build.build_id
    assert builds.get(old_build.build_id).catalog_available

    # 外部执行已停止但部署意图尚未协调的失败构建也应释放输入。
    _, unknown_build, unknown_source, _ = stage("unknown")
    with database.fact_session() as session:
        job = session.get(Build, unknown_build.build_id)
        job.build_status, job.error_code = "OUTCOME_UNKNOWN", "EXECUTION_OUTCOME_UNKNOWN"
        session.get(RuntimeAttempt, job.current_attempt_id).state = "STOPPED"
    _, unsafe, unsafe_source, _ = stage("unsafe")
    with database.fact_session() as session:
        job = session.get(Build, unsafe.build_id)
        job.build_status, job.error_code = "OUTCOME_UNKNOWN", "EXECUTION_OUTCOME_UNKNOWN"
        session.get(RuntimeAttempt, job.current_attempt_id).state = "EXPIRED_UNCONFIRMED"
    pending, pending_build, _, _ = stage("pending")
    new, current_build, _, _ = stage("new", shared_source=old_source)
    if existing:
        # 模拟升级前两个目标曾同时保有活动指针；管理命令按实际部署时间选择当前版本。
        with database.fact_session() as session:
            session.get(DeploymentTarget, natural(new)).active_build_id = current_build.build_id
            attempt = session.get(DeploymentAttempt, (*natural(new), 1))
            attempt.deployment_status, attempt.deployed_at = "DEPLOYED", func.clock_timestamp()
    else:
        # 注入回收故障，切换、旧指针和已解除的任务引用必须一同回滚。
        def fail_gc(*args):
            raise RuntimeError("gc unavailable")

        with monkeypatch.context() as patch:
            patch.setattr(ArtifactStore, "delete_in_transaction", fail_gc)
            with pytest.raises(RuntimeError, match="gc unavailable"):
                settle(new)
        assert deployments.current(old).active_build_id == old_build.build_id
        assert catalogs.read(old_build.build_id)[1] == b'{}'
        settle(new)
        assert deployments.current(old).active_build_id is None
        assert store.get(old_build.build_id).output_set_id is None

    # 重复清理、新连接与首次切换选择同一个版本；历史记录、其他环境和待部署工作仍可读。
    for _ in range(2):
        assert deployments.retain_current(new.environment) == current_build.build_id
    assert deployments.current(old).active_build_id is None
    assert deployments.current(new).active_build_id == current_build.build_id
    assert deployments.current(other).active_build_id == other_build.build_id
    assert deployments.attempt(pending, 1).deployment_status == "WAITING_FOR_BUILD"
    assert catalogs.read(pending_build.build_id)[1] == b'{}'
    assert catalogs.read(other_build.build_id)[1] == catalogs.read(current_build.build_id)[1] == b'{}'
    history = builds.get(old_build.build_id)
    assert history.build_status == "SUCCEEDED" and history.initial_deployment.deployment_status == "DEPLOYED"
    assert not history.catalog_available
    assert [entry.message for entry in builds.execution_logs(old_build.build_id).items] == ["old"]
    with database.session() as session:
        assert session.get(ArtifactSet, unsafe_source) is not None
        assert session.get(ArtifactSet, unknown_source) is None
        assert session.get(ArtifactSet, old_output) is None
        assert session.get(ArtifactSet, old_source) is not None
    assert store.get(old_build.build_id).input_set_id is None

    # 此前已成功读取的目录、维度与查询不能通过旧构建身份再次读取。
    retired = builds.load(old_build.build_id)
    for read in (
        lambda: catalogs.read(old_build.build_id),
        lambda: executor.available_dimensions(retired, ("revenue",)),
        lambda: executor.execute_query(retired, PreviewRequest(mode="PREVIEW", selector="orders")),
    ):
        with pytest.raises(ServiceError) as error:
            read()
        assert error.value.status == 410 and not error.value.error.retryable
        assert error.value.error.code == "ARTIFACT_UNAVAILABLE"
