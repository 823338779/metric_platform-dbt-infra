import asyncio
import os
from types import SimpleNamespace
from uuid import uuid4

from dbt_metricflow_service.settings import Settings
from tests.data_helpers import leased_job
from tests.test_publication_storage import store as store


async def test_worker_completes_with_artifact_identity_without_result_payload():
    from dbt_metricflow_service.runtime.worker import Worker

    completed = []
    output = str(uuid4())

    class Jobs:
        def finish(self, job_id, token, *, output_set_id, seal):
            completed.append((job_id, token, output_set_id, seal))
            return True

        def fail(self, *args, **kwargs):
            raise AssertionError("successful artifact completion must not fail")

        def external_started(self, attempt_id):
            return False

    class Executor:
        async def execute(self, job):
            return SimpleNamespace(output_set_id=output)

    seal = object()
    worker = Worker(
        SimpleNamespace(
            jobs=Jobs(),
            artifacts=SimpleNamespace(seal=seal),
            settings=SimpleNamespace(heartbeat_seconds=60),
        )
    )
    worker.executor = Executor()
    job = leased_job()
    await worker._execute(job)
    assert completed == [(job.job_id, job.lease_token, output, seal)]


async def test_runtime_reconciles_explicit_deployments(store, tmp_path):
    from dbt_metricflow_service.application.catalog import CatalogService
    from dbt_metricflow_service.application.deployments import DeploymentService
    from dbt_metricflow_service.models.deployments import DeploymentKey, DeploymentRequest
    from dbt_metricflow_service.runtime.service import Runtime
    from dbt_metricflow_service.storage.deployments import DeploymentStore
    from tests.test_catalog_queries import ready_build

    builds, build, jobs, artifacts = ready_build(store, tmp_path)
    deployments = DeploymentService(
        DeploymentStore(store.db),
        lambda *_: build.commit_sha,
        builds=builds,
        catalogs=CatalogService(builds.store, artifacts),
    )
    deployments.submit(
        DeploymentRequest(
            buildId=build.build_id,
            branchName=build.branch_name,
            expectedTargetVersion=0,
            idempotencyKey=uuid4().hex,
        ),
        "worker-test",
    )
    key = DeploymentKey(
        repository=build.repository, environment=str(build.execution.environment_id), branchName=build.branch_name
    )
    runtime = Runtime(
        Settings(
            profiles_dir=tmp_path,
            database_url=os.environ["SERVICE_TEST_DATABASE_URL"],
            max_output_bytes=1024,
            temp_root=tmp_path / "worker",
            toolchain_version=build.toolchain_version,
            heartbeat_seconds=1,
        )
    )
    try:
        await runtime.start(deployments=deployments)
        async with asyncio.timeout(5):
            while deployments.store.current(key).active_build_id is None:
                await asyncio.sleep(0.02)
        assert deployments.store.current(key).active_build_id == str(build.build_id)
    finally:
        await runtime.close()
