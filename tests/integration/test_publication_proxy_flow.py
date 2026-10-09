"""真实工具链生成可直接读取的发布产物。"""

import asyncio
import json
import os
import shutil
from decimal import Decimal
from uuid import uuid4

import psycopg2
import pytest
from psycopg2 import sql

from dbt_metricflow_service.publications.models import FixedCommitRequest, PublishedQueryRequest, QueryOptionsRequest
from dbt_metricflow_service.publications.queries import QueryService
from dbt_metricflow_service.publications.service import PublicationService, ReleaseGone
from dbt_metricflow_service.runtime.completion import complete_job
from dbt_metricflow_service.runtime.executor import ExecutionError, RuntimeExecutor
from dbt_metricflow_service.runtime.service import Runtime
from dbt_metricflow_service.settings import Settings
from dbt_metricflow_service.storage.postgres import Database
from tests.integration.helpers import FIXTURE, PROFILES, git

DSN_ENV = "SERVICE_TEST_DATABASE_URL"
ENABLED_ENV = "PLATFORM_TEST_POSTGRES"


async def test_full_publication_with_real_dbt_and_metricflow(tmp_path):
    if os.getenv(ENABLED_ENV) != "1" or not os.getenv(DSN_ENV):
        pytest.skip("需要独立 PostgreSQL 与真实工具链环境")
    database = Database(os.environ[DSN_ENV])
    database.migrate()
    database.close()
    repo = tmp_path / "repo"
    shutil.copytree(FIXTURE, repo)
    # 真实嵌套目录的 fqn 包含路径段，不能由 unique_id 直接去前缀得到。
    (repo / "models" / "staging").mkdir()
    (repo / "models" / "orders.sql").rename(repo / "models" / "staging" / "orders.sql")
    git(repo, "init", "-b", "main")
    git(repo, "config", "user.name", "Test")
    git(repo, "config", "user.email", "test@example.invalid")
    git(repo, "add", ".")
    git(repo, "commit", "-m", "fixture")
    profiles = tmp_path / "profiles"
    profiles.mkdir()
    (profiles / "profiles.yml").write_text(PROFILES)
    settings = Settings(profiles, 180, 1048576, database_url=os.environ[DSN_ENV],
                        temp_root=tmp_path / "runtime", toolchain_version=uuid4().hex)
    runtime = Runtime(settings)
    try:
        project = "real-publish-" + uuid4().hex
        runtime.jobs.register_project(project, {"projectId": project, "remote": str(repo),
                                               "projectSubdir": ".", "profileBindingId": "postgres"})
        from dbt_metricflow_service.validation.service import CommitValidationService
        validation_service = CommitValidationService(runtime)
        validation = validation_service.submit(project, FixedCommitRequest(
            commitSha=git(repo, "rev-parse", "HEAD"), idempotencyKey=uuid4().hex))
        validation_job = runtime.jobs.claim(str(uuid4()), toolchain_version=runtime.toolchain,
                                            kinds=["DRAFT_VALIDATION"])
        validation_result = await RuntimeExecutor(settings, runtime.jobs, runtime.artifacts).execute(validation_job)
        assert validation_result.payload["valid"] is True
        complete_job(runtime.jobs, validation_job["job_id"], validation_job["lease_token"], validation_result.payload)
        assert validation_service.get(project, str(validation.validation_id)).valid is True
        assert PublicationService(runtime).publication(project)["activePublication"] is None
        with runtime.db.transaction() as connection:
            assert connection.exec_driver_sql("SELECT 1 FROM pg_namespace WHERE nspname=%s",
                ("validation_" + str(validation.validation_id).replace("-", ""),)).first() is None
        release = PublicationService(runtime).submit(project, FixedCommitRequest(
            commitSha=git(repo, "rev-parse", "HEAD"), idempotencyKey=uuid4().hex))
        job = runtime.jobs.claim(str(uuid4()), toolchain_version=runtime.toolchain)
        result = await RuntimeExecutor(settings, runtime.jobs, runtime.artifacts).execute(job)
        assert complete_job(runtime.jobs, job["job_id"], job["lease_token"], result.payload,
                                   output_set_id=result.output_set_id)
        published = PublicationService(runtime).store.get_release(project, release["release_id"])
        assert published["state"] == "PUBLISHED"
        catalog = runtime.artifacts.metadata(published["artifact_set_id"])["catalog_json"]
        assert any(item["kind"] == "METRIC" for item in catalog["resources"])
        assert catalog["releaseId"] == release["release_id"]
        old_relation = next(item["relation"] for item in catalog["relationBindings"]
                            if item["nativeId"].endswith(".orders"))

        def old_rows():
            with psycopg2.connect(host=os.environ["PLATFORM_TEST_PGHOST"],
                                  port=os.environ["PLATFORM_TEST_PGPORT"],
                                  user=os.environ["PLATFORM_TEST_PGUSER"],
                                  password=os.environ["PLATFORM_TEST_PGPASSWORD"],
                                  dbname=os.environ["PLATFORM_TEST_PGDATABASE"]) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(sql.SQL("SELECT * FROM {}.{} ORDER BY 1").format(
                        sql.Identifier(old_relation["schema"]), sql.Identifier(old_relation["identifier"])))
                    return cursor.fetchall()

        original_rows = old_rows()
        service = QueryService(runtime)
        releases = PublicationService(runtime)
        metric = next(item["resourceId"] for item in catalog["resources"] if item["kind"] == "METRIC")
        # 真实 worker 生成选项；停止它后保持 A 查询排队，明确覆盖切换后的旧查询执行。
        await runtime.start()
        await asyncio.to_thread(service.query_options, project, QueryOptionsRequest(
            releaseId=release["release_id"], metricResourceIds=[metric]))
        await runtime.worker.close()
        runtime.worker = None
        query_request = PublishedQueryRequest(releaseId=release["release_id"], idempotencyKey=uuid4().hex,
                                             mode="QUERY", metricResourceIds=[metric])
        accepted = await asyncio.to_thread(service.submit_query, project, query_request, "platform")
        # 语义变更与 SQL 变更都全量构建，旧版本的物理结果继续保留。
        model_file = next(path for path in (repo / "models").glob("*.yml") if "revenue" in path.read_text())
        model_file.write_text(model_file.read_text() + "\n# semantic documentation update\n")
        git(repo, "commit", "-am", "semantic-only")
        semantic_release = PublicationService(runtime).submit(project, FixedCommitRequest(
            commitSha=git(repo, "rev-parse", "HEAD"), idempotencyKey=uuid4().hex))
        semantic_job = runtime.jobs.claim(str(uuid4()), toolchain_version=runtime.toolchain, kinds=["BUILD_RUN"])
        semantic_result = await RuntimeExecutor(settings, runtime.jobs, runtime.artifacts).execute(semantic_job)
        complete_job(runtime.jobs, semantic_job["job_id"], semantic_job["lease_token"], semantic_result.payload,
                            output_set_id=semantic_result.output_set_id)
        semantic_row = PublicationService(runtime).store.get_release(project, semantic_release["release_id"])
        assert semantic_row["build_mode"] == "FULL_BUILD"
        semantic_catalog = runtime.artifacts.metadata(semantic_row["artifact_set_id"])["catalog_json"]
        assert {item["relation"]["identifier"] for item in semantic_catalog["relationBindings"]} != {
            item["relation"]["identifier"] for item in catalog["relationBindings"]}
        assert all(item["mode"] == "BUILT" for item in semantic_catalog["relationBindings"])
        with pytest.raises(ReleaseGone):
            service.submit_query(project, query_request.model_copy(update={"idempotency_key": uuid4().hex}), "platform")
        assert service.submit_query(project, query_request, "platform")["queryId"] == accepted["queryId"]
        query_job = runtime.jobs.claim(str(uuid4()), toolchain_version=runtime.toolchain, kinds=["METRIC_QUERY"])
        query_result = await RuntimeExecutor(settings, runtime.jobs, runtime.artifacts).execute(query_job)
        complete_job(runtime.jobs, query_job["job_id"], query_job["lease_token"], query_result.payload)
        old_result = service.get_query(project, accepted["queryId"])
        assert old_result["state"] == "READY"
        assert old_result["releaseId"] == release["release_id"]
        assert Decimal(str(old_result["rows"][0][0])) == Decimal("31.00")
        sql_file = repo / "models" / "staging" / "orders.sql"
        sql_file.write_text(sql_file.read_text().replace("10.25", "11.25"))
        git(repo, "commit", "-am", "physical-change")
        selected_release = PublicationService(runtime).submit(project, FixedCommitRequest(
            commitSha=git(repo, "rev-parse", "HEAD"), idempotencyKey=uuid4().hex))
        selected_job = runtime.jobs.claim(str(uuid4()), toolchain_version=runtime.toolchain)
        selected_result = await RuntimeExecutor(settings, runtime.jobs, runtime.artifacts).execute(selected_job)
        complete_job(runtime.jobs, selected_job["job_id"], selected_job["lease_token"], selected_result.payload,
                            output_set_id=selected_result.output_set_id)
        selected_row = PublicationService(runtime).store.get_release(project, selected_release["release_id"])
        assert selected_row["build_mode"] == "FULL_BUILD"
        selected_catalog = runtime.artifacts.metadata(selected_row["artifact_set_id"])["catalog_json"]
        assert any(item["mode"] == "BUILT" for item in selected_catalog["relationBindings"])
        # 不安全候选 D 在解析前失败，活动 C 与 A 已受理结果均保留。
        unsafe = repo / "macros" / "unsafe.sql"
        dangerous = 'drop table "' + old_relation["schema"] + '"."' + old_relation["identifier"] + '"'
        unsafe.write_text("{{ run_query(" + json.dumps(dangerous) + ") }}")
        git(repo, "add", ".")
        git(repo, "commit", "-m", "unsafe-candidate")
        rejected = releases.submit(project, FixedCommitRequest(
            commitSha=git(repo, "rev-parse", "HEAD"), idempotencyKey=uuid4().hex))
        rejected_job = runtime.jobs.claim(str(uuid4()), toolchain_version=runtime.toolchain)
        with pytest.raises((ValueError, ExecutionError)):
            await RuntimeExecutor(settings, runtime.jobs, runtime.artifacts).execute(rejected_job)
        runtime.jobs.fail(rejected_job["job_id"], rejected_job["lease_token"], "INVALID_PUBLICATION")
        assert releases.release(project, rejected["release_id"])["state"] == "FAILED"
        assert releases.publication(project)["activePublication"]["releaseId"] == selected_release["release_id"]
        assert service.get_query(project, accepted["queryId"])["rows"] == old_result["rows"]
        assert old_rows() == original_rows
        # 全程没有平台回调；服务仍能独立发布 E，新的只读消费者立即读取它。
        unsafe.unlink()
        git(repo, "commit", "-am", "recover-candidate")
        recovered = releases.submit(project, FixedCommitRequest(
            commitSha=git(repo, "rev-parse", "HEAD"), idempotencyKey=uuid4().hex))
        recovered_job = runtime.jobs.claim(str(uuid4()), toolchain_version=runtime.toolchain)
        recovered_result = await RuntimeExecutor(settings, runtime.jobs, runtime.artifacts).execute(recovered_job)
        complete_job(runtime.jobs, recovered_job["job_id"], recovered_job["lease_token"], recovered_result.payload,
                            output_set_id=recovered_result.output_set_id)
        consumer = PublicationService(runtime)
        assert consumer.publication(project)["activePublication"]["releaseId"] == recovered["release_id"]
        assert service.catalogs.catalog(project, recovered["release_id"])["resources"]
    finally:
        await runtime.close()
