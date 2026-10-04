"""真实 PostgreSQL/dbt/MetricFlow 验证分支发布、查询和数据库权限边界。"""

import asyncio
import os
import secrets
import shutil
from decimal import Decimal
from uuid import uuid4

import psycopg2
import pytest
import yaml
from psycopg2 import sql

from dbt_metricflow_service.branch_models import CreateBranchRequest
from dbt_metricflow_service.branches import BranchService
from dbt_metricflow_service.publication import PublicationService
from dbt_metricflow_service.publication_models import PublishedQueryRequest
from dbt_metricflow_service.runtime import Runtime
from dbt_metricflow_service.runtime_execution import ExecutionError, RuntimeExecutor
from dbt_metricflow_service.settings import Settings
from dbt_metricflow_service.storage.branches import BranchStore
from tests.integration.test_postgres_platform_flow import FIXTURE, git

DSN_ENV = "SERVICE_TEST_DATABASE_URL"
ENABLED = "PLATFORM_TEST_POSTGRES"
UTF8 = "utf-8"
PRODUCTION = "production"
PREVIEW = "preview"
MODEL = "models/orders.sql"
SCHEMA_PREFIX = "dbt_dev_"
BUILD = "BUILD_RUN"
QUERY = "METRIC_QUERY"
CREATE_ROLE = "CREATE ROLE {} LOGIN PASSWORD %s"
CREATE_SCHEMA = "CREATE SCHEMA {}"
GRANT_SCHEMA = "GRANT USAGE,CREATE ON SCHEMA {} TO {}"
GRANT_RAW = "GRANT USAGE ON SCHEMA {} TO {}"
GRANT_SELECT = "GRANT SELECT ON ALL TABLES IN SCHEMA {} TO {}"
DROP_SCHEMA = "DROP SCHEMA {} CASCADE"
DROP_OWNED = "DROP OWNED BY {}"
DROP_ROLE = "DROP ROLE {}"
CREATE_RAW = """CREATE TABLE {}.orders AS
 SELECT 1 AS order_id,date '2024-01-01' AS ordered_at,10.25::numeric AS revenue,'A'::text AS region
 UNION ALL SELECT 2,date '2024-02-01',20.75::numeric,'B'::text"""
RAW_TOTAL = "SELECT sum(revenue) FROM {}.orders"
WRITE_RAW = "UPDATE {}.orders SET revenue=0"
SET_ROLE = "SET ROLE {}"
MODEL_SQL = "select order_id,ordered_at,revenue {adjustment} as revenue,region from {{{{ source('raw','orders') }}}}\n"


async def test_two_branches_and_merged_main_are_physically_isolated(tmp_path, monkeypatch):
    if os.getenv(ENABLED) != "1" or not os.getenv(DSN_ENV):
        pytest.skip("需要独立 PostgreSQL 与真实工具链环境")
    # 随机 schema 和低权限账号只存在于显式指定的测试库，不使用生产数据。
    identifier = uuid4().hex[:16]
    raw_schema, prod_schema = "branch_raw_" + identifier, "branch_prod_" + identifier
    prod_role, dev_role = "branch_prod_" + identifier, "branch_dev_" + identifier
    schemas, roles = [raw_schema, prod_schema], [prod_role, dev_role]
    admin = psycopg2.connect(os.environ[DSN_ENV])
    admin.autocommit = True
    runtime = None
    passwords = {role: secrets.token_urlsafe(24) for role in roles}
    try:
        with admin.cursor() as cursor:
            for role in roles:
                cursor.execute(sql.SQL(CREATE_ROLE).format(sql.Identifier(role)), (passwords[role],))
            for schema in schemas:
                cursor.execute(sql.SQL(CREATE_SCHEMA).format(sql.Identifier(schema)))
            cursor.execute(sql.SQL(CREATE_RAW).format(sql.Identifier(raw_schema)))
            cursor.execute(sql.SQL(GRANT_SCHEMA).format(sql.Identifier(prod_schema), sql.Identifier(prod_role)))
            for role in roles:
                cursor.execute(sql.SQL(GRANT_RAW).format(sql.Identifier(raw_schema), sql.Identifier(role)))
                cursor.execute(sql.SQL(GRANT_SELECT).format(sql.Identifier(raw_schema), sql.Identifier(role)))
        repo, profiles = tmp_path / "repo", tmp_path / "profiles"
        shutil.copytree(FIXTURE, repo)
        profiles.mkdir()
        config = {"version": 2, "sources": [{"name": "raw", "schema": raw_schema,
                                              "tables": [{"name": "orders"}]}]}
        (repo / "models/raw.yml").write_text(yaml.safe_dump(config), encoding=UTF8)
        (repo / MODEL).write_text(MODEL_SQL.format(adjustment=""), encoding=UTF8)
        git(repo, "init", "-b", "main")
        git(repo, "config", "user.name", "Test")
        git(repo, "config", "user.email", "test@example.invalid")
        git(repo, "add", ".")
        git(repo, "commit", "-m", "raw baseline")
        dsn = psycopg2.extensions.parse_dsn(os.environ[DSN_ENV])
        outputs = {}
        for target, role in ((PRODUCTION, prod_role), (PREVIEW, dev_role)):
            env = "BRANCH_TEST_PASSWORD_" + target.upper()
            monkeypatch.setenv(env, passwords[role])
            outputs[target] = {"type": "postgres", "host": dsn["host"], "port": int(dsn.get("port", 5432)),
                               "user": role, "password": "{{ env_var('" + env + "') }}", "dbname": dsn["dbname"],
                               "schema": "{{ env_var('DBT_PLATFORM_SCHEMA') }}", "threads": 2}
        (profiles / "profiles.yml").write_text(yaml.safe_dump({"postgres_platform": {
            "target": PRODUCTION, "outputs": outputs}}), encoding=UTF8)
        settings = Settings(tmp_path, profiles, 180, 1048576, database_url=os.environ[DSN_ENV],
                            temp_root=tmp_path / "runtime", toolchain_version=uuid4().hex,
                            branch_preview_profile_binding_id=PREVIEW)
        runtime = Runtime(settings)
        project = "branch-real-" + identifier
        runtime.jobs.register_project(project, {"projectId": project, "remote": str(repo), "projectSubdir": ".",
                                               "profileBindingId": PRODUCTION, "schemaName": prod_schema})
        branches = BranchService(runtime)
        main = BranchStore(runtime.db).production(project)
        base_sha = git(repo, "rev-parse", "HEAD")

        async def build(branch_id=None, *, succeeds=True):
            service = PublicationService(runtime, branch_id=branch_id)
            release = service.submit(project, uuid4().hex)
            job = runtime.jobs.claim(str(uuid4()), toolchain_version=runtime.toolchain, kinds=[BUILD])
            executor = RuntimeExecutor(settings, runtime.jobs, runtime.artifacts)
            if succeeds:
                result = await executor.execute(job)
                assert runtime.jobs.finish(job["job_id"], job["lease_token"], result.payload,
                                           output_set_id=result.output_set_id)
                assert service.release(project, release["release_id"])["state"] == "PUBLISHED"
            else:
                with pytest.raises(ExecutionError) as failure:
                    await executor.execute(job)
                runtime.jobs.fail(job["job_id"], job["lease_token"], "TEST_FAILED", failure.value.payload)
                summary = service.release(project, release["release_id"])["validationSummary"]
                assert any(check["status"] == "FAILED" for check in summary["checks"])
                assert any("fail" in check["name"] for check in summary["checks"])
            return service, release, job

        async def total(service, release):
            catalog = service.catalog(project, release["release_id"])
            resource = next(item for item in catalog["resources"] if item["kind"] == "METRIC")
            metric = resource["resourceId"]
            # 本验收逐步驱动真实执行器，先完成异步选项任务再受理查询。
            options = runtime.submit_options(release["run_id"], (resource["name"],))
            if options["status"] != "SUCCEEDED":
                option_job = runtime.jobs.claim(str(uuid4()), toolchain_version=runtime.toolchain,
                                                kinds=["QUERY_OPTIONS"])
                option_result = await RuntimeExecutor(settings, runtime.jobs, runtime.artifacts).execute(option_job)
                assert runtime.jobs.finish(option_job["job_id"], option_job["lease_token"], option_result.payload)
            accepted = await asyncio.to_thread(service.submit_query, project, PublishedQueryRequest(
                releaseId=release["release_id"], mode="QUERY", metricResourceIds=[metric],
                idempotencyKey=uuid4().hex), "platform")
            job = runtime.jobs.claim(str(uuid4()), toolchain_version=runtime.toolchain, kinds=[QUERY])
            output = await RuntimeExecutor(settings, runtime.jobs, runtime.artifacts).execute(job)
            assert runtime.jobs.finish(job["job_id"], job["lease_token"], output.payload)
            return Decimal(str(service.get_query(project, accepted["queryId"])["rows"][0][0]))

        production, initial, _ = await build()
        assert await total(production, initial) == Decimal("31")
        created = [branches.create(project, CreateBranchRequest(name=name, sourceBranchId=main["branch_id"],
                    sourceCommitSha=base_sha, idempotencyKey=uuid4().hex)) for name in ("feature-a", "feature-b")]
        with admin.cursor() as cursor:
            for branch in created:
                schema = SCHEMA_PREFIX + branch.branch_id.hex
                schemas.append(schema)
                cursor.execute(sql.SQL(CREATE_SCHEMA).format(sql.Identifier(schema)))
                cursor.execute(sql.SQL(GRANT_SCHEMA).format(sql.Identifier(schema), sql.Identifier(dev_role)))
        git(repo, "checkout", "feature-a")
        (repo / MODEL).write_text(MODEL_SQL.format(adjustment="* 2"), encoding=UTF8)
        git(repo, "commit", "-am", "double revenue")
        a, ra, ja = await build(str(created[0].branch_id))
        assert await total(a, ra) == Decimal("62")
        git(repo, "checkout", "feature-b")
        (repo / MODEL).write_text(MODEL_SQL.format(adjustment="* 3"), encoding=UTF8)
        (repo / "tests").mkdir(exist_ok=True)
        (repo / "tests/fail.sql").write_text("select 1 as failure", encoding=UTF8)
        git(repo, "add", ".")
        git(repo, "commit", "-m", "failed preview")
        b, _, jb = await build(str(created[1].branch_id), succeeds=False)
        assert b.publication(project)["activePublication"] is None
        assert await total(a, ra) == Decimal("62")
        assert await total(production, initial) == Decimal("31")
        assert ja["schema_name"] != jb["schema_name"] != prod_schema
        git(repo, "checkout", "main")
        git(repo, "merge", "--no-ff", "feature-a", "-m", "merge approved feature")
        production, merged, merged_job = await build()
        assert merged_job["profile_binding_id"] == PRODUCTION
        assert merged_job["schema_name"] == prod_schema
        assert merged["request_json"]["commitSha"] == git(repo, "rev-parse", "HEAD")
        assert await total(production, merged) == Decimal("62")
        with admin.cursor() as cursor:
            cursor.execute(sql.SQL(RAW_TOTAL).format(sql.Identifier(raw_schema)))
            assert cursor.fetchone()[0] == Decimal("31")
            cursor.execute(sql.SQL(SET_ROLE).format(sql.Identifier(dev_role)))
            with pytest.raises(psycopg2.errors.InsufficientPrivilege):
                cursor.execute(sql.SQL(WRITE_RAW).format(sql.Identifier(raw_schema)))
            cursor.execute("RESET ROLE")
    finally:
        if runtime:
            await runtime.close()
        # 仅清理本测试生成并记录的 schema/角色；测试库发布记录保留审计证据。
        with admin.cursor() as cursor:
            cursor.execute("RESET ROLE")
            for schema in reversed(schemas):
                cursor.execute(sql.SQL(DROP_SCHEMA).format(sql.Identifier(schema)))
            for role in roles:
                cursor.execute(sql.SQL(DROP_OWNED).format(sql.Identifier(role)))
                cursor.execute(sql.SQL(DROP_ROLE).format(sql.Identifier(role)))
        admin.close()
