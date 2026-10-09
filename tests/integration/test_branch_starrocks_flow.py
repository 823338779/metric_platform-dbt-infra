"""显式启用的 StarRocks 分支隔离验收；仅使用随机测试库和临时低权限账号。"""

import asyncio
import os
import secrets
import shutil
from decimal import Decimal
from uuid import uuid4

import mysql.connector
import pytest
import yaml

from dbt_metricflow_service.branches.models import CreateBranchRequest
from dbt_metricflow_service.branches.service import BranchService
from dbt_metricflow_service.publications.models import PublishedQueryRequest
from dbt_metricflow_service.publications.service import PublicationService
from dbt_metricflow_service.runtime.executor import ExecutionError, RuntimeExecutor
from dbt_metricflow_service.runtime.service import Runtime
from dbt_metricflow_service.settings import Settings
from dbt_metricflow_service.storage.branches import BranchStore
from tests.integration.test_postgres_platform_flow import FIXTURE, git

UTF8 = "utf-8"
MODEL = "models/orders.sql"
PRODUCTION = "production"
PREVIEW = "preview"
RAW_SQL = "select order_id,ordered_at,revenue {factor} as revenue,region from {{{{ source('raw','orders') }}}}\n"


async def test_starrocks_branch_build_query_and_write_permissions(tmp_path, monkeypatch):
    if os.getenv("RUN_STARROCKS_E2E") != "1" or not os.getenv("SERVICE_TEST_DATABASE_URL"):
        pytest.skip("需要显式启用 StarRocks 与独立服务测试库")
    host, port = os.environ["DBT_STARROCKS_HOST"], int(os.environ["DBT_STARROCKS_PORT"])
    admin = mysql.connector.connect(
        host=host,
        port=port,
        user=os.environ["DBT_STARROCKS_USER"],
        password=os.environ["DBT_ENV_SECRET_STARROCKS_PASSWORD"],
    )
    suffix = uuid4().hex[:12]
    raw, production = "branch_raw_" + suffix, "branch_prod_" + suffix
    users = {PRODUCTION: "prod_" + suffix, PREVIEW: "dev_" + suffix}
    passwords = {target: secrets.token_hex(16) for target in users}
    databases, created_users = [], []
    runtime = None

    def execute(statement, params=None):
        with admin.cursor() as cursor:
            cursor.execute(statement, params)
            return cursor.fetchall() if cursor.with_rows else []

    def provision(database, target):
        execute(f"CREATE DATABASE `{database}`")
        databases.append(database)
        execute(f"GRANT CREATE TABLE, CREATE VIEW ON DATABASE `{database}` TO USER '{users[target]}'")
        execute(f"GRANT ALL ON ALL TABLES IN DATABASE `{database}` TO USER '{users[target]}'")
        execute(f"GRANT ALL ON ALL VIEWS IN DATABASE `{database}` TO USER '{users[target]}'")

    try:
        # 凭据只进入进程环境；项目 profile 不保存密码，清理仅触及此处登记的随机对象。
        for target, user in users.items():
            execute(f"CREATE USER '{user}' IDENTIFIED BY %s", (passwords[target],))
            created_users.append(user)
            monkeypatch.setenv("BRANCH_STARROCKS_" + target.upper(), passwords[target])
        provision(production, PRODUCTION)
        execute(f"CREATE DATABASE `{raw}`")
        databases.append(raw)
        execute(
            f"CREATE TABLE `{raw}`.orders (order_id INT, ordered_at DATE, revenue DECIMAL(12,2), region VARCHAR(8)) "
            "DUPLICATE KEY(order_id) DISTRIBUTED BY HASH(order_id) BUCKETS 1 PROPERTIES ('replication_num'='1')"
        )
        execute(f"INSERT INTO `{raw}`.orders VALUES (1,'2024-01-01',10.25,'A'),(2,'2024-02-01',20.75,'B')")
        for user in users.values():
            execute(f"GRANT SELECT ON ALL TABLES IN DATABASE `{raw}` TO USER '{user}'")
        repo, profiles = tmp_path / "repo", tmp_path / "profiles"
        shutil.copytree(FIXTURE, repo)
        profiles.mkdir()
        project_config = yaml.safe_load((repo / "dbt_project.yml").read_text(encoding=UTF8))
        project_config["models"]["postgres_platform"]["+properties"] = {"replication_num": "1"}
        (repo / "dbt_project.yml").write_text(yaml.safe_dump(project_config), encoding=UTF8)
        (repo / "models/time_spine.sql").write_text("select cast('2024-01-01' as date) as date_day\n", encoding=UTF8)
        (repo / MODEL).write_text(RAW_SQL.format(factor=""), encoding=UTF8)
        (repo / "models/raw.yml").write_text(
            yaml.safe_dump({"version": 2, "sources": [{"name": "raw", "schema": raw, "tables": [{"name": "orders"}]}]}),
            encoding=UTF8,
        )
        outputs = {
            target: {
                "type": "starrocks",
                "host": host,
                "port": port,
                "username": user,
                "password": "{{ env_var('BRANCH_STARROCKS_" + target.upper() + "') }}",
                "schema": "{{ env_var('DBT_PLATFORM_SCHEMA') }}",
                "threads": 2,
            }
            for target, user in users.items()
        }
        (profiles / "profiles.yml").write_text(
            yaml.safe_dump({"postgres_platform": {"target": PRODUCTION, "outputs": outputs}}), encoding=UTF8
        )
        git(repo, "init", "-b", "main")
        git(repo, "config", "user.name", "Branch Test")
        git(repo, "config", "user.email", "branch@example.invalid")
        git(repo, "add", ".")
        git(repo, "commit", "-m", "isolated raw baseline")
        settings = Settings(
            tmp_path,
            profiles,
            180,
            1048576,
            database_url=os.environ["SERVICE_TEST_DATABASE_URL"],
            temp_root=tmp_path / "runtime",
            toolchain_version=uuid4().hex,
            branch_preview_profile_binding_id=PREVIEW,
        )
        runtime = Runtime(settings)
        project = "starrocks-branch-" + suffix
        runtime.jobs.register_project(
            project,
            {
                "projectId": project,
                "remote": str(repo),
                "projectSubdir": ".",
                "profileBindingId": PRODUCTION,
                "schemaName": production,
            },
        )
        executor = RuntimeExecutor(settings, runtime.jobs, runtime.artifacts)

        async def finish(kind, succeeds=True):
            job = runtime.jobs.claim(str(uuid4()), toolchain_version=runtime.toolchain, kinds=[kind])
            assert job is not None
            if succeeds:
                output = await executor.execute(job)
                assert runtime.jobs.finish(
                    job["job_id"], job["lease_token"], output.payload, output_set_id=output.output_set_id
                )
            else:
                with pytest.raises(ExecutionError) as failure:
                    await executor.execute(job)
                runtime.jobs.fail(job["job_id"], job["lease_token"], "TEST_FAILED", failure.value.payload)
            return job

        async def build(branch_id=None, succeeds=True):
            service = PublicationService(runtime, branch_id=branch_id)
            release = service.submit(project, uuid4().hex)
            job = await finish("BUILD_RUN", succeeds)
            assert service.release(project, release["release_id"])["state"] == ("PUBLISHED" if succeeds else "FAILED")
            return service, release, job

        async def total(service, release):
            metric = next(
                item
                for item in service.catalog(project, release["release_id"])["resources"]
                if item["kind"] == "METRIC"
            )
            options = runtime.submit_options(release["run_id"], (metric["name"],))
            if options["status"] != "SUCCEEDED":
                await finish("QUERY_OPTIONS")
            receipt = await asyncio.to_thread(
                service.submit_query,
                project,
                PublishedQueryRequest(
                    releaseId=release["release_id"],
                    mode="QUERY",
                    metricResourceIds=[metric["resourceId"]],
                    idempotencyKey=uuid4().hex,
                ),
                "platform",
            )
            await finish("METRIC_QUERY")
            return Decimal(str(service.get_query(project, receipt["queryId"])["rows"][0][0]))

        prod, baseline, _ = await build()
        assert await total(prod, baseline) == Decimal("31")
        main = BranchStore(runtime.db).production(project)
        branches = [
            BranchService(runtime).create(
                project,
                CreateBranchRequest(
                    name=name,
                    sourceBranchId=main["branch_id"],
                    sourceCommitSha=git(repo, "rev-parse", "HEAD"),
                    idempotencyKey=uuid4().hex,
                ),
            )
            for name in ("dev-a", "dev-b")
        ]
        for branch in branches:
            provision("dbt_dev_" + branch.branch_id.hex, PREVIEW)
        built = []
        for branch, factor in zip(branches, ("* 2", "* 3"), strict=True):
            git(repo, "checkout", branch.git_ref.removeprefix("refs/heads/"))
            (repo / MODEL).write_text(RAW_SQL.format(factor=factor), encoding=UTF8)
            git(repo, "commit", "-am", "change branch model")
            built.append(await build(str(branch.branch_id)))
        assert await total(built[0][0], built[0][1]) == Decimal("62")
        assert await total(built[1][0], built[1][1]) == Decimal("93")
        assert len({production, built[0][2]["schema_name"], built[1][2]["schema_name"]}) == 3
        (repo / "tests").mkdir(exist_ok=True)
        (repo / "tests/fail.sql").write_text("select 1 as failure", encoding=UTF8)
        git(repo, "add", ".")
        git(repo, "commit", "-m", "fail latest preview")
        failed, _, _ = await build(str(branches[1].branch_id), succeeds=False)
        assert failed.publication(project)["activePublication"]["releaseId"] == str(built[1][1]["release_id"])
        assert await total(prod, baseline) == Decimal("31")
        git(repo, "checkout", "main")
        git(repo, "merge", "--no-ff", "dev-a", "-m", "merge approved branch")
        prod, merged, job = await build()
        assert job["profile_binding_id"] == PRODUCTION
        assert merged["request_json"]["commitSha"] == git(repo, "rev-parse", "HEAD")
        assert await total(prod, merged) == Decimal("62")
        # 实际低权限连接分别尝试写 raw 和生产库，拒绝不能仅依赖 SQL 文本检查。
        dev = mysql.connector.connect(host=host, port=port, user=users[PREVIEW], password=passwords[PREVIEW])
        try:
            with dev.cursor() as cursor:
                for statement in (
                    f"INSERT INTO `{raw}`.orders VALUES (3,'2024-01-01',1,'C')",
                    f"CREATE TABLE `{production}`.forbidden (id INT) DISTRIBUTED BY HASH(id) "
                    "PROPERTIES ('replication_num'='1')",
                ):
                    with pytest.raises(mysql.connector.Error):
                        cursor.execute(statement)
        finally:
            dev.close()
        assert Decimal(str(execute(f"SELECT sum(revenue) FROM `{raw}`.orders")[0][0])) == Decimal("31")
    finally:
        if runtime:
            await runtime.close()
        for database in reversed(databases):
            execute(f"DROP DATABASE `{database}` FORCE")
        for user in created_users:
            execute(f"DROP USER '{user}'")
        admin.close()
