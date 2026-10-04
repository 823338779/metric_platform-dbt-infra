"""在临时 schema 准备 v3 历史，验证真实增量迁移不改写身份和产物。"""

import hashlib
from contextlib import contextmanager
from uuid import uuid4

from psycopg2 import sql

from dbt_metricflow_service.storage.branches import BranchStore
from dbt_metricflow_service.storage.jobs import JobStore
from dbt_metricflow_service.storage.postgres import MIGRATIONS, Database
from tests.test_publication_storage import store as store
from tests.test_publication_transaction import prepared

CREATE_SCHEMA = "CREATE SCHEMA {}"
DROP_SCHEMA = "DROP SCHEMA {} CASCADE"
SEARCH_PATH = "SET LOCAL search_path TO {}"
SCHEMA_PREFIX = "branch_migration_"
UTF8 = "utf-8"
PROJECT = "historical-project"
CONTENT = b'{"schemaVersion":1,"resources":[]}\n'
SQL_PROJECT = "INSERT INTO runtime_project(project_id,publication_sequence) VALUES(%s,1)"
SQL_JOB = """INSERT INTO runtime_job(job_id,project_id,kind,parent_run_id,request_fingerprint,
 config_version,toolchain_version,deadline_at) VALUES(%s,%s,%s,%s,'fingerprint','1','legacy',now())"""
SQL_ARTIFACT = "INSERT INTO runtime_artifact_set(set_id,project_id,kind) VALUES(%s,%s,'EXECUTION')"
SQL_FILE = """INSERT INTO runtime_artifact_file(set_id,relative_path,content,codec,raw_sha256,raw_size,stored_size)
 VALUES(%s,'target/published_catalog.json',%s,'raw',%s,%s,%s)"""
SQL_SEAL = "UPDATE runtime_artifact_set SET state='SEALED' WHERE set_id=%s"
SQL_RELEASE = """INSERT INTO runtime_release(release_id,project_id,sequence,idempotency_key,request_json,
 run_id,artifact_set_id,state,published_at,catalog_digest) VALUES(%s,%s,1,'legacy','{}',%s,%s,'PUBLISHED',now(),%s)"""
SQL_POINTER = "UPDATE runtime_project SET active_published_release_id=%s WHERE project_id=%s"
SQL_RELEASES = "SELECT * FROM runtime_release"
SQL_JOBS = "SELECT * FROM runtime_job ORDER BY job_id"
SQL_ARTIFACTS = "SELECT * FROM runtime_artifact_file"
BUILD = "BUILD_RUN"
QUERY = "METRIC_QUERY"


class ScopedDatabase:
    """只改变测试事务 search_path，复用真实迁移实现和连接池。"""

    def __init__(self, db, schema):
        # schema 是本次测试生成的随机标识符，使用 SQL Identifier 转义。
        self.db = db
        self.schema = schema

    @contextmanager
    def transaction(self):
        # 所有准备、迁移与断言都在同一个独立命名空间内。
        with self.db.transaction() as cursor:
            cursor.execute(sql.SQL(SEARCH_PATH).format(sql.Identifier(self.schema)))
            yield cursor

    migrate = Database.migrate


def test_upgrade_preserves_production_identities(store):
    # 原始历史不依赖当前生产 Python API，以免把新行为混进 v3 fixture。
    schema = SCHEMA_PREFIX + uuid4().hex
    scoped = ScopedDatabase(store.db, schema)
    run, query, release, artifact = (str(uuid4()) for _ in range(4))
    digest = hashlib.sha256(CONTENT).hexdigest()
    with store.db.transaction() as cursor:
        cursor.execute(sql.SQL(CREATE_SCHEMA).format(sql.Identifier(schema)))
    try:
        with scoped.transaction() as cursor:
            for migration in MIGRATIONS[:3]:
                cursor.execute(migration.read_text(encoding=UTF8))
            cursor.execute(SQL_PROJECT, (PROJECT,))
            cursor.execute(SQL_JOB, (run, PROJECT, BUILD, None))
            cursor.execute(SQL_JOB, (query, PROJECT, QUERY, run))
            cursor.execute(SQL_ARTIFACT, (artifact, PROJECT))
            cursor.execute(SQL_FILE, (artifact, CONTENT, digest, len(CONTENT), len(CONTENT)))
            cursor.execute(SQL_SEAL, (artifact,))
            cursor.execute(SQL_RELEASE, (release, PROJECT, run, artifact, digest))
            cursor.execute(SQL_POINTER, (release, PROJECT))
            cursor.execute(SQL_RELEASES)
            before_release = dict(cursor.fetchone())
            cursor.execute(SQL_JOBS)
            before_jobs = [dict(row) for row in cursor.fetchall()]
            cursor.execute(SQL_ARTIFACTS)
            before_files = cursor.fetchall()
        scoped.migrate()
        branch = BranchStore(scoped).production(PROJECT)
        scoped.migrate()
        assert BranchStore(scoped).list(PROJECT) == [branch]
        assert branch["active_release_id"] == branch["latest_release_id"] == release
        assert branch["publication_sequence"] == 1
        with scoped.transaction() as cursor:
            cursor.execute(SQL_RELEASES)
            after_release = dict(cursor.fetchone())
            assert after_release.pop("branch_id") == branch["branch_id"]
            assert after_release == before_release
            cursor.execute(SQL_JOBS)
            after_jobs = [dict(row) for row in cursor.fetchall()]
            for row in after_jobs:
                assert row.pop("branch_id") == branch["branch_id"]
            assert after_jobs == before_jobs
            cursor.execute(SQL_ARTIFACTS)
            assert cursor.fetchall() == before_files
    finally:
        # 只移除本测试创建的随机 schema，不清理共享测试表。
        with store.db.transaction() as cursor:
            cursor.execute(sql.SQL(DROP_SCHEMA).format(sql.Identifier(schema)))


def test_legacy_project_facade_reads_production_pointer(store, tmp_path):
    # 旧项目读取门面也必须从 main 派生，避免调用方看到过期指针。
    jobs, job, release, output, _ = prepared(store, tmp_path)
    assert jobs.finish(job["job_id"], job["lease_token"], output_set_id=output)
    project = JobStore(store.db).project(job["project_id"])
    assert project["active_published_release_id"] == release["release_id"]
    assert project["publication_sequence"] == 1
