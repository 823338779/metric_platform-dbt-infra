"""验证生产登记与分支级候选隔离，使用独立 PostgreSQL。"""

from uuid import uuid4

import pytest
from sqlalchemy.exc import IntegrityError

from dbt_metricflow_service.storage.branches import BranchStore
from dbt_metricflow_service.storage.jobs import JobStore, StoreConflict
from dbt_metricflow_service.storage.rows import row_dict
from tests.test_publication_storage import store as store

SQL_BRANCH_TABLE = "SELECT to_regclass('runtime_branch') AS name"
SQL_MAIN = "SELECT * FROM runtime_branch WHERE project_id=%s AND mode='PRODUCTION'"
PROJECT_PREFIX = "branch-storage-"
MAIN_REF = "refs/heads/main"
ACTIVE = "ACTIVE"
SQL_PREVIEW = """INSERT INTO runtime_branch(branch_id,project_id,git_ref,mode,status,config_version)
 VALUES(%s,%s,%s,'PREVIEW','ACTIVE','1')"""
SQL_BAD_POINTER = "UPDATE runtime_branch SET active_release_id=%s WHERE branch_id=%s"
REF_PREFIX = "refs/heads/"
KEY = "same-key"
REQUEST = {"commitSha": "a" * 40, "configVersion": "1"}
BUILD = "BUILD_RUN"


def preview(store, project):
    # 生命周期由后续服务负责；存储测试只准备合法的 ACTIVE 分支。
    identifier = str(uuid4())
    with store.db.transaction() as connection:
        connection.exec_driver_sql(SQL_PREVIEW, (identifier, project, REF_PREFIX + uuid4().hex))
    return identifier


def test_new_project_has_one_production_branch(store):
    # 重复登记不得更换生产分支身份。
    project = PROJECT_PREFIX + uuid4().hex
    jobs = JobStore(store.db)
    jobs.register_project(project)
    with store.db.transaction() as connection:
        sql_result = connection.exec_driver_sql(SQL_BRANCH_TABLE)
        assert row_dict(sql_result)["name"] is not None, "项目注册必须准备独立分支存储"
        sql_result = connection.exec_driver_sql(SQL_MAIN, (project,))
        original = [dict(row) for row in sql_result.mappings()]
    jobs.register_project(project)
    with store.db.transaction() as connection:
        sql_result = connection.exec_driver_sql(SQL_MAIN, (project,))
        current = [dict(row) for row in sql_result.mappings()]
    assert len(current) == len(original) == 1
    assert current[0]["branch_id"] == original[0]["branch_id"]
    assert current[0]["git_ref"] == MAIN_REF
    assert current[0]["status"] == ACTIVE


def test_branch_sequence_and_key_are_independent(store):
    # 同名幂等键在两个分支独立，同一分支内仍严格比较输入。
    project = PROJECT_PREFIX + uuid4().hex
    JobStore(store.db).register_project(project)
    branch_a, branch_b = preview(store, project), preview(store, project)
    a = store.create_candidate(project, REQUEST, KEY, branch_id=branch_a)
    b = store.create_candidate(project, REQUEST, KEY, branch_id=branch_b)
    assert a["sequence"] == b["sequence"] == 1
    assert a["release_id"] != b["release_id"]
    assert store.create_candidate(project, REQUEST, KEY, branch_id=branch_a)["release_id"] == a["release_id"]
    with pytest.raises(StoreConflict):
        store.create_candidate(project, {**REQUEST, "commitSha": "b" * 40}, KEY, branch_id=branch_a)
    assert BranchStore(store.db).get(project, branch_a)["latest_release_id"] == a["release_id"]
    assert BranchStore(store.db).production(project)["publication_sequence"] == 0


def test_cross_project_and_branch_references_are_rejected(store):
    # 不同项目不能使用别人的分支；同项目也不能指向另一分支的发布。
    project, other = PROJECT_PREFIX + uuid4().hex, PROJECT_PREFIX + uuid4().hex
    jobs = JobStore(store.db)
    jobs.register_project(project)
    jobs.register_project(other)
    branch_a, branch_b = preview(store, project), preview(store, project)
    with pytest.raises(KeyError):
        store.create_candidate(other, REQUEST, KEY, branch_id=branch_a)
    release = store.create_candidate(project, REQUEST, KEY, branch_id=branch_a)
    with pytest.raises(IntegrityError), store.db.transaction() as connection:
        connection.exec_driver_sql(SQL_BAD_POINTER, (release["release_id"], branch_b))


def test_publication_job_inherits_release_branch(store):
    # 发布任务必须持久化同一分支身份，通用任务仍允许空分支。
    project = PROJECT_PREFIX + uuid4().hex
    jobs = JobStore(store.db)
    jobs.register_project(project)
    branch = preview(store, project)
    release = store.create_candidate(project, REQUEST, KEY, branch_id=branch)
    job = jobs.reserve(BUILD, project, {**REQUEST, "releaseId": release["release_id"]})
    assert job["branch_id"] == branch
    assert jobs.reserve(BUILD, project, {})["branch_id"] is None
