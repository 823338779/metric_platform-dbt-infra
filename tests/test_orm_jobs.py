"""任务结果存在性查询通过短 ORM Session 执行。"""

from contextlib import contextmanager
from uuid import uuid4

from sqlalchemy import Column, MetaData, Table, Uuid, create_engine
from sqlalchemy.orm import Session

from dbt_metricflow_service.storage.jobs import JobStore
from tests.test_runtime_storage import store as store


def test_result_exists_uses_session_without_loading_payload():
    # 仅建存在性查询需要的字段，避免测试依赖结果正文的加载。
    engine = create_engine("sqlite://")
    results = Table("runtime_job_result", MetaData(), Column("job_id", Uuid(as_uuid=False), primary_key=True))
    results.metadata.create_all(engine)
    job_id = str(uuid4())
    with engine.begin() as connection:
        connection.execute(results.insert().values(job_id=job_id))

    class SessionDatabase:
        @contextmanager
        def session(self):
            with Session(engine) as session:
                yield session

    jobs = JobStore(SessionDatabase())
    assert jobs.result_exists(job_id)
    assert not jobs.result_exists(str(uuid4()))
    engine.dispose()


def test_finish_without_input_does_not_replace_project_output(store, tmp_path):
    from dbt_metricflow_service.storage.artifacts import ArtifactStore

    # SQL 的 NULL 比较不能在迁移后变成“无输入也匹配项目源码”。
    project_id = str(uuid4())
    version = str(uuid4())
    store.register_project(project_id)
    job = store.reserve("DBT_COMMAND", project_id, {}, toolchain_version=version)
    attempt = store.claim(str(uuid4()), toolchain_version=version)
    (tmp_path / "dbt_project.yml").write_text("name: no_source\n", encoding="utf-8")
    artifacts = ArtifactStore(store.db)
    output = artifacts.capture(project_id, tmp_path, producer_attempt_id=attempt["attempt_id"], kind="EXECUTION")

    assert store.finish(job["job_id"], attempt["lease_token"], output_set_id=output)
    assert store.get(job["job_id"])["output_set_id"] == output
    assert store.project(project_id)["current_output_set_id"] is None
