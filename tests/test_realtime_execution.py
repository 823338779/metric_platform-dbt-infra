"""真实进程内执行边界：查询不创建数据库记录，锁和临时目录及时释放。"""

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace
from uuid import uuid4

import pytest

from dbt_metricflow_service.application.errors import ServiceError
from dbt_metricflow_service.runtime.service import Runtime
from dbt_metricflow_service.settings import Settings
from tests.test_execution_profiles import secrets as secrets
from tests.test_execution_profiles import settings as infrastructure
from tests.test_execution_snapshot_storage import snapshot
from tests.test_publication_storage import store as store


@pytest.fixture
def realtime(tmp_path, secrets):
    runtime = Runtime.__new__(Runtime)
    runtime.settings = Settings(
        tmp_path, 5, 64, temp_root=tmp_path / "queries", max_result_bytes=1024, **vars(infrastructure())
    )
    build = {
        "build_id": str(uuid4()),
        "run_id": str(uuid4()),
        "output_set_id": str(uuid4()),
        "config_snapshot": snapshot().model_dump(mode="json", by_alias=True),
    }
    events = []

    @contextmanager
    def query_run(run_id):
        assert run_id == build["run_id"]
        events.append("lock")
        try:
            yield {"project_id": "test", "schema_name": "run_test", "profile_binding_id": "test"}
        finally:
            events.append("unlock")

    def materialize(set_id, directory):
        assert set_id == build["output_set_id"]
        assert events[-1] == "lock"
        (directory / "dbt_project.yml").write_text("name: test\n", encoding="utf-8")

    # 故意不提供 reserve/claim/finish/result 等持久化方法。
    runtime.jobs = SimpleNamespace(query_run=query_run)
    runtime.artifacts = SimpleNamespace(materialize=materialize)
    return runtime, build, events


def test_repeated_calls_execute_in_memory_and_remove_workspaces(realtime, monkeypatch):
    import dbt_metricflow_service.platform.metricflow as engine

    runtime, build, events = realtime
    calls, paths = [], []
    payload = {"mode": "OPTIONS", "metrics": ["orders"]}

    def execute(project, profiles, data):
        assert data is payload
        assert events[-1] == "lock"
        assert (project / "dbt_project.yml").is_file()
        paths.append(project)
        calls.append(data)
        return {"rows": [[len(calls)]]}

    monkeypatch.setattr(engine, "execute_programmatic", execute)
    assert runtime.execute_query(build, payload) == {"rows": [[1]]}
    assert runtime.execute_query(build, payload) == {"rows": [[2]]}
    assert paths[0] != paths[1]
    assert all(not path.exists() for path in paths)
    assert events == ["lock", "unlock", "lock", "unlock"]


@pytest.mark.parametrize(
    "failure,code,status",
    [
        ("invalid", "INVALID_QUERY", 422),
        ("connection", "COMMAND_FAILED", 503),
        ("timeout", "COMMAND_TIMEOUT", 504),
    ],
)
def test_errors_release_lock_and_do_not_publish_late_results(realtime, monkeypatch, failure, code, status):
    import time

    import dbt_metricflow_service.platform.metricflow as engine

    runtime, build, events = realtime
    paths = []
    if failure == "timeout":
        runtime.settings = replace(runtime.settings, command_timeout_seconds=0.01)

    def execute(project, profiles, data):
        paths.append(project)
        if failure == "invalid":
            raise engine.InvalidDimensionsRequest("private-details")
        if failure == "connection":
            raise ConnectionError("private-details")
        time.sleep(0.04)
        assert events == ["lock"]
        assert project.exists()
        return {"rows": [[123]]}

    monkeypatch.setattr(engine, "execute_programmatic", execute)
    with pytest.raises(ServiceError) as caught:
        runtime.execute_query(build, {"mode": "OPTIONS", "metrics": ["orders"]})
    assert caught.value.status == status
    assert caught.value.error.code == code
    assert "private-details" not in str(caught.value)
    assert events == ["lock", "unlock"]
    assert all(not path.exists() for path in paths)


@pytest.mark.parametrize(
    "value,limit,error_code",
    [
        ({"x": "中"}, 12, None),
        ({"x": "中"}, 11, "RESULT_TOO_LARGE"),
        ({}, 2, None),
        ({}, 1, "RESULT_TOO_LARGE"),
        ([], 1024, "RESULT_INVALID"),
        (None, 1024, "RESULT_INVALID"),
    ],
)
def test_realtime_result_size_and_shape(realtime, monkeypatch, value, limit, error_code):
    import dbt_metricflow_service.platform.metricflow as engine

    runtime, build, events = realtime
    runtime.settings = replace(runtime.settings, max_result_bytes=limit)
    monkeypatch.setattr(engine, "execute_programmatic", lambda *args: value)
    if error_code is None:
        assert runtime.execute_query(build, {"mode": "OPTIONS"}) is value
    else:
        with pytest.raises(ServiceError) as caught:
            runtime.execute_query(build, {"mode": "OPTIONS"})
        assert caught.value.error.code == error_code
    assert events == ["lock", "unlock"]


def test_waiting_queries_do_not_hold_cleanup_connections(realtime, monkeypatch):
    import time

    import dbt_metricflow_service.platform.metricflow as engine

    runtime, build, events = realtime

    def execute(*args):
        time.sleep(0.01)
        return {"rows": [[1]]}

    monkeypatch.setattr(engine, "execute_programmatic", execute)
    with ThreadPoolExecutor(max_workers=9) as pool:
        results = list(pool.map(lambda _: runtime.execute_query(build, {"mode": "OPTIONS"}), range(9)))
    assert results == [{"rows": [[1]]}] * 9
    assert events == ["lock", "unlock"] * 9


def test_real_database_query_path_only_reads_without_persisting_records(store, tmp_path, monkeypatch, secrets):
    import dbt_metricflow_service.platform.metricflow as engine
    from tests.test_application_reads import reads
    from tests.test_catalog_queries import ready_build

    builds, view, jobs, artifacts = ready_build(store, tmp_path)
    build = builds.store.get(view.build_id)
    runtime = Runtime.__new__(Runtime)
    runtime.settings = Settings(tmp_path, 5, 64, temp_root=tmp_path / "queries", **vars(infrastructure()))
    runtime.jobs, runtime.artifacts = jobs, artifacts
    monkeypatch.setattr(engine, "execute_programmatic", lambda *args: {"rows": [[1]]})
    with reads(store.db) as statements:
        assert runtime.execute_query(build, {"mode": "OPTIONS", "metrics": ["revenue"]}) == {"rows": [[1]]}
        assert runtime.execute_query(build, {"mode": "QUERY", "request": {}}) == {"rows": [[1]]}
    assert statements
    assert all(statement.lstrip().upper().startswith("SELECT") for statement in statements)
    with store.db.transaction() as connection:
        assert (
            connection.exec_driver_sql(
                "SELECT count(*) FROM runtime_job WHERE parent_run_id=%s",
                (build["run_id"],),
            ).scalar_one()
            == 0
        )
