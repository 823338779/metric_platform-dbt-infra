"""删除阶段日志后，执行事实、详细日志和事务串行约束保持有效。"""

from concurrent.futures import ThreadPoolExecutor, TimeoutError
from datetime import UTC, datetime
from pathlib import Path
from threading import Event
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import inspect

from dbt_metricflow_service.execution.logs import EngineLog
from dbt_metricflow_service.storage.execution_logs import ExecutionLogStore
from dbt_metricflow_service.storage.jobs import JobStore
from tests.schema_helpers import isolated_database
from tests.test_build_acceptance import CALLER, TOOLCHAIN, request, service


def test_remove_legacy_stage_logs_preserves_execution_and_detailed_logs():
    with isolated_database() as db:
        db.initialize()
        legacy = Path("tests/fixtures/legacy_build_stage_logs.sql").read_text(encoding="utf-8")
        with db.transaction() as connection:
            connection.exec_driver_sql(legacy)
        app = service(SimpleNamespace(db=db))
        build = app.submit_snapshot(request(), CALLER)
        jobs = JobStore(db)
        job = jobs.claim(str(uuid4()), toolchain_version=TOOLCHAIN)
        jobs.phase(job.job_id, job.lease_token, "PARSING")
        logs = ExecutionLogStore(db)
        logs.append(job, "PARSING", "parse", [EngineLog(datetime.now(UTC), "info", "real dbt log")])
        with db.transaction() as connection:
            assert connection.exec_driver_sql("SELECT count(*) FROM engine_change").scalar_one() > 0
        cleanup = Path("src/dbt_metricflow_service/storage/remove_build_stage_logs.sql").read_text(encoding="utf-8")
        for _ in range(2):
            with db.transaction() as connection:
                connection.exec_driver_sql(cleanup)
        db.check()
        with db.transaction() as connection:
            assert not {"engine_change", "engine_change_counter"} & set(inspect(connection).get_table_names())
        assert jobs.get(job.job_id).phase == "PARSING"
        assert app.snapshot_view(build.build_id).phase == "PARSING"
        assert [row.message for row in logs.list(str(build.build_id))[0]] == ["real dbt log"]
        assert jobs.fail(job.job_id, job.lease_token, "COMMAND_FAILED")
        assert app.snapshot_view(build.build_id).error.code == "COMMAND_FAILED"


@pytest.mark.parametrize("rollback", [False, True])
def test_fact_transactions_serialize_and_release_after_commit_or_rollback(rollback):
    with isolated_database() as db:
        db.initialize()
        started = Event()

        def contender():
            started.set()
            with db.fact_transaction() as connection:
                return connection.exec_driver_sql("SELECT 1").scalar_one()

        with ThreadPoolExecutor(max_workers=1) as pool:
            try:
                with db.fact_session():
                    waiting = pool.submit(contender)
                    assert started.wait(3)
                    with pytest.raises(TimeoutError):
                        waiting.result(timeout=0.1)
                    if rollback:
                        raise RuntimeError("rollback probe")
            except RuntimeError:
                if not rollback:
                    raise
            assert waiting.result(timeout=3) == 1
