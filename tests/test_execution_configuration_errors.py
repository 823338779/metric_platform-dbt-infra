"""缺失运行凭据属于服务配置故障，查询与 worker 返回相同稳定错误码。"""

import asyncio
from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

import pytest

from dbt_metricflow_service.application.errors import ServiceError
from dbt_metricflow_service.platform.execution_profile import materialized_profile
from dbt_metricflow_service.runtime.service import Runtime
from dbt_metricflow_service.runtime.worker import Worker
from tests.test_execution_profiles import settings
from tests.test_execution_snapshot_storage import snapshot


def unavailable_settings(tmp_path):
    config = settings()
    config.execution_credentials = {}
    config.temp_root = tmp_path
    config.command_timeout_seconds = 30
    config.heartbeat_seconds = 60
    return config


def test_query_missing_credentials_is_retryable_service_failure(tmp_path, monkeypatch):
    runtime = object.__new__(Runtime)
    runtime.settings = unavailable_settings(tmp_path)
    runtime.artifacts = None
    runtime.jobs = SimpleNamespace(query_run=lambda _: nullcontext({}))
    monkeypatch.setattr("dbt_metricflow_service.runtime.service.materialized_workspace",
                        lambda *args: nullcontext(tmp_path))
    build = {"build_id": str(uuid4()), "run_id": "run", "output_set_id": "output", "protocol_version": 4,
             "config_snapshot": snapshot().model_dump(mode="json", by_alias=True)}
    with pytest.raises(ServiceError) as raised:
        runtime.execute_query(build, {})
    assert raised.value.status == 503
    assert raised.value.error.code == "CREDENTIAL_UNAVAILABLE"
    assert raised.value.error.retryable


def test_worker_missing_credentials_records_specific_failure(tmp_path):
    config = unavailable_settings(tmp_path)
    jobs = SimpleNamespace(fail=Mock(), external_started=Mock(return_value=False))
    worker = Worker(SimpleNamespace(settings=config, jobs=jobs, artifacts=None))

    async def execute(_):
        with materialized_profile(snapshot(), tmp_path, config):
            pytest.fail("missing credential was accepted")

    worker.executor = SimpleNamespace(execute=execute)
    asyncio.run(worker._execute({"job_id": "job", "lease_token": "lease", "attempt_id": "attempt"}))
    assert jobs.fail.call_args.args[2] == "CREDENTIAL_UNAVAILABLE"
    assert jobs.fail.call_args.kwargs["stopped"] is True
