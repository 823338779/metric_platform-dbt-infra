from dataclasses import replace
from datetime import UTC, datetime

import pytest
import yaml

from dbt_metricflow_service.execution.embedded import create_output, engine_environment
from dbt_metricflow_service.execution.logs import EngineLog
from dbt_metricflow_service.models.execution import ExecutionSnapshot
from dbt_metricflow_service.platform.execution_profile import PROFILE_SECRETS, materialized_profile
from dbt_metricflow_service.settings import Settings


@pytest.mark.parametrize("adapter", ["starrocks", "postgres"])
@pytest.mark.parametrize("password", ["", "local-test-password"])
def test_file_credentials_without_environment(tmp_path, monkeypatch, adapter, password):
    for key in ("DBT_ENV_SECRET_WAREHOUSE_USER", "DBT_ENV_SECRET_WAREHOUSE_PASSWORD"):
        monkeypatch.delenv(key, raising=False)
    config = tmp_path / "service.yaml"
    config.write_text(yaml.safe_dump({
        "EXECUTION_INSTANCES": {"local": {"type": adapter, "hosts": ["localhost"], "port": 9030}},
        "EXECUTION_CREDENTIALS": {"local": {"user": "root", "password": password}},
    }), encoding="utf-8")
    settings = replace(Settings.from_file(config), temp_root=tmp_path)
    snapshot = ExecutionSnapshot.model_validate({
        "environmentId": "00000000-0000-0000-0000-000000000001", "configVersion": 1,
        "connection": {"type": adapter, "instanceId": "local", "host": "localhost", "port": 9030},
        "credentialRef": "local", "catalog": "warehouse", "schema": "analytics",
    })
    with materialized_profile(snapshot, tmp_path, settings) as directory:
        profile = yaml.safe_load((directory / "profiles.yml").read_text(encoding="utf-8"))
        output = next(iter(next(iter(profile.values()))["outputs"].values()))
        assert output["username" if adapter == "starrocks" else "user"] == "root"
        assert output["password"] == password
        # 文件凭据也必须从引擎日志及输出缓冲中脱敏。
        buffer = create_output(1024)
        events = []
        with engine_environment({}, buffer, events.append) as capture:
            capture(EngineLog(event_time=datetime.now(UTC), level="info", message=f"root:{password}"))
        buffer.finish()
        assert "root" not in buffer.decode()
        assert "root" not in events[0].message
        if password:
            assert password not in buffer.decode()
            assert password not in events[0].message
    assert not directory.exists()
    assert PROFILE_SECRETS.get() == ()
