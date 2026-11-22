from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from dbt_metricflow_service.storage.builds import BuildStore


def store_with_prior(monkeypatch, prior):
    class Session:
        def execute(self, statement):
            pass

        def scalar(self, statement):
            return prior

    class Database:
        @contextmanager
        def fact_session(self):
            yield Session()

    monkeypatch.setattr("dbt_metricflow_service.storage.builds.entity_record", lambda row, model: row)
    return BuildStore(Database())


def test_idempotent_retry_does_not_validate_current_target(monkeypatch, build_request):
    prior = SimpleNamespace(build_id="existing")
    store = store_with_prior(monkeypatch, prior)
    validator = Mock(side_effect=ValueError("instance removed"))
    result = store.accept(build_request, "service", "toolchain", validate_new=validator)
    assert result.build_id == "existing"
    validator.assert_not_called()


def test_new_request_validates_before_any_record_is_created(monkeypatch, build_request):
    store = store_with_prior(monkeypatch, None)
    validator = Mock(side_effect=ValueError("instance removed"))
    with pytest.raises(ValueError, match="instance removed"):
        store.accept(build_request, "service", "toolchain", validate_new=validator)
    validator.assert_called_once_with(build_request.execution)
