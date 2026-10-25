from contextlib import contextmanager
from copy import copy
from types import SimpleNamespace

import pytest

from dbt_metricflow_service.application.deployments import DeploymentService
from dbt_metricflow_service.models.deployments import DeploymentKey
from dbt_metricflow_service.storage.deployments import DeploymentStore
from dbt_metricflow_service.storage.entities import DeploymentTarget


@pytest.fixture
def observation_store(monkeypatch):
    target = SimpleNamespace(version=3, active_build_id=None, observed_head_sha="b" * 40,
                             source_state="UNDEPLOYED", desired_generation=1)
    attempt = SimpleNamespace(deployment_status="PENDING", build_id="build-b", version=1)
    build = SimpleNamespace(build_id="build-b", build_status="SUCCEEDED", output_set_id="artifact",
                            commit_sha="b" * 40)

    class Session:
        def get(self, model, key, **kwargs):
            return target if model is DeploymentTarget else attempt

        def scalar(self, statement):
            return build

        def execute(self, statement):
            return None

        def flush(self):
            pass

    class Database:
        @contextmanager
        def fact_session(self):
            yield Session()

    monkeypatch.setattr("dbt_metricflow_service.storage.deployments.entity_record", lambda row, model: row)
    key = DeploymentKey(repository="repo", environment="00000000-0000-0000-0000-000000000001",
                        branchName="main")
    return DeploymentStore(Database()), key, target, attempt


@pytest.mark.parametrize("head,state", [("c" * 40, "UNDEPLOYED"), (None, "UNKNOWN"), (None, "MISSING")])
def test_superseded_observation_cannot_deploy(observation_store, head, state):
    store, key, target, attempt = observation_store
    store.observe(key, head, state, expected_version=3, expected_active_build_id=None)
    store.settle(key, 1, "DEPLOYED", expected_version=3, head="b" * 40)
    assert target.active_build_id is None
    assert attempt.deployment_status != "DEPLOYED"


def test_same_head_new_observation_invalidates_old_token_without_staling_intent(observation_store):
    store, key, target, attempt = observation_store
    store.observe(key, "b" * 40, "UNDEPLOYED", expected_version=3, expected_active_build_id=None)
    assert target.version == 4
    store.settle(key, 1, "DEPLOYED", expected_version=3, head="b" * 40)
    assert attempt.deployment_status == "PENDING"
    store.settle(key, 1, "DEPLOYED", expected_version=4, head="b" * 40)
    assert attempt.deployment_status == "DEPLOYED"


def test_old_reader_cannot_overwrite_new_observation(observation_store):
    store, key, target, _ = observation_store
    store.observe(key, "c" * 40, "UNDEPLOYED", expected_version=3, expected_active_build_id=None)
    result = store.observe(key, "b" * 40, "UNDEPLOYED", expected_version=3, expected_active_build_id=None)
    assert result is None
    assert target.observed_head_sha == "c" * 40


@pytest.mark.parametrize("head,state", [("b" * 40, "UNDEPLOYED"), (None, "UNKNOWN")])
def test_superseded_observation_cannot_stale_intent(observation_store, head, state):
    store, key, target, attempt = observation_store
    store.observe(key, "c" * 40, "UNDEPLOYED", expected_version=3, expected_active_build_id=None)
    store.observe(key, head, state, expected_version=4, expected_active_build_id=None)
    store.settle(key, 1, "STALE", "SOURCE_HEAD_CHANGED", expected_version=4, head="c" * 40)
    assert attempt.deployment_status == "PENDING"
    assert target.observed_head_sha == head


def test_current_mismatched_head_stales_intent(observation_store):
    store, key, _, attempt = observation_store
    store.observe(key, "c" * 40, "UNDEPLOYED", expected_version=3, expected_active_build_id=None)
    store.settle(key, 1, "STALE", "SOURCE_HEAD_CHANGED", expected_version=4, head="c" * 40)
    assert attempt.deployment_status == "STALE"
    assert attempt.reason == "SOURCE_HEAD_CHANGED"


@pytest.mark.parametrize("head,state", [("b" * 40, "UNDEPLOYED"), (None, "UNKNOWN")])
def test_reconcile_carries_observation_into_stale_decision(observation_store, monkeypatch, head, state):
    store, key, target, attempt = observation_store
    # Real database records are immutable snapshots; reproduce that isolation for this session stub.
    monkeypatch.setattr("dbt_metricflow_service.storage.deployments.entity_record", lambda row, model: copy(row))
    monkeypatch.setattr("dbt_metricflow_service.application.deployments.attempt_view", lambda row: row)
    monkeypatch.setattr(store, "current", lambda _: copy(target))
    monkeypatch.setattr(store, "attempt", lambda *args: copy(attempt))
    settle = store.settle

    def settle_after_new_observation(*args, **kwargs):
        store.observe(key, head, state, expected_version=target.version, expected_active_build_id=None)
        return settle(*args, **kwargs)

    monkeypatch.setattr(store, "settle", settle_after_new_observation)
    service = DeploymentService(
        store, lambda *args: "c" * 40,
        builds=SimpleNamespace(get=lambda _: SimpleNamespace(build_status="SUCCEEDED", commit_sha="b" * 40)),
        catalogs=SimpleNamespace(read_build=lambda _: None),
    )
    assert service.reconcile(key, 1).deployment_status == "PENDING"
    assert target.observed_head_sha == head
