from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from dbt_metricflow_service.platform.store import PlatformJobStore, RunState


def test_same_key_returns_same_run_across_reopen(tmp_path: Path) -> None:
    path = tmp_path / "jobs.sqlite"
    first = PlatformJobStore(path).reserve_run("key", "fingerprint")
    second = PlatformJobStore(path).reserve_run("key", "fingerprint")
    assert first == second
    assert PlatformJobStore(path).find_run(first).state is RunState.QUEUED


def test_key_with_different_payload_conflicts(tmp_path: Path) -> None:
    store = PlatformJobStore(tmp_path / "jobs.sqlite")
    store.reserve_run("key", "first")
    with pytest.raises(ValueError):
        store.reserve_run("key", "second")


def test_restart_reconciles_nonterminal_run_without_ready(tmp_path: Path) -> None:
    path = tmp_path / "jobs.sqlite"
    store = PlatformJobStore(path)
    run_id = store.reserve_run("key", "fingerprint")
    store.transition_run(run_id, RunState.RUNNING)

    reopened = PlatformJobStore(path)
    assert reopened.find_run(run_id).state is RunState.FAILED


def test_concurrent_reserve_creates_one_run(tmp_path: Path) -> None:
    path = tmp_path / "jobs.sqlite"

    def reserve(_: int) -> object:
        return PlatformJobStore(path).reserve_run("key", "fingerprint")

    with ThreadPoolExecutor(max_workers=8) as executor:
        ids = list(executor.map(reserve, range(16)))
    assert len(set(ids)) == 1
