from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from dbt_metricflow_service.platform_runs import PlatformRunCoordinator
from dbt_metricflow_service.platform_store import PlatformJobStore, RunState


def ready_run(tmp_path: Path) -> tuple[PlatformRunCoordinator, object, Path]:
    store = PlatformJobStore(tmp_path / "jobs.sqlite")
    run_id = store.reserve_run("run-key", "fingerprint")
    directory = tmp_path / "artifacts" / str(run_id)
    directory.mkdir(parents=True)
    (directory / "READY").write_text("ready\n", encoding="utf-8")
    store.transition_run(run_id, RunState.READY, directory)
    coordinator = PlatformRunCoordinator(store, {}, tmp_path / "artifacts", tmp_path / "profiles")
    return coordinator, run_id, directory


def test_cleanup_rejects_running_query(tmp_path: Path) -> None:
    coordinator, run_id, _directory = ready_run(tmp_path)
    coordinator.store.reserve_query("query-key", "fingerprint", run_id)
    with pytest.raises(ValueError):
        coordinator.cleanup_run(run_id)
    assert coordinator.store.find_run(run_id).state is RunState.READY


def test_cleanup_refuses_linked_or_outside_path(tmp_path: Path) -> None:
    coordinator, run_id, directory = ready_run(tmp_path)
    linked = tmp_path / "linked"
    try:
        linked.symlink_to(directory, target_is_directory=True)
    except OSError as error:
        pytest.skip(f"Symlink creation is unavailable: {error}")
    coordinator.store.transition_run(run_id, RunState.READY, linked)
    with pytest.raises(ValueError):
        coordinator.cleanup_run(run_id)


def test_cleanup_retry_is_idempotent(tmp_path: Path) -> None:
    coordinator, run_id, _directory = ready_run(tmp_path)
    coordinator.store.transition_run(run_id, RunState.CLEANED)
    coordinator.cleanup_run(run_id)
    coordinator.cleanup_run(run_id)
    assert coordinator.store.find_run(run_id).state is RunState.CLEANED


def test_cleanup_restarts_after_directory_removed(tmp_path: Path) -> None:
    coordinator, run_id, directory = ready_run(tmp_path)
    assert coordinator.store.claim_cleanup(run_id)
    shutil.rmtree(directory)
    restarted = PlatformRunCoordinator(
        PlatformJobStore(tmp_path / "jobs.sqlite"), {}, tmp_path / "artifacts", tmp_path / "profiles"
    )
    restarted.cleanup_run(run_id)
    assert restarted.store.find_run(run_id).state is RunState.CLEANED


def test_cleanup_has_single_claimant(tmp_path: Path) -> None:
    coordinator, run_id, _directory = ready_run(tmp_path)
    assert coordinator.store.claim_cleanup(run_id)
    assert not coordinator.store.claim_cleanup(run_id)
