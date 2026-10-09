"""服务拥有 Git 输入与候选受理，不依赖平台发布。"""

from types import SimpleNamespace
from uuid import uuid4

import pytest

from dbt_metricflow_service.publications.models import FixedCommitRequest
from dbt_metricflow_service.publications.service import PublicationService
from dbt_metricflow_service.storage.artifacts import ArtifactStore
from dbt_metricflow_service.storage.jobs import JobStore, StoreConflict
from tests.test_platform_bindings import digest, git
from tests.test_platform_bindings import repository as repository
from tests.test_publication_storage import store as store


def test_submit_fixes_input_and_reserves_run_atomically(store, repository, tmp_path):
    project = "publish-" + uuid4().hex
    jobs = JobStore(store.db)
    jobs.register_project(project, binding_config={"projectId": project, "remote": str(repository),
                                                  "projectSubdir": ".", "profileBindingId": "postgres"})
    runtime = SimpleNamespace(db=store.db, jobs=jobs, artifacts=ArtifactStore(store.db), toolchain="publication-test",
                              settings=SimpleNamespace(temp_root=tmp_path, command_timeout_seconds=60, config_version="1"))
    service = PublicationService(runtime)
    key = FixedCommitRequest(commitSha=git(repository, "rev-parse", "HEAD"), idempotencyKey=uuid4().hex)
    first = service.submit(project, key)
    second = service.submit(project, key)
    assert first["release_id"] == second["release_id"]
    run = jobs.get(first["run_id"])
    assert run["request_json"]["commitSha"] == git(repository, "rev-parse", "HEAD")
    assert run["request_json"]["projectDigest"] == digest(repository, git(repository, "rev-parse", "HEAD"))
    assert run["request_json"]["releaseId"] == first["release_id"]
    assert run["status"] == "QUEUED"
    assert run["input_set_id"] is not None
    assert runtime.artifacts.read_file(run["input_set_id"], "models/a.sql") == b"select 1 as value\n"
    (repository / "models/a.sql").write_text("select 2 as value\n", encoding="utf-8")
    git(repository, "commit", "-am", "moved remote")
    with pytest.raises(StoreConflict):
        service.submit(project, key.model_copy(update={"commit_sha": git(repository, "rev-parse", "HEAD")}))
    jobs.register_project(project, binding_config={"remote": "unavailable"}, config_version="2")
    assert service.submit(project, key)["run_id"] == first["run_id"]


def test_concurrent_fixed_commit_requests_create_one_run(store, repository, tmp_path):
    from concurrent.futures import ThreadPoolExecutor

    project = "concurrent-" + uuid4().hex
    jobs = JobStore(store.db)
    jobs.register_project(project, binding_config={"remote": str(repository), "projectSubdir": ".",
                                                  "profileBindingId": "postgres"})
    runtime = SimpleNamespace(db=store.db, jobs=jobs, artifacts=ArtifactStore(store.db), toolchain="test",
                              settings=SimpleNamespace(temp_root=tmp_path, config_version="1", command_timeout_seconds=60))
    service = PublicationService(runtime)
    request = FixedCommitRequest(commitSha=git(repository, "rev-parse", "HEAD"), idempotencyKey=uuid4().hex)
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _: service.submit(project, request), range(2)))
    assert len({row["run_id"] for row in results}) == 1
    assert len(service.releases(project)) == 1


def test_binding_change_during_snapshot_rejects_candidate(store, repository, tmp_path, monkeypatch):
    project = "binding-" + uuid4().hex
    jobs = JobStore(store.db)
    binding = {"remote": str(repository), "projectSubdir": ".", "profileBindingId": "postgres"}
    jobs.register_project(project, binding_config=binding)
    artifacts = ArtifactStore(store.db)
    capture = artifacts.capture

    def changed(*args, **kwargs):
        snapshot = capture(*args, **kwargs)
        jobs.register_project(project, {**binding, "profileBindingId": "other"})
        return snapshot

    monkeypatch.setattr(artifacts, "capture", changed)
    runtime = SimpleNamespace(db=store.db, jobs=jobs, artifacts=artifacts, toolchain="test",
                              settings=SimpleNamespace(temp_root=tmp_path, config_version="1", command_timeout_seconds=60))
    service = PublicationService(runtime)
    with pytest.raises(StoreConflict):
        service.submit(project, FixedCommitRequest(commitSha=git(repository, "rev-parse", "HEAD"),
                                                   idempotencyKey=uuid4().hex))
    assert service.releases(project) == []
