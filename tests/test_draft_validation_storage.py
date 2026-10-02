"""真实数据库验证输入与任务原子持久化，不依赖 API 实例内存。"""

import importlib
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from uuid import uuid4

import pytest

from dbt_metricflow_service.draft_validation_models import DraftValidationRequest
from dbt_metricflow_service.storage.artifacts import ArtifactStore
from dbt_metricflow_service.storage.jobs import JobStore, StoreConflict
from tests.test_publication_storage import store as store


def service(runtime):
    spec = importlib.util.find_spec("dbt_metricflow_service.draft_validation")
    assert spec is not None, "durable draft validation admission is not implemented"
    return importlib.import_module(spec.name).DraftValidationService(runtime)


@pytest.fixture
def runtime(store, tmp_path):
    return SimpleNamespace(db=store.db, jobs=JobStore(store.db), artifacts=ArtifactStore(store.db),
                           settings=SimpleNamespace(temp_root=tmp_path, config_version="1",
                                                    command_timeout_seconds=600), toolchain="test")


def setup(runtime):
    project = "draft-" + uuid4().hex
    runtime.jobs.register_project(project, {"remote": "configured-remote", "projectSubdir": ".",
                                            "profileBindingId": "test"})
    request = DraftValidationRequest(baseCommitSha="a" * 40, idempotencyKey="same",
                                    changes=[{"path": "models/new.yml", "operation": "CREATE",
                                              "content": "version: 2\n"}])
    return project, request


def test_concurrent_admission_survives_restart_and_rejects_changed_input(runtime):
    project, request = setup(runtime)
    candidate = service(runtime)
    with ThreadPoolExecutor(max_workers=4) as pool:
        receipts = list(pool.map(lambda _: candidate.submit(project, request), range(4)))
    assert len({item.validation_id for item in receipts}) == 1
    identifier = str(receipts[0].validation_id)
    job = runtime.jobs.get(identifier)
    assert job["input_mode"] == "DURABLE"
    assert "content" not in str(job["request_json"])
    payload = runtime.artifacts.read_file(job["input_set_id"], "changes.json")
    assert DraftValidationRequest.model_validate_json(payload).changes == request.changes
    assert not runtime.artifacts.delete_unreferenced(job["input_set_id"])
    # 不重读已变化的当前配置，未知回执重试仍恢复旧受理身份。
    runtime.jobs.register_project(project, {"remote": "changed"}, config_version="2")
    restored = service(runtime).submit(project, request)
    assert restored.validation_id == receipts[0].validation_id
    changed = request.model_copy(update={"base_commit_sha": "b" * 40})
    with pytest.raises(StoreConflict):
        candidate.submit(project, changed)
    result = service(runtime).get(project, identifier)
    assert result.base_commit_sha == "a" * 40
    assert result.config_version == "1"
    with pytest.raises(KeyError):
        candidate.get("another-project", identifier)


def test_validation_does_not_occupy_writer_or_change_project_pointers(runtime):
    project, request = setup(runtime)
    before = runtime.jobs.project(project)
    service(runtime).submit(project, request)
    after = runtime.jobs.project(project)
    for field in ("busy_job_id", "source_set_id", "current_output_set_id", "active_published_release_id"):
        assert after[field] == before[field]
