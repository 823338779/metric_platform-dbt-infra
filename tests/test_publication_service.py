"""服务拥有 Git 输入与候选受理，不依赖平台发布。"""

from types import SimpleNamespace
from uuid import uuid4

from dbt_metricflow_service.publication import PublicationService
from dbt_metricflow_service.storage.jobs import JobStore
from tests.test_platform_bindings import digest, git
from tests.test_platform_bindings import repository as repository
from tests.test_publication_storage import store as store


def test_submit_fixes_input_and_reserves_run_atomically(store, repository, tmp_path):
    project = "publish-" + uuid4().hex
    jobs = JobStore(store.db)
    jobs.register_project(project, binding_config={"projectId": project, "remote": str(repository),
                                                  "projectSubdir": ".", "profileBindingId": "postgres"})
    runtime = SimpleNamespace(db=store.db, jobs=jobs, toolchain="publication-test",
                              settings=SimpleNamespace(temp_root=tmp_path, command_timeout_seconds=60))
    service = PublicationService(runtime)
    key = uuid4().hex
    first = service.submit(project, key)
    second = service.submit(project, key)
    assert first["release_id"] == second["release_id"]
    run = jobs.get(first["run_id"])
    assert run["request_json"]["commitSha"] == git(repository, "rev-parse", "HEAD")
    assert run["request_json"]["projectDigest"] == digest(repository, git(repository, "rev-parse", "HEAD"))
    assert run["request_json"]["releaseId"] == first["release_id"]
    assert run["status"] == "QUEUED"
