"""最终审查回归：配置升级、删除身份、固定基线与失败证明。"""

import hashlib
import hmac
import json
from uuid import uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient

from dbt_metricflow_service.branch_events import create_branch_event_router
from dbt_metricflow_service.branch_models import RegisterBranchRequest
from dbt_metricflow_service.storage.artifacts import ArtifactStore
from tests.test_branch_lifecycle import context as context
from tests.test_platform_bindings import git
from tests.test_platform_bindings import repository as repository
from tests.test_publication_storage import store as store

SECRET = "review-event-secret"
PATH = "/internal/git-branch-events"
UTF8 = "utf-8"


def test_config_upgrade_refreshes_preview_but_preserves_namespace(context):
    # 新配置只能影响后续执行，分支身份和物理命名空间保持固定。
    service, project, request = context
    branch = service.create(project, request)
    before = service.store.get(project, str(branch.branch_id))
    config = {**service.runtime.jobs.project(project)["binding_config"], "businessTimezone": "Asia/Shanghai"}
    service.runtime.jobs.register_project(project, config, "2")
    after = service.store.get(project, str(branch.branch_id))
    assert after["config_version"] == "2"
    assert after["binding_config"]["businessTimezone"] == "Asia/Shanghai"
    assert after["binding_config"]["schemaName"] == before["binding_config"]["schemaName"]
    assert after["binding_config"]["profileBindingId"] == before["binding_config"]["profileBindingId"]


def test_delete_event_fences_old_identity_even_if_ref_already_recreated(context, repository):
    # 服务未扫描时外部同名重建；已验证的删除事件仍须关闭旧实例。
    service, project, request = context
    branch = service.create(project, request)
    runtime = service.runtime
    runtime.settings.branch_events_enabled = True
    runtime.settings.branch_event_secret = SECRET
    app = FastAPI()
    app.include_router(create_branch_event_router(runtime))
    body = json.dumps({"ref": request.name, "ref_type": "branch",
                       "repository": {"clone_url": str(repository)}}).encode(UTF8)
    headers = {"X-Forgejo-Event": "delete", "X-Forgejo-Delivery": uuid4().hex, "X-Forgejo-Signature":
               hmac.new(SECRET.encode(UTF8), body, hashlib.sha256).hexdigest()}
    with TestClient(app) as client:
        assert client.post(PATH, content=body, headers=headers).status_code == 202
    assert service.get(project, str(branch.branch_id)).status == "DELETED"
    replacement = service.register(project, RegisterBranchRequest(
        name=request.name, base_commit_sha=request.source_commit_sha, idempotency_key=uuid4().hex))
    assert replacement.branch_id != branch.branch_id
    with TestClient(app) as client:
        assert client.post(PATH, content=body, headers=headers).status_code == 204
    assert service.get(project, str(replacement.branch_id)).status == "ACTIVE"


def test_branch_baseline_snapshot_survives_ref_delete_and_gc(context):
    # 基线是持久引用，不能依赖可删除的 Git ref 或被 GC 回收。
    service, project, request = context
    branch = service.create(project, request)
    row = service.store.get(project, str(branch.branch_id))
    artifacts = ArtifactStore(service.runtime.db)
    source = row["base_input_set_id"]
    assert artifacts.read_file(source, "models/a.sql")
    service.delete(project, str(branch.branch_id), branch.version)
    assert not artifacts.delete_unreferenced(source)
    assert artifacts.read_file(source, "models/a.sql")


def test_source_without_matching_release_does_not_claim_current_main_catalog(context, repository):
    # 只有完全相同源码的发布才能作为资源基线，不能拿当前生产冒充。
    service, project, request = context
    from dbt_metricflow_service.storage.publications import PublicationStore
    from tests.test_publication_transaction import prepared

    jobs, job, release, output, _ = prepared(PublicationStore(service.runtime.db), repository.parent, project)
    assert jobs.finish(job["job_id"], job["lease_token"], output_set_id=output)
    git(repository, "commit", "--allow-empty", "-m", "unpublished source")
    advanced = request.model_copy(update={"source_commit_sha": git(repository, "rev-parse", "HEAD")})
    branch = service.create(project, advanced)
    row = service.store.get(project, str(branch.branch_id))
    assert row["base_release_id"] is None
    assert row["production_base_release_id"] == release["release_id"]


def test_old_scan_observation_cannot_supersede_explicit_build(context, repository, monkeypatch):
    # 扫描读完旧 head 后显式受理新 head，恢复扫描必须放弃旧输入。
    from dbt_metricflow_service import branch_sync
    from dbt_metricflow_service.publication import PublicationService
    from tests.test_branch_sync import scanner

    sync, service, project, branch = scanner(context)
    original = branch_sync.observe_revision
    accepted = []

    def interleaved(*args, **kwargs):
        observed = original(*args, **kwargs)
        if kwargs.get("git_ref") == branch.git_ref:
            git(repository, "checkout", "feature")
            git(repository, "commit", "--allow-empty", "-m", "newer observation")
            accepted.append(PublicationService(service.runtime).submit(
                project, uuid4().hex, branch_id=str(branch.branch_id)))
        return observed

    monkeypatch.setattr(branch_sync, "observe_revision", interleaved)
    sync.scan(project_id=project)
    row = service.store.get(project, str(branch.branch_id))
    assert row["latest_release_id"] == accepted[0]["release_id"]
    assert row["observed_head_sha"] == git(repository, "rev-parse", "HEAD")


def test_create_recovery_does_not_adopt_unowned_equal_sha(context, repository, monkeypatch):
    import pytest

    from dbt_metricflow_service.storage.jobs import StoreConflict

    service, project, request = context
    resume = service._resume_create

    def interrupted(*_args):
        raise RuntimeError("before push")

    monkeypatch.setattr(service, "_resume_create", interrupted)
    with pytest.raises(RuntimeError):
        service.create(project, request)
    git(repository, "branch", request.name, request.source_commit_sha)
    monkeypatch.setattr(service, "_resume_create", resume)
    with pytest.raises(StoreConflict):
        service.create(project, request)


def test_owned_create_recovery_accepts_normal_descendant(context, repository, monkeypatch):
    import pytest

    service, project, request = context
    activate = service._activate

    def interrupted(*_args):
        raise RuntimeError("after push")

    monkeypatch.setattr(service, "_activate", interrupted)
    with pytest.raises(RuntimeError):
        service.create(project, request)
    git(repository, "checkout", request.name)
    git(repository, "commit", "--allow-empty", "-m", "normal descendant")
    monkeypatch.setattr(service, "_activate", activate)
    assert service.create(project, request).observed_head_sha == git(repository, "rev-parse", "HEAD")


def test_v2_rejects_unsupported_manifest_policy(tmp_path):
    from dbt_metricflow_service.draft_validation_worker import validate_project
    from tests.resource_helpers import make_resource_project

    project, profiles, _ = make_resource_project(tmp_path)
    (project / "models/unsafe.sql").write_text(
        "{{ config(materialized='incremental', post_hook='delete from some_table') }} select 1 as id", encoding=UTF8)
    result = validate_project(project, profiles, "test", validate_sql=True)
    assert result["valid"] is False
    assert result["diagnostics"][0]["code"] == "unsupported_execution_policy"


def test_failed_release_exposes_safe_attempt_summary(store, tmp_path):
    from types import SimpleNamespace

    from dbt_metricflow_service.publication import PublicationService
    from tests.test_branch_publication import SelectedStore, branch_pair
    from tests.test_publication_transaction import prepared

    project, branch, _ = branch_pair(store)
    jobs, job, release, _, artifacts = prepared(SelectedStore(store.db, branch), tmp_path, project)
    summary = {"phase": "build", "checks": [{"name": "test.project.orders", "status": "FAILED",
               "message": "测试未通过。"}], "truncated": False}
    assert jobs.fail(job["job_id"], job["lease_token"], "COMMAND_FAILED", {"validationSummary": summary})
    view = PublicationService(SimpleNamespace(db=store.db, jobs=jobs, artifacts=artifacts), branch_id=branch)
    assert view.release(project, release["release_id"])["validationSummary"] == summary


def test_upgrade_fences_inflight_preview(store, tmp_path):
    from tests.test_branch_publication import SelectedStore, branch_pair
    from tests.test_publication_transaction import prepared

    project, branch, _ = branch_pair(store)
    jobs, job, release, output, _ = prepared(SelectedStore(store.db, branch), tmp_path, project)
    jobs.register_project(project, config_version="2")
    assert jobs.finish(job["job_id"], job["lease_token"], output_set_id=output)
    assert store.get_release(project, release["release_id"])["state"] == "SUPERSEDED"


def test_failed_check_summary_does_not_expose_sql_or_stderr(tmp_path):
    from dbt_metricflow_service.branch_validation_summary import failure_summary

    (tmp_path / "run_results.json").write_text(json.dumps({"results": [
        {"unique_id": "test.project.fail", "status": "fail", "message": "SECRET sql and connection"}]}),
        encoding=UTF8)
    summary = failure_summary(tmp_path, "build", 4096)
    assert summary["checks"][0]["name"] == "test.project.fail"
    assert summary["checks"][0]["status"] == "FAILED"
    assert "SECRET" not in json.dumps(summary)


def test_branch_release_list_carries_fixed_context(store, tmp_path):
    from dbt_metricflow_service.publication import PublicationService
    from tests.test_branch_catalog import published_pair

    runtime, project, branch, _, release, _ = published_pair(store, tmp_path)
    service = PublicationService(runtime, branch_id=branch)
    listed = service.releases(project)[0]
    assert listed["branchId"] == branch
    assert listed["gitRef"] == service._branch(project)["git_ref"]
    assert listed["releaseId"] == release["release_id"]
