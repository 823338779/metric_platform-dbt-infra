"""分支生命周期使用真实本地 Git 远端和 PostgreSQL。"""

from importlib import import_module
from types import SimpleNamespace
from uuid import uuid4

import pytest

from dbt_metricflow_service.branches.models import CreateBranchRequest, RegisterBranchRequest
from dbt_metricflow_service.storage.branches import BranchStore
from dbt_metricflow_service.storage.jobs import JobStore, StoreConflict
from tests.test_platform_bindings import git
from tests.test_platform_bindings import repository as repository
from tests.test_publication_storage import store as store

SERVICE_MODULE = "dbt_metricflow_service.branches.service"
PROJECT_PREFIX = "lifecycle-"
REV_PARSE = "rev-parse"
HEAD = "HEAD"
BRANCH = "branch"
FEATURE = "feature"
REF = "refs/heads/feature"
PROFILE = "postgres-preview"
TEMP = "branch-work"
ROOT = "."
PRODUCTION_PROFILE = "postgres"


@pytest.fixture
def context(store, repository, tmp_path):
    # 独立项目绑定本地测试仓库，不接触用户的远端。
    project = PROJECT_PREFIX + uuid4().hex
    jobs = JobStore(store.db)
    jobs.register_project(project, {"remote": str(repository), "projectSubdir": ROOT,
                                    "profileBindingId": PRODUCTION_PROFILE})
    runtime = SimpleNamespace(db=store.db, jobs=jobs, settings=SimpleNamespace(
        temp_root=tmp_path / TEMP, branch_preview_profile_binding_id=PROFILE))
    service = import_module(SERVICE_MODULE).BranchService(runtime)
    source = BranchStore(store.db).production(project)
    request = CreateBranchRequest(name=FEATURE, source_branch_id=source["branch_id"],
                                  source_commit_sha=git(repository, REV_PARSE, HEAD), idempotency_key=uuid4().hex)
    return service, project, request


def test_create_retry_reconciles_same_ref(context, repository, monkeypatch):
    # 模拟远端创建成功、数据库激活失败；同键重试应恢复同一身份。
    service, project, request = context
    original = service._activate

    def interrupted(*_args):
        raise RuntimeError("injected activation failure")

    monkeypatch.setattr(service, "_activate", interrupted)
    with pytest.raises(RuntimeError):
        service.create(project, request)
    assert git(repository, REV_PARSE, REF) == request.source_commit_sha
    pending = service.store.list(project)[-1]
    monkeypatch.setattr(service, "_activate", original)
    result = service.create(project, request)
    assert str(result.branch_id) == pending["branch_id"]
    assert result.status == "ACTIVE"
    assert service.create(project, request).branch_id == result.branch_id
    assert service.store.get(project, str(result.branch_id))["signal_version"] == 1


def test_deleted_name_gets_new_branch_id(context):
    # 删除旧身份后允许同名新建；旧幂等重试仍返回旧删除状态。
    service, project, request = context
    original = service.create(project, request)
    deleted = service.delete(project, str(original.branch_id), original.version)
    assert deleted.status == "DELETED"
    renewed = service.create(project, request.model_copy(update={"idempotency_key": uuid4().hex}))
    assert renewed.branch_id != original.branch_id
    assert service.create(project, request).status == "DELETED"


def test_main_cannot_be_deleted(context):
    service, project, request = context
    with pytest.raises(StoreConflict):
        service.delete(project, str(request.source_branch_id), 1)


def test_existing_unowned_ref_requires_register(context, repository):
    service, project, request = context
    git(repository, BRANCH, FEATURE)
    with pytest.raises(StoreConflict):
        service.create(project, request)
    registered = service.register(project, RegisterBranchRequest(
        name=FEATURE, base_commit_sha=request.source_commit_sha, idempotency_key=uuid4().hex))
    assert registered.git_ref == REF
    assert registered.status == "ACTIVE"


def test_stale_delete_version_is_rejected(context):
    service, project, request = context
    branch = service.create(project, request)
    with pytest.raises(StoreConflict):
        service.delete(project, str(branch.branch_id), branch.version - 1)


def test_delete_retry_does_not_remove_changed_remote(context, repository, monkeypatch):
    # 首次删除失败后远端有新提交，重试不得扩大原删除意图。
    service, project, request = context
    branch = service.create(project, request)
    module = import_module(SERVICE_MODULE)
    original = module._git

    def interrupted(directory, *args, **kwargs):
        if args[0] == "push":
            raise ValueError("injected remote failure")
        return original(directory, *args, **kwargs)

    monkeypatch.setattr(module, "_git", interrupted)
    with pytest.raises(ValueError):
        service.delete(project, str(branch.branch_id), branch.version)
    monkeypatch.setattr(module, "_git", original)
    git(repository, "commit", "--allow-empty", "-m", "new head")
    git(repository, BRANCH, "-f", FEATURE, HEAD)
    changed = git(repository, REV_PARSE, REF)
    with pytest.raises(StoreConflict):
        service.delete(project, str(branch.branch_id), branch.version)
    assert git(repository, REV_PARSE, REF) == changed


def test_merge_link_uses_only_controlled_repository_url(context):
    service, project, request = context
    config = service.runtime.jobs.project(project)["binding_config"]
    config["gitWebUrl"] = "https://forgejo.example.test/team/metrics_store"
    service.runtime.jobs.register_project(project, config)
    branch = service.create(project, request.model_copy(update={"name": "feature/sales"}))
    assert branch.merge_request_url == "https://forgejo.example.test/team/metrics_store/compare/main...feature%2Fsales"
