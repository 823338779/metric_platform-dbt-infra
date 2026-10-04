"""固定创建基线与封存目标的差异，不以当前生产覆盖历史输入。"""

from importlib import import_module
from types import SimpleNamespace
from uuid import uuid4

from dbt_metricflow_service.platform_bindings import resolve_draft_revision
from dbt_metricflow_service.publication import PublicationService
from dbt_metricflow_service.storage.artifacts import ArtifactStore
from tests.test_branch_lifecycle import context as context
from tests.test_platform_bindings import git
from tests.test_platform_bindings import repository as repository
from tests.test_publication_storage import store as store

MODULE = "dbt_metricflow_service.branch_diff"
MODEL = "models/a.sql"
SQL = "select 2 as value\n"
UTF8 = "utf-8"
TOOLCHAIN = "diff-test-"
SOURCE = "SOURCE"


def test_diff_reads_fixed_baseline_without_reactivating_history(context, repository, tmp_path):
    service, project, request = context
    branch = service.create(project, request)
    git(repository, "checkout", request.name)
    (repository / MODEL).write_text(SQL, encoding=UTF8)
    git(repository, "commit", "-am", "change model")
    runtime = service.runtime
    runtime.toolchain = TOOLCHAIN + uuid4().hex
    runtime.settings.command_timeout_seconds = 600
    runtime.artifacts = ArtifactStore(runtime.db)
    release = PublicationService(runtime).submit(project, uuid4().hex, branch_id=str(branch.branch_id))
    job = runtime.jobs.claim(str(uuid4()), toolchain_version=runtime.toolchain)
    source, _ = resolve_draft_revision(service._binding(project), release["request_json"]["commitSha"],
                                       tmp_path / "source-copy", git_ref=branch.git_ref)
    input_set = runtime.artifacts.capture(project, source, kind=SOURCE)
    assert runtime.jobs.attach_input(job["job_id"], job["lease_token"], input_set)
    # 远端再次变化后，diff 仍读取该 release 的封存输入。
    (repository / MODEL).write_text("select 3 as value\n", encoding=UTF8)
    git(repository, "commit", "-am", "later model")
    result = import_module(MODULE).BranchDiffService(runtime).compare(
        project, str(branch.branch_id), release["release_id"])
    assert result["baseCommitSha"] == request.source_commit_sha
    assert result["targetCommitSha"] == release["request_json"]["commitSha"]
    assert result["files"][0]["path"] == MODEL
    assert "+select 2 as value" in result["files"][0]["diff"]
    assert "select 3" not in result["files"][0]["diff"]
    assert result["truncated"] is False
    assert service.store.production(project)["active_release_id"] is None
    # 删除远端后依然读取相同的封存基线和目标，不访问已消失的 ref。
    git(repository, "checkout", "main")
    service.delete(project, str(branch.branch_id), branch.version)
    historical = import_module(MODULE).BranchDiffService(runtime).compare(
        project, str(branch.branch_id), release["release_id"])
    assert historical == result


def test_resource_diff_detects_attribute_change_without_renaming():
    # 相同 nativeId 的口径改变必须展示为修改，而不是漏报或新增身份。
    module = import_module(MODULE)
    resource = {"nativeId": "metric.sample.orders", "kind": "METRIC", "name": "orders",
                "attributes": {"definitionSummary": "sum(amount)"}}
    docs = {"base": {"resources": [resource]}, "target": {"resources": [
        {**resource, "attributes": {"definitionSummary": "sum(net_amount)"}}]}}
    service = module.BranchDiffService(SimpleNamespace())
    # 使用独立资源比较入口，不依赖当前活动指针。
    assert service._compare_resources(docs["base"]["resources"], docs["target"]["resources"]) == [
        {"nativeId": resource["nativeId"], "kind": "METRIC", "status": "MODIFIED"}]
