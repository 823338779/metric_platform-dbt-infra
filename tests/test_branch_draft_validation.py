"""SQL/YAML 草稿 v2 保留工作区修订和分支证据，v1 仍只接受 YAML。"""

import json
from importlib import import_module
from pathlib import Path
from uuid import uuid4

import pytest
from resource_helpers import make_resource_project

from dbt_metricflow_service.draft_validation import DraftValidationService
from dbt_metricflow_service.draft_validation_models import DraftChange
from dbt_metricflow_service.storage.artifacts import ArtifactStore
from dbt_metricflow_service.storage.jobs import StoreConflict
from tests.test_branch_lifecycle import context as context
from tests.test_draft_validation_storage import runtime as runtime
from tests.test_platform_bindings import git
from tests.test_platform_bindings import repository as repository
from tests.test_publication_storage import store as store

MODELS = "dbt_metricflow_service.draft_validation_models"
CHANGE = {"path": "models/new.sql", "operation": "CREATE", "content": "select 1 as amount\n"}
FIXTURE = Path(__file__).parent / "fixtures/agent_contract/changes-v2.json"


def request(sha, key=None, changes=None):
    module = import_module(MODELS)
    assert hasattr(module, "BranchDraftValidationRequest"), "缺少分支 SQL/YAML 验证协议"
    return module.BranchDraftValidationRequest(baseCommitSha=sha, idempotencyKey=key or uuid4().hex,
                                              workspaceId=uuid4().hex, draftRevision=1,
                                              changes=changes or [CHANGE])


def test_revision_or_branch_change_invalidates_evidence(context):
    service, project, create = context
    branch = service.create(project, create)
    runtime = service.runtime
    runtime.settings.config_version = "1"
    runtime.settings.command_timeout_seconds = 600
    runtime.artifacts = ArtifactStore(runtime.db)
    runtime.toolchain = uuid4().hex
    draft = request(create.source_commit_sha)
    validator = DraftValidationService(runtime)
    receipt = validator.submit(project, draft, branch_id=str(branch.branch_id))
    assert validator.submit(project, draft, branch_id=str(branch.branch_id)) == receipt
    with pytest.raises(StoreConflict):
        validator.submit(project, draft.model_copy(update={"draft_revision": 2}), branch_id=str(branch.branch_id))
    with pytest.raises(StoreConflict):
        validator.submit(project, draft.model_copy(update={"workspace_id": uuid4().hex}),
                         branch_id=str(branch.branch_id))
    result = validator.get(project, str(receipt.validation_id), branch_id=str(branch.branch_id))
    assert str(result.branch_id) == str(branch.branch_id)
    assert result.workspace_id == draft.workspace_id
    assert result.draft_revision == 1
    with pytest.raises(KeyError):
        validator.get(project, str(receipt.validation_id))


@pytest.mark.parametrize("path", ["../models/a.sql", "models/../a.sql", "macros/a.sql", "dbt_project.yml",
                                   "models/a.py", "tests/a.txt", "models\\a.sql"])
def test_traversal_and_unsupported_input_fail(path):
    module = import_module(MODELS)
    assert hasattr(module, "BranchDraftValidationRequest"), "缺少分支 SQL/YAML 验证协议"
    with pytest.raises(ValueError):
        request("a" * 40, changes=[{**CHANGE, "path": path}])


def test_case_collision_fails_and_v1_stays_yaml_only():
    with pytest.raises(ValueError):
        request("a" * 40, changes=[CHANGE, {**CHANGE, "path": "models/NEW.sql"}])
    with pytest.raises(ValueError):
        DraftChange.model_validate(CHANGE)


def test_v2_digest_matches_cross_language_fixture():
    module = import_module(MODELS)
    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
    draft = request("a" * 40, changes=fixture["changes"])
    assert module.changes_digest_v2(draft.changes) == fixture["digest"]
    assert module.changes_digest_v2(list(reversed(draft.changes))) == fixture["digest"]


def test_v2_compile_rejects_model_ddl_without_executing_it(tmp_path):
    # dbt parse 不验证 SQL 语法，v2 必须在执行任何模型前拒绝写语句。
    from dbt_metricflow_service.draft_validation_worker import validate_project

    project, profiles, _ = make_resource_project(tmp_path)
    (project / "models/danger.sql").write_text("drop table forbidden_table", encoding="utf-8")
    result = validate_project(project, profiles, "test", validate_sql=True)
    assert result["valid"] is False
    assert result["diagnostics"][0]["code"] == "invalid_model_sql"


def test_v2_accepts_supported_sql_test_in_private_copy(tmp_path):
    # tests 下定义属于 v2 范围，只应用任务副本，不放宽 v1 model-paths。
    from dbt_metricflow_service.draft_validation_execution import apply_changes

    project, _, _ = make_resource_project(tmp_path)
    draft = request("a" * 40, changes=[{**CHANGE, "path": "tests/check_amount.sql"}])
    apply_changes(project, draft.changes, version=2)
    assert (project / "tests/check_amount.sql").read_text(encoding="utf-8") == CHANGE["content"]


async def test_sql_and_yaml_changes_are_validated_on_feature_baseline(runtime, tmp_path):
    # 真实 dbt 进程从 feature 独有 SHA 解析 SQL/YAML/测试，生产指针保持为空。
    from dbt_metricflow_service.branch_models import CreateBranchRequest
    from dbt_metricflow_service.branches import BranchService
    from dbt_metricflow_service.runtime_execution import RuntimeExecutor
    from dbt_metricflow_service.storage.branches import BranchStore

    project_path, profiles, _ = make_resource_project(tmp_path)
    git(project_path, "init", "-b", "main")
    git(project_path, "config", "user.name", "Test")
    git(project_path, "config", "user.email", "test@example.invalid")
    git(project_path, "add", ".")
    git(project_path, "commit", "-m", "baseline")
    project = uuid4().hex
    runtime.jobs.register_project(project, {"remote": str(project_path), "projectSubdir": ".",
                                            "profileBindingId": "test"})
    runtime.settings.branch_preview_profile_binding_id = "test"
    main = BranchStore(runtime.db).production(project)
    branch = BranchService(runtime).create(project, CreateBranchRequest(
        name="feature", sourceBranchId=main["branch_id"], sourceCommitSha=git(project_path, "rev-parse", "HEAD"),
        idempotencyKey=uuid4().hex))
    git(project_path, "checkout", "feature")
    git(project_path, "commit", "--allow-empty", "-m", "feature-only")
    draft = request(git(project_path, "rev-parse", "HEAD"), changes=[CHANGE,
        {"path": "models/extra.yml", "operation": "CREATE", "content": "version: 2\n"},
        {"path": "tests/empty.sql", "operation": "CREATE", "content": "select 1 where false\n"}])
    runtime.settings.profiles_dir = profiles
    runtime.settings.max_output_bytes = 1024 * 1024
    runtime.settings.max_result_bytes = 16 * 1024 * 1024
    runtime.toolchain = uuid4().hex
    validator = DraftValidationService(runtime)
    receipt = validator.submit(project, draft, branch_id=str(branch.branch_id))
    job = runtime.jobs.claim(str(uuid4()), toolchain_version=runtime.toolchain)
    output = await RuntimeExecutor(runtime.settings, runtime.jobs, runtime.artifacts).execute(job)
    assert output.output_set_id is None
    assert runtime.jobs.finish(job["job_id"], job["lease_token"], output.payload)
    result = validator.get(project, str(receipt.validation_id), branch_id=str(branch.branch_id))
    assert result.valid is True
    assert "SQL_DEFINITION" in result.checked_levels
    assert BranchStore(runtime.db).production(project)["active_release_id"] is None
    assert not (project_path / CHANGE["path"]).exists()
