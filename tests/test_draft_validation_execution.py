"""实际 dbt parse 与语义规则测试；不连接仓库或执行模型 SQL。"""

import importlib
import json
import os
import subprocess
import sys
from uuid import uuid4

import pytest
from resource_helpers import make_resource_project

from dbt_metricflow_service.validation.models import DraftChange
from tests.test_draft_validation_storage import runtime as runtime
from tests.test_draft_validation_storage import service, setup
from tests.test_platform_bindings import git
from tests.test_publication_storage import store as store


def validate(tmp_path, modify=None):
    spec = importlib.util.find_spec("dbt_metricflow_service.validation.worker")
    assert spec is not None, "isolated dbt validation worker is not implemented"
    project, profiles, *_ = make_resource_project(tmp_path)
    if modify:
        modify(project)
    output = tmp_path / "validation.json"
    result = subprocess.run([sys.executable, "-m", spec.name, str(project), str(profiles), "test", str(output)],
                            capture_output=True, text=True, timeout=60,
                            env={**os.environ, "PYTHONUTF8": "1", "DBT_SEND_ANONYMOUS_USAGE_STATS": "false"})
    assert result.returncode == 0, result.stderr[-1500:]
    return json.loads(output.read_text("utf-8")), project


def test_actual_inline_semantics_are_valid_without_building_tables(tmp_path):
    result, project = validate(tmp_path)
    assert result["valid"] is True
    assert result["checkedLevels"] == ["YAML", "TEMPLATE", "DBT_PARSE", "SEMANTIC"]
    assert not (project / "target/run_results.json").exists()


def test_duplicate_yaml_key_has_real_line_location(tmp_path):
    def modify(project):
        (project / "models/duplicate.yml").write_text("version: 2\nversion: 2\n", encoding="utf-8")
    result, _ = validate(tmp_path, modify)
    assert result["valid"] is False
    assert result["diagnostics"][0]["path"] == "models/duplicate.yml"
    assert result["diagnostics"][0]["line"] == 2


def test_missing_ratio_dependency_is_definition_error(tmp_path):
    def modify(project):
        (project / "models/ratio.yml").write_text(
            "version: 2\nmetrics:\n  - name: ratio\n    label: Ratio\n    type: ratio\n"
            "    type_params:\n      numerator: revenue\n      denominator: missing_metric\n", encoding="utf-8")
    result, _ = validate(tmp_path, modify)
    assert result["valid"] is False
    assert result["diagnostics"]


def test_top_level_metric_can_reference_inline_metric(tmp_path):
    def modify(project):
        (project / "models/ratio.yml").write_text(
            "version: 2\nmetrics:\n  - name: ratio\n    label: Ratio\n    type: ratio\n"
            "    type_params:\n      numerator: revenue\n      denominator: revenue\n", encoding="utf-8")
    result, _ = validate(tmp_path, modify)
    assert result["valid"] is True
    assert "SEMANTIC" in result["checkedLevels"]


def test_deleted_semantic_definition_is_invalid(tmp_path):
    result, _ = validate(tmp_path, lambda project: (project / "models/orders.yml").unlink())
    assert result["valid"] is False


def test_missing_profile_environment_is_infrastructure_failure(tmp_path, monkeypatch):
    from dbt_metricflow_service.validation.worker import validate_project

    monkeypatch.delenv("AGENT_FIXTURE_MISSING_ENV", raising=False)
    project, profiles, _ = make_resource_project(tmp_path)
    profile = profiles / "profiles.yml"
    profile.write_text(profile.read_text("utf-8").replace("schema: main",
                       "schema: \"{{ env_var('AGENT_FIXTURE_MISSING_ENV') }}\""), encoding="utf-8")
    with pytest.raises(RuntimeError, match="infrastructure"):
        validate_project(project, profiles, "test")


def test_changes_use_complete_paths_and_old_hash(tmp_path):
    spec = importlib.util.find_spec("dbt_metricflow_service.validation.execution")
    assert spec is not None, "draft change application is not implemented"
    apply = importlib.import_module(spec.name).apply_changes
    project, *_ = make_resource_project(tmp_path)
    changes = [DraftChange(path=f"models/{folder}/same.yml", operation="CREATE", content="version: 2\n")
               for folder in ("first", "second")]
    digest = apply(project, changes)
    assert (project / "models/first/same.yml").read_bytes() == b"version: 2\n"
    assert (project / "models/second/same.yml").read_bytes() == b"version: 2\n"
    assert len(digest) == 64
    with pytest.raises(ValueError):
        apply(project, [DraftChange(path="models/first/same.yml", operation="DELETE", expectedSha256="a" * 64)])
    with pytest.raises(ValueError):
        apply(project, [DraftChange(path="dbt_project.yml", operation="UPDATE", content="x", expectedSha256="a" * 64)])


@pytest.mark.parametrize("schema_from_environment", [False, True])
async def test_actual_executor_finishes_without_replacing_any_pointer(runtime, tmp_path, schema_from_environment):
    from dbt_metricflow_service.runtime.executor import RuntimeExecutor
    from dbt_metricflow_service.validation.models import DraftValidationRequest

    project_path, profiles, _ = make_resource_project(tmp_path)
    if schema_from_environment:
        profile = profiles / "profiles.yml"
        profile.write_text(profile.read_text("utf-8").replace("schema: main",
                           "schema: \"{{ env_var('DBT_PLATFORM_SCHEMA') }}\""), encoding="utf-8")
    git(project_path, "init", "-b", "main")
    git(project_path, "config", "user.name", "Fixture")
    git(project_path, "config", "user.email", "fixture@example.invalid")
    git(project_path, "add", ".")
    git(project_path, "commit", "-m", "fixture")
    project, _ = setup(runtime)
    runtime.jobs.register_project(project, {"remote": str(project_path), "projectSubdir": ".",
                                            "profileBindingId": "test", "schemaName": "agent_fixture"})
    request = DraftValidationRequest(baseCommitSha=git(project_path, "rev-parse", "HEAD"), idempotencyKey="actual",
        changes=[{"path": "models/extra.yml", "operation": "CREATE", "content": "version: 2\n"}])
    receipt = service(runtime).submit(project, request)
    runtime.settings.profiles_dir = profiles
    runtime.settings.max_output_bytes = 1024 * 1024
    runtime.settings.max_result_bytes = 16 * 1024 * 1024
    runtime.toolchain = "test"
    # 按项目外的随机工具链隔离认领，避免取到其他测试遗留队列。
    with runtime.db.transaction() as cursor:
        runtime.toolchain = uuid4().hex
        cursor.execute("UPDATE runtime_job SET toolchain_version=%s WHERE job_id=%s",
                       (runtime.toolchain, str(receipt.validation_id)))
    job = runtime.jobs.claim(str(uuid4()), toolchain_version=runtime.toolchain)
    executor = RuntimeExecutor(runtime.settings, runtime.jobs, runtime.artifacts)
    output = await executor.execute(job)
    assert output.output_set_id is None
    runtime.jobs.finish(job["job_id"], job["lease_token"], output.payload)
    result = service(runtime).get(project, str(receipt.validation_id))
    assert result.valid is True
    assert result.validated_project_digest
    after = runtime.jobs.project(project)
    assert after["current_output_set_id"] is None
    assert after["active_published_release_id"] is None
