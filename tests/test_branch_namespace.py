"""真实 Git 候选受理固定开发 profile、分支 schema 和 run 前缀。"""

from uuid import UUID, uuid4

from dbt_metricflow_service.platform_namespace import run_prefix
from dbt_metricflow_service.publication import PublicationService
from tests.test_branch_lifecycle import context as context
from tests.test_platform_bindings import repository as repository
from tests.test_publication_storage import store as store

TOOLCHAIN = "branch-test"
SCHEMA_PREFIX = "dbt_dev_"


def test_branches_and_runs_have_isolated_physical_names(context):
    # 两分支同一模型名也使用不同 schema，同分支的新 run 前缀不同。
    service, project, request = context
    a = service.create(project, request)
    b = service.create(project, request.model_copy(update={"name": "feature-b", "idempotency_key": uuid4().hex}))
    runtime = service.runtime
    runtime.toolchain = TOOLCHAIN
    runtime.settings.command_timeout_seconds = 600
    publications = PublicationService(runtime)
    first = publications.submit(project, uuid4().hex, branch_id=str(a.branch_id))
    second = publications.submit(project, uuid4().hex, branch_id=str(b.branch_id))
    again = publications.submit(project, uuid4().hex, branch_id=str(a.branch_id))
    first_job, second_job = runtime.jobs.get(first["run_id"]), runtime.jobs.get(second["run_id"])
    assert first_job["schema_name"] == SCHEMA_PREFIX + a.branch_id.hex
    assert second_job["schema_name"] == SCHEMA_PREFIX + b.branch_id.hex
    assert first_job["profile_binding_id"] == runtime.settings.branch_preview_profile_binding_id
    assert first_job["request_json"]["gitRef"] == a.git_ref
    assert first_job["branch_id"] == str(a.branch_id)
    assert run_prefix(UUID(first["run_id"])) != run_prefix(UUID(again["run_id"]))


def test_explicit_production_build_records_observed_source(context):
    service, project, request = context
    runtime = service.runtime
    runtime.toolchain = TOOLCHAIN
    runtime.settings.command_timeout_seconds = 600
    PublicationService(runtime).submit(project, uuid4().hex)
    main = service.store.production(project)
    assert main["base_commit_sha"] == request.source_commit_sha
    assert main["observed_head_sha"] == request.source_commit_sha
