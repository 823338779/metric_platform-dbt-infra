"""持久信号只触发核对真实 Git，重复、乱序和遗漏事件都收敛。"""

from importlib import import_module
from uuid import uuid4

from dbt_metricflow_service.publication import PublicationService
from tests.test_branch_lifecycle import context as context
from tests.test_platform_bindings import git
from tests.test_platform_bindings import repository as repository
from tests.test_publication_storage import store as store

MODULE = "dbt_metricflow_service.branch_sync"
SQL_DUE = "UPDATE runtime_branch SET scan_expires_at=now()-interval '1 second' WHERE project_id=%s"
SQL_FAIL = "UPDATE runtime_release SET state='FAILED' WHERE release_id=%s"


def scanner(context):
    service, project, request = context
    runtime = service.runtime
    runtime.toolchain = uuid4().hex
    runtime.settings.branch_poll_seconds = 30
    runtime.settings.command_timeout_seconds = 600
    branch = service.create(project, request)
    sync = import_module(MODULE).BranchSynchronizer(runtime)
    return sync, service, project, branch


def test_duplicate_and_out_of_order_events_converge(context):
    sync, service, project, branch = scanner(context)
    sync.signal(project, branch.git_ref)
    sync.signal(project, branch.git_ref)
    sync.scan(project_id=project)
    first = service.store.get(project, str(branch.branch_id))
    sync.signal(project, branch.git_ref)
    sync.scan(project_id=project)
    last = service.store.get(project, str(branch.branch_id))
    assert first["latest_release_id"] == last["latest_release_id"]
    assert last["publication_sequence"] == 1
    assert last["processed_signal_version"] == last["signal_version"]


def test_missed_event_is_found_on_scan(context, repository):
    sync, service, project, branch = scanner(context)
    sync.scan(project_id=project)
    git(repository, "checkout", branch.git_ref.removeprefix("refs/heads/"))
    git(repository, "commit", "--allow-empty", "-m", "missed push")
    with service.runtime.db.transaction() as cursor:
        cursor.execute(SQL_DUE, (project,))
    sync.scan(project_id=project)
    row = service.store.get(project, str(branch.branch_id))
    assert row["publication_sequence"] == 2
    assert row["observed_head_sha"] == git(repository, "rev-parse", "HEAD")


def test_signal_during_scan_is_not_lost(context, monkeypatch):
    sync, service, project, branch = scanner(context)
    module = import_module(MODULE)
    original = module.observe_revision

    def during_scan(*args, **kwargs):
        if kwargs.get("git_ref") == branch.git_ref:
            sync.signal(project, branch.git_ref)
        return original(*args, **kwargs)

    monkeypatch.setattr(module, "observe_revision", during_scan)
    sync.scan(project_id=project)
    row = service.store.get(project, str(branch.branch_id))
    assert row["processed_signal_version"] < row["signal_version"]
    monkeypatch.setattr(module, "observe_revision", original)
    sync.scan(project_id=project)
    row = service.store.get(project, str(branch.branch_id))
    assert row["processed_signal_version"] == row["signal_version"]
    assert row["publication_sequence"] == 1


def test_failed_same_input_is_not_auto_retried(context):
    sync, service, project, branch = scanner(context)
    sync.scan(project_id=project)
    first = service.store.get(project, str(branch.branch_id))
    with service.runtime.db.transaction() as cursor:
        cursor.execute(SQL_FAIL, (first["latest_release_id"],))
    sync.signal(project, branch.git_ref)
    sync.scan(project_id=project)
    assert service.store.get(project, str(branch.branch_id))["latest_release_id"] == first["latest_release_id"]
    explicit = PublicationService(service.runtime).submit(project, uuid4().hex, branch_id=str(branch.branch_id))
    assert explicit["release_id"] != first["latest_release_id"]
