"""Agent 校验凭据需要当前配置上下文，不能从历史 active release 推断。"""

from types import SimpleNamespace

from dbt_metricflow_service.publications.service import PublicationService


def test_branch_publication_exposes_current_validation_context_without_changing_legacy(monkeypatch):
    runtime = SimpleNamespace(db=None, toolchain="toolchain-new")
    branch = {
        "branch_id": "branch",
        "git_ref": "refs/heads/dev",
        "config_version": "config-new",
        "binding_config": {"projectSubdir": "."},
    }
    monkeypatch.setattr(PublicationService, "_branch", lambda self, project: branch)
    selected = PublicationService(runtime, branch_id="branch")
    monkeypatch.setattr(
        selected.store, "get_publication", lambda *args, **kwargs: {"projectId": "project", "activePublication": None}
    )
    result = selected.publication("project")
    assert result["configVersion"] == "config-new"
    assert result["toolchainVersion"] == "toolchain-new"
    legacy = PublicationService(runtime)
    monkeypatch.setattr(
        legacy.store, "get_publication", lambda *args, **kwargs: {"projectId": "project", "activePublication": None}
    )
    assert "configVersion" not in legacy.publication("project")
