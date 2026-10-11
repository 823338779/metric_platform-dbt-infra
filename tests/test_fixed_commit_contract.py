"""固定提交入口替代隐式分支观察和通用执行接口。"""

from types import SimpleNamespace

import pytest

from dbt_metricflow_service.api.app import create_app
from dbt_metricflow_service.models.builds import BuildRequest
from dbt_metricflow_service.platform.bindings import ProjectBinding, resolve_commit
from tests.test_platform_bindings import git
from tests.test_platform_bindings import repository as repository
from tests.test_v3_contract import build_body


def test_fixed_commit_request_rejects_moving_refs_and_environment_overrides():
    request = BuildRequest.model_validate(build_body())
    assert request.commit_sha == "a" * 40
    for fields in ({"commitSha": "main"}, {"remote": "untrusted"}, {"changes": []}):
        with pytest.raises(ValueError):
            BuildRequest.model_validate(build_body(**fields))


def test_resolve_fixed_commit_outside_main(repository, tmp_path):
    git(repository, "checkout", "-b", "external-development")
    (repository / "models/a.sql").write_text("select 42 as value\n", encoding="utf-8")
    git(repository, "commit", "-am", "outside main")
    sha = git(repository, "rev-parse", "HEAD")
    git(repository, "checkout", "main")
    project, _ = resolve_commit(ProjectBinding("sample", str(repository), ".", "postgres"), sha, tmp_path / "fixed")
    assert (project / "models/a.sql").read_text("utf-8") == "select 42 as value\n"


def test_application_requires_postgresql():
    with pytest.raises(ValueError, match="SERVICE_DATABASE_URL"):
        create_app(SimpleNamespace(database_url=None))


def test_application_exposes_only_fixed_commit_business_routes(monkeypatch):
    import dbt_metricflow_service.api.app as api

    monkeypatch.setattr(
        api,
        "Runtime",
        lambda settings: SimpleNamespace(
            db=None, jobs=SimpleNamespace(db=None), artifacts=None, toolchain="test", settings=settings
        ),
    )
    app = api.create_app(
        SimpleNamespace(service_token="test", database_url="unused", command_timeout_seconds=30, temp_root=None)
    )
    paths = set(app.openapi()["paths"])
    assert "/v3/builds" in paths
    assert not any("validations" in path for path in paths)
    assert not any(path.startswith("/v1/") and path != "/v1/versions" for path in paths)
    assert not any("branches" in path or "compatibility" in path or path.startswith("/internal/") for path in paths)
