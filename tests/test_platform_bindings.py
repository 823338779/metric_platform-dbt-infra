from __future__ import annotations

import hashlib
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from dbt_metricflow_service.admin import register_bindings
from dbt_metricflow_service.platform.bindings import ProjectBinding, resolve_revision
from dbt_metricflow_service.storage.history_models import FixedCommitRequest
from tests.test_publication_storage import store as store


def git(repo: Path, *args: str) -> str:
    return subprocess.check_output(["git", "-C", str(repo), *args], text=True).strip()


def digest(repo: Path, sha: str, subdir: str = ".") -> str:
    prefix = "" if subdir == "." else f"{subdir}/"
    lines = git(repo, "ls-tree", "-r", sha).splitlines()
    result = hashlib.sha256()
    for line in lines:
        metadata, path = line.split("\t", 1)
        if not path.startswith(prefix):
            continue
        relative = path[len(prefix) :]
        if relative != "dbt_project.yml" and not relative.startswith("models/"):
            continue
        mode, _kind, blob = metadata.split()
        result.update(relative.encode() + b"\0" + f"{mode} {blob}".encode() + b"\0")
    return result.hexdigest()


@pytest.fixture
def repository(tmp_path: Path) -> Path:
    repo = tmp_path / "source"
    repo.mkdir()
    git(repo, "init", "-b", "main")
    git(repo, "config", "user.email", "test@example.invalid")
    git(repo, "config", "user.name", "Test")
    (repo / "dbt_project.yml").write_text("name: sample\n", encoding="utf-8")
    (repo / "models").mkdir()
    (repo / "models" / "a.sql").write_text("select 1 as value\n", encoding="utf-8")
    git(repo, "add", ".")
    git(repo, "commit", "-m", "A")
    return repo


def test_resolve_fixed_sha_and_digest(repository: Path, tmp_path: Path) -> None:
    first = git(repository, "rev-parse", "HEAD")
    expected = digest(repository, first)
    (repository / "models" / "a.sql").write_text("select 2 as value\n", encoding="utf-8")
    git(repository, "commit", "-am", "B")
    binding = ProjectBinding("sample", str(repository), ".", "postgres")

    project = resolve_revision(binding, first, expected, tmp_path / "work")

    assert (project / "models" / "a.sql").read_text(encoding="utf-8") == "select 1 as value\n"


def test_resolves_exact_old_commit_without_published_digest(repository, tmp_path):
    import dbt_metricflow_service.platform.bindings as platform_bindings

    assert hasattr(platform_bindings, "resolve_commit"), "fixed commit resolver is not implemented"
    old = git(repository, "rev-parse", "HEAD")
    expected = digest(repository, old)
    (repository / "models/a.sql").write_text("select 2\n", encoding="utf-8")
    git(repository, "commit", "-am", "newer unpublished source")
    project, actual = platform_bindings.resolve_commit(
        ProjectBinding("sample", str(repository), ".", "postgres"), old, tmp_path / "validation"
    )
    assert actual == expected
    assert (project / "models/a.sql").read_text("utf-8") == "select 1 as value\n"


def test_reject_unsafe_sha_subdir_symlink_and_digest(repository: Path, tmp_path: Path) -> None:
    sha = git(repository, "rev-parse", "HEAD")
    expected = digest(repository, sha)
    binding = ProjectBinding("sample", str(repository), ".", "postgres")
    with pytest.raises(ValueError):
        resolve_revision(binding, "HEAD", expected, tmp_path / "work")
    with pytest.raises(ValueError):
        resolve_revision(
            ProjectBinding("sample", str(repository), "../escape", "postgres"), sha, expected, tmp_path / "work"
        )
    with pytest.raises(ValueError):
        resolve_revision(binding, sha, "0" * 64, tmp_path / "work")

    # Git tree mode 120000 must be rejected even where filesystem symlinks are unavailable.
    (repository / "link").write_text("../outside", encoding="utf-8")
    git(repository, "add", "link")
    git(repository, "update-index", "--cacheinfo", "120000", git(repository, "rev-parse", ":link"), "link")
    git(repository, "commit", "-m", "symlink")
    linked_sha = git(repository, "rev-parse", "HEAD")
    with pytest.raises(ValueError):
        resolve_revision(binding, linked_sha, digest(repository, linked_sha), tmp_path / "work")


def test_remote_cannot_be_supplied_by_request() -> None:
    with pytest.raises(ValidationError):
        FixedCommitRequest.model_validate(
            {
                "projectId": "sample",
                "commitSha": "a" * 40,
                "projectDigest": "b" * 64,
                "profileBindingId": "postgres",
                "configVersion": "1",
                "idempotencyKey": "one",
                "remote": "https://untrusted.invalid/repo.git",
            }
        )


def test_fixed_schema_binding_is_controlled_by_service(tmp_path: Path, store) -> None:
    config = tmp_path / "bindings.json"
    config.write_text(
        '[{"repository":"https://example.invalid/project.git","executionBinding":"sample",'
        '"configVersion":"1","config":{"environments":["PREVIEW"],"profileBindingId":"starrocks","schemaName":"dbt_ecom"}}]',
        encoding="utf-8",
    )

    register_bindings(store.db, SimpleNamespace(max_artifact_file_bytes=4096, config_version="1"), config)

    with store.db.transaction() as connection:
        assert (
            connection.exec_driver_sql(
                "SELECT config_json FROM engine_execution_binding WHERE execution_binding=%s", ("sample",)
            ).scalar_one()["schemaName"]
            == "dbt_ecom"
        )
    with pytest.raises(ValidationError):
        FixedCommitRequest.model_validate(
            {
                "projectId": "sample",
                "commitSha": "a" * 40,
                "projectDigest": "b" * 64,
                "profileBindingId": "starrocks",
                "configVersion": "2",
                "idempotencyKey": "one",
                "schemaName": "attacker_db",
            }
        )


@pytest.mark.parametrize("schema", ["dbt-ecom", "", "a" * 257])
def test_fixed_schema_binding_rejects_unsafe_name(tmp_path: Path, schema: str) -> None:
    config = tmp_path / "bindings.json"
    config.write_text(
        '{"repository":"https://example.invalid/project.git","executionBinding":"sample",'
        '"configVersion":"1","config":{"environments":["PREVIEW"],"profileBindingId":"starrocks","schemaName":"'
        + schema
        + '"}}',
        encoding="utf-8",
    )
    config.write_text("[" + config.read_text(encoding="utf-8") + "]", encoding="utf-8")

    with pytest.raises(ValueError):
        register_bindings(None, SimpleNamespace(max_artifact_file_bytes=4096, config_version="1"), config)
