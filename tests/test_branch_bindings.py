"""真实 Git 验证开发分支源码边界，不依赖 main 包含开发提交。"""

import pytest

from dbt_metricflow_service.platform.bindings import ProjectBinding, observe_revision, resolve_draft_revision
from tests.test_platform_bindings import git
from tests.test_platform_bindings import repository as repository

FEATURE = "feature-a"
REF = "refs/heads/feature-a"
CHECKOUT = "checkout"
NEW_BRANCH = "-b"
COMMIT = "commit"
ALL_MESSAGE = "-am"
MESSAGE = "feature change"
MODEL = "models/a.sql"
SQL = "select 2 as value\n"
UTF8 = "utf-8"
PROJECT = "sample"
ROOT = "."
PROFILE = "postgres"
WORK = "work"


def test_feature_commit_not_on_main_is_readable(repository, tmp_path):
    # 目标提交只存在于 feature，读该分支成功，默认 main 拒绝。
    git(repository, CHECKOUT, NEW_BRANCH, FEATURE)
    (repository / MODEL).write_text(SQL, encoding=UTF8)
    git(repository, COMMIT, ALL_MESSAGE, MESSAGE)
    binding = ProjectBinding(PROJECT, str(repository), ROOT, PROFILE)
    sha, digest = observe_revision(binding, tmp_path / WORK, git_ref=REF)
    path, actual = resolve_draft_revision(binding, sha, tmp_path / WORK, git_ref=REF)
    assert actual == digest
    assert (path / MODEL).read_text(encoding=UTF8) == SQL
    with pytest.raises(ValueError):
        resolve_draft_revision(binding, sha, tmp_path / WORK)


@pytest.mark.parametrize("ref", ["HEAD", "refs/tags/main", "refs/heads/../main", "--upload-pack=evil",
                                  "refs/heads/a:b", "refs/heads/a.lock", "refs/heads/a@{1}"])
def test_invalid_ref_is_rejected(repository, tmp_path, ref):
    # 输入仅接受完整 heads ref，不能解释为 Git 选项或 refspec。
    binding = ProjectBinding(PROJECT, str(repository), ROOT, PROFILE)
    with pytest.raises(ValueError):
        observe_revision(binding, tmp_path / WORK, git_ref=ref)
