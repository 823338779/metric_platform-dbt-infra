"""只读 Git 来源核对；不创建分支、不合并、不处理业务交付。"""

import tempfile
from pathlib import Path
from subprocess import CalledProcessError

from .bindings import SHA_PATTERN, _git

HEADS_PREFIX = "refs/heads/"
LS_REMOTE = "ls-remote"
HEADS = "--heads"
SEPARATOR = "--"


def remote_head(repository: str, branch_name: str, temp_root: Path) -> str | None:
    """成功读取但 ref 不存在返回 None；网络失败保持异常，不能当删除。"""
    temp_root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="head-", dir=temp_root) as directory:
        raw = _git(Path(directory), LS_REMOTE, HEADS, SEPARATOR, repository, HEADS_PREFIX + branch_name)
    rows = [line.split() for line in raw.decode().splitlines() if line]
    exact = [parts[0] for parts in rows if len(parts) == 2 and parts[1] == HEADS_PREFIX + branch_name]
    if not exact:
        return None
    if len(exact) != 1 or not SHA_PATTERN.fullmatch(exact[0]):
        raise ValueError("invalid remote branch response")
    return exact[0]


def is_ancestor(repository: str, ancestor: str, descendant: str, temp_root: Path) -> bool:
    """读取两份固定提交的共同祖先，不将分支可变名称作为输入。"""
    if not SHA_PATTERN.fullmatch(ancestor) or not SHA_PATTERN.fullmatch(descendant):
        raise ValueError("full commit SHA is required")
    temp_root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="ancestry-", dir=temp_root) as directory:
        root = Path(directory)
        _git(root, "init", "--bare")
        _git(root, "fetch", "--no-tags", SEPARATOR, repository, ancestor, descendant)
        try:
            bases = _git(root, "merge-base", ancestor, descendant).decode().splitlines()
        except ValueError as error:
            # merge-base 的 1 明确表示无共同祖先，不等于网络观察失败。
            if isinstance(error.__cause__, CalledProcessError) and error.__cause__.returncode == 1:
                return False
            raise
        return ancestor in bases
