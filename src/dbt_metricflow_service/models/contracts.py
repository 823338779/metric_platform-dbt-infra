"""Shared strict JSON contract and Git branch validation."""

import re
from typing import Final

from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel

from dbt_metricflow_service import constants as strings

INVALID_BRANCH_NAME_MESSAGE: Final[str] = 'branchName must be a short Git branch name'

SHA_PATTERN = r"^[0-9a-f]{40}(?:[0-9a-f]{24})?$"
REF_PREFIX = strings.GIT_REF_PREFIX
MAIN_BRANCH = "main"
INVALID_REF = re.compile(r"[\x00-\x20\x7f~^:?*\[\\]")
REF_SUFFIX = strings.GIT_LOCK_SUFFIX


class Contract(BaseModel):
    """公开 JSON 统一 camelCase，拒绝未声明的历史别名。"""

    model_config = ConfigDict(extra=strings.EXTRA_FORBID, populate_by_name=True, alias_generator=to_camel)


def validate_branch(value: str | None) -> str | None:
    """校验 Git 短分支名，防止 ref 或选项被混入远端读取。"""
    if value is None:
        return value
    if (
        not value
        or value.startswith((REF_PREFIX, "-", "/"))
        or value.endswith(("/", "."))
        or INVALID_REF.search(value)
        or ".." in value
        or "@{" in value
        or value == "@"
        or any(not part or part.startswith(".") or part.endswith(REF_SUFFIX) for part in value.split("/"))
    ):
        raise ValueError(INVALID_BRANCH_NAME_MESSAGE)
    return value


