"""筛选条件固定的有界游标；仅用于浏览，不充当变化消费水位。"""

from __future__ import annotations

import base64
import hashlib
import json
from collections.abc import Callable, Iterable
from typing import Final, TypeVar

from pydantic import BaseModel, ConfigDict, Field, JsonValue

from dbt_metricflow_service import constants as strings

CURSOR_FILTER_MISMATCH_MESSAGE: Final = "cursor filter mismatch"
INVALID_CURSOR_MESSAGE: Final = strings.INVALID_CURSOR_MESSAGE
INVALID_PAGE_LIMIT_MESSAGE: Final = "limit must be between 1 and 200"

# 泛型保留调用方的元素或执行结果类型。
T = TypeVar("T")


class CursorToken(BaseModel):
    model_config = ConfigDict(extra=strings.EXTRA_FORBID, populate_by_name=True, strict=True)

    version: int
    filter_digest: str = Field(alias=strings.CURSOR_FILTER_ALIAS)
    after: str


CURSOR_VERSION = 1
HISTORY_CURSOR_VERSION = 2


def _scope_digest(scope: JsonValue) -> str:
    return hashlib.sha256(json.dumps(scope, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


def read_cursor(scope: JsonValue, cursor: str | None, limit: int, *, version: int = CURSOR_VERSION) -> str | None:
    if not 1 <= limit <= 200:
        raise ValueError(INVALID_PAGE_LIMIT_MESSAGE)
    condition = _scope_digest(scope)
    if cursor is not None:
        try:
            token = CursorToken.model_validate_json(base64.urlsafe_b64decode(cursor.encode()))
            if (
                token.version != version
                or token.filter_digest != condition
                or not isinstance(token.after, str)
            ):
                raise ValueError(CURSOR_FILTER_MISMATCH_MESSAGE)
            return token.after
        except (ValueError, KeyError, TypeError, UnicodeError) as error:
            raise ValueError(INVALID_CURSOR_MESSAGE) from error
    return None


def write_cursor(scope: JsonValue, after: str, *, version: int = CURSOR_VERSION) -> str:
    raw = CursorToken(version=version, filter_digest=_scope_digest(scope), after=after).model_dump_json(by_alias=True)
    return base64.urlsafe_b64encode(raw.encode()).decode()


def page(
    items: Iterable[T],
    *,
    scope: JsonValue,
    cursor: str | None = None,
    limit: int = 50,
    identity: Callable[[T], str],
    reverse: bool = False,
) -> tuple[list[T], str | None]:
    after = read_cursor(scope, cursor, limit)
    # 唯一身份排序保证并发新增记录不会令既有后续页移位。
    ordered = sorted(items, key=identity, reverse=reverse)
    if after is not None:
        ordered = [item for item in ordered if (identity(item) < after if reverse else identity(item) > after)]
    selected = ordered[:limit]
    next_cursor = None
    if len(ordered) > limit:
        next_cursor = write_cursor(scope, identity(selected[-1]))
    return selected, next_cursor
