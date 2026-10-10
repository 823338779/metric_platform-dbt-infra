"""筛选条件固定的有界游标；仅用于浏览，不充当变化消费水位。"""

import base64
import json

from .builds import digest

CURSOR_VERSION = 1


def page(items, *, scope, cursor=None, limit=50, identity, reverse=False):
    if not 1 <= limit <= 200:
        raise ValueError("limit must be between 1 and 200")
    condition = digest(scope)
    after = None
    if cursor is not None:
        try:
            token = json.loads(base64.urlsafe_b64decode(cursor.encode()))
            if (
                token["version"] != CURSOR_VERSION
                or token["filter"] != condition
                or not isinstance(token["after"], str)
            ):
                raise ValueError("cursor filter mismatch")
            after = token["after"]
        except (ValueError, KeyError, TypeError, UnicodeError) as error:
            raise ValueError("invalid cursor") from error
    # 唯一身份排序保证并发新增记录不会令既有后续页移位。
    ordered = sorted(items, key=identity, reverse=reverse)
    if after is not None:
        ordered = [item for item in ordered if (identity(item) < after if reverse else identity(item) > after)]
    selected = ordered[:limit]
    next_cursor = None
    if len(ordered) > limit:
        raw = json.dumps({"version": CURSOR_VERSION, "filter": condition, "after": identity(selected[-1])})
        next_cursor = base64.urlsafe_b64encode(raw.encode()).decode()
    return selected, next_cursor
