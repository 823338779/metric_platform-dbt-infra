"""等待后台线程结束后传播取消，保护文件与环境清理。"""
from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import ParamSpec, TypeVar

P = ParamSpec("P")
T = TypeVar("T")

async def _finish_task(task: asyncio.Task[T]) -> T:
    """重复取消只延迟调用者返回，不中断已经开始的清理或文件读写。"""

    cancelled = False
    while True:
        try:
            result = await asyncio.shield(task)
            break
        except asyncio.CancelledError:
            cancelled = True
            if task.cancelled():
                raise
        except Exception:
            if cancelled:
                raise asyncio.CancelledError from None
            raise
    if cancelled:
        raise asyncio.CancelledError
    return result


async def run_in_thread(function: Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> T:
    """取消时等待读写线程退出，防止清理后仍向已删除目录写入。"""

    return await _finish_task(asyncio.create_task(asyncio.to_thread(function, *args, **kwargs)))
