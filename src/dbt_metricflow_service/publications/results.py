"""耐久结果的有界传输；不重执行、不把保留行数当成数据库总行数。"""

import json

from .errors import PublicationError

MAX_PAGE_BYTES = 8 * 1024 * 1024
MAX_PAGE_ROWS = 200


def task_error(code: str) -> dict:
    """只映射稳定错误码，原始数据库或 CLI 异常不进入公共协议。"""
    oversized = code == "RESULT_TOO_LARGE"
    return PublicationError("query_failed", "result_too_large" if oversized else "query_execution_failed", None,
        "结果超过保存上限，请缩小查询。" if oversized else "查询执行失败，请检查查询条件或服务状态。",
        False, "reduce_query" if oversized else "inspect_query").detail()


def result_page(payload: dict, metadata: dict, offset: int, limit: int) -> dict:
    if offset < 0 or not 1 <= limit <= MAX_PAGE_ROWS:
        raise PublicationError("invalid_query_selection", "invalid_page", "offset/limit",
                               "offset 必须非负，limit 必须介于 1 和 200。", False, "fix_pagination")
    available = len(payload.get("rows", []))
    rows = payload.get("rows", [])[offset:offset + limit]
    page = {**metadata, "state": "READY", "columns": payload.get("columns", []), "rows": rows,
            "offset": offset, "availableRows": available, "resultTruncated": payload.get("truncated", False)}
    if "sql" in payload:
        page["sql"] = payload["sql"]
    while True:
        page.update(returnedRows=len(rows), nextOffset=offset + len(rows) if offset + len(rows) < available else None)
        if len(json.dumps(page, ensure_ascii=False, separators=(",", ":")).encode()) <= MAX_PAGE_BYTES:
            return page
        # 整行缩小，不截断数值；至少一行都放不下时明确错误，不返回无法推进的游标。
        if len(rows) <= 1:
            raise PublicationError("result_too_large", "result_row_too_large", None,
                                   "单行或结果元数据超过响应上限，请缩小查询。", False, "reduce_query", 422)
        rows.pop()
