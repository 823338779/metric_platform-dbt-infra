"""耐久结果的有界传输；不重执行、不把保留行数当成数据库总行数。"""

import json

from dbt_metricflow_service.models.payloads import JsonObject

MAX_PAGE_BYTES = 8 * 1024 * 1024
MAX_PAGE_ROWS = 200


def result_page(payload: JsonObject, metadata: JsonObject, offset: int, limit: int) -> JsonObject:
    if offset < 0 or not 1 <= limit <= MAX_PAGE_ROWS:
        raise ValueError("INVALID_PAGE")
    available = len(payload.get("rows", []))
    rows = payload.get("rows", [])[offset : offset + limit]
    page = {
        **metadata,
        "columns": payload.get("columns", []),
        "rows": rows,
        "offset": offset,
        "availableRows": available,
        "resultTruncated": payload.get("truncated", False),
    }
    if "sql" in payload:
        page["sql"] = payload["sql"]
    while True:
        page.update(returnedRows=len(rows), nextOffset=offset + len(rows) if offset + len(rows) < available else None)
        if len(json.dumps(page, ensure_ascii=False, separators=(",", ":")).encode()) <= MAX_PAGE_BYTES:
            return page
        # 整行缩小，不截断数值；至少一行都放不下时明确错误，不返回无法推进的游标。
        if len(rows) <= 1:
            raise ValueError("RESULT_TOO_LARGE")
        rows.pop()
