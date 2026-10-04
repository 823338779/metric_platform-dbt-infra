"""失败构建的有界脱敏摘要；不公开 CLI 日志、SQL 和数据库连接信息。"""

import json
import re

RESULTS = "run_results.json"
UTF8 = "utf-8"
FAILED = "FAILED"
PASSED = "PASSED"
SKIPPED = "SKIPPED"
SUCCESS = frozenset({"pass", "success"})
NAME = re.compile(r"^[A-Za-z0-9_.-]{1,200}$")
MAX_CHECKS = 100
SUMMARY = "validationSummary"
MESSAGE = "检查未通过，请检查定义与输入数据。"
PHASES_WITH_RESULTS = frozenset({"build", "test"})


def failure_summary(target, phase, max_bytes):
    # 只读取本 attempt 的当前构建结果；其他命令不能复用旧 run_results 冒充证明。
    checks, truncated = [], False
    path = target / RESULTS
    if phase in PHASES_WITH_RESULTS and path.is_file():
        with path.open("rb") as stream:
            raw = stream.read(max_bytes + 1)
        truncated = len(raw) > max_bytes
        if not truncated:
            try:
                rows = json.loads(raw).get("results", [])
            except (ValueError, AttributeError):
                rows = []
            if isinstance(rows, list):
                truncated = len(rows) > MAX_CHECKS
                for row in rows[:MAX_CHECKS]:
                    if not isinstance(row, dict):
                        continue
                    name = row.get("unique_id", "")
                    if not isinstance(name, str) or not NAME.fullmatch(name):
                        truncated = True
                        continue
                    status = (PASSED if row.get("status") in SUCCESS else
                              SKIPPED if row.get("status") == "skipped" else FAILED)
                    checks.append({"name": name, "status": status, "message": MESSAGE if status == FAILED else None})
    if not checks:
        checks.append({"name": phase, "status": FAILED, "message": MESSAGE})
    return {"phase": phase, "checks": checks, "truncated": truncated}
