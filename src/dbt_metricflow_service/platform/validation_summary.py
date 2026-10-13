"""失败构建的有界脱敏摘要；不公开 CLI 日志、SQL 和数据库连接信息。"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Final

from pydantic import Field, JsonValue, ValidationError

from dbt_metricflow_service import constants as strings
from dbt_metricflow_service.models.native import NativeProjection
from dbt_metricflow_service.models.payloads import ValidationCheck, ValidationSummary

RESULTS = strings.RUN_RESULTS_FILE
UTF8 = strings.UTF8
FAILED = strings.FAILED
PASSED = strings.PASSED
SKIPPED = strings.SKIPPED
SUCCESS = frozenset({strings.DBT_PASS_STATUS, strings.DBT_SUCCESS_STATUS})
NAME = re.compile(r"^[A-Za-z0-9_.-]{1,200}$")
MAX_CHECKS = 100
SUMMARY = strings.VALIDATION_SUMMARY_FIELD
MESSAGE: Final[str] = "检查未通过，请检查定义与输入数据。"
PHASES_WITH_RESULTS = frozenset({strings.DBT_BUILD_COMMAND, strings.DBT_TEST_COMMAND})


class CheckResult(NativeProjection):
    unique_id: str = ""
    status: str = ""


class CheckResults(NativeProjection):
    # A malformed entry must not hide valid checks elsewhere in the bounded result list.
    results: list[JsonValue] = Field(default_factory=list)


def failure_summary(target: Path, phase: str, max_bytes: int) -> ValidationSummary:
    # 只读取本 attempt 的当前构建结果；其他命令不能复用旧 run_results 冒充证明。
    checks: list[ValidationCheck] = []
    truncated = False
    path = target / RESULTS
    if phase in PHASES_WITH_RESULTS and path.is_file():
        with path.open(strings.BINARY_READ_MODE) as stream:
            raw = stream.read(max_bytes + 1)
        truncated = len(raw) > max_bytes
        if not truncated:
            try:
                rows = CheckResults.model_validate_json(raw).results
            except ValueError:
                rows = []
            if isinstance(rows, list):
                truncated = len(rows) > MAX_CHECKS
                for row in rows[:MAX_CHECKS]:
                    try:
                        check = CheckResult.model_validate(row)
                    except ValidationError:
                        truncated = True
                        continue
                    name = check.unique_id
                    if not isinstance(name, str) or not NAME.fullmatch(name):
                        truncated = True
                        continue
                    status = (
                        PASSED
                        if check.status in SUCCESS
                        else SKIPPED
                        if check.status == strings.DBT_SKIPPED_STATUS
                        else FAILED
                    )
                    checks.append(
                        ValidationCheck(name=name, status=status, message=MESSAGE if status == FAILED else None)
                    )
    if not checks:
        checks.append(ValidationCheck(name=phase, status=FAILED, message=MESSAGE))
    return ValidationSummary(phase=phase, checks=checks, truncated=truncated)
