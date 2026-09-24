from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID

from metricflow.engine.metricflow_engine import MetricFlowQueryRequest
from metricflow_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from metricflow_semantics.errors.error_classes import InvalidQueryException

from dbt_metricflow_service.platform_models import PlatformQueryRequest, QueryMode
from dbt_metricflow_service.platform_store import PlatformJobStore, RunState

ALLOWED_OPERATORS = frozenset({"=", "!=", ">", ">=", "<", "<=", "IN"})


def query_options(engine: Any, metrics: tuple[str, ...]) -> dict[str, Any]:
    """按 MetricFlow 原生语义返回多指标可组合维度与经 explain 核实的时间 token。"""

    available = {metric.name for metric in engine.list_metrics(include_dimensions=False)}
    if not metrics or not set(metrics) <= available:
        raise ValueError("指标不存在或为空")
    dimensions = [{
        "token": dimension.dunder_name, "name": dimension.name,
        "type": getattr(dimension.type, "value", str(dimension.type)),
    } for dimension in engine.list_dimensions(metric_names=list(metrics))]
    time_dimensions = []
    for granularity in TimeGranularity:
        token = f"metric_time__{granularity.value}"
        request = MetricFlowQueryRequest.create(metric_names=list(metrics), group_by_names=[token])
        try:
            engine.explain(request)
        except (InvalidQueryException, ValueError):
            continue
        time_dimensions.append({"token": token, "granularity": granularity.value})
    return {
        "metrics": [{"name": name} for name in metrics],
        "dimensions": dimensions, "timeDimensions": time_dimensions,
        "allowedFilters": sorted(ALLOWED_OPERATORS),
    }


def validate_query(request: PlatformQueryRequest, options: dict[str, Any]) -> None:
    """所有查询字段先与该固定 run 的原生可用项比对。"""

    metrics = {item["name"] for item in options["metrics"]}
    dimensions = {item["token"] for item in options["dimensions"]}
    time_dimensions = {item["token"] for item in options["timeDimensions"]}
    if not request.metrics or not set(request.metrics) <= metrics:
        raise ValueError("指标不可查询")
    if not set(request.group_by) <= dimensions | time_dimensions:
        raise ValueError("分组维度不可组合")
    orderable = metrics | dimensions | time_dimensions
    if any(item.lstrip("-") not in orderable for item in request.order_by):
        raise ValueError("排序字段不可组合")
    if request.mode is QueryMode.DIMENSION_VALUES and request.dimension not in dimensions | time_dimensions:
        raise ValueError("维度值字段不可查询")
    if any(item.field not in dimensions or item.operator not in ALLOWED_OPERATORS for item in request.filters):
        raise ValueError("筛选条件不在允许范围内")
    if request.start_time and request.end_time and request.start_time > request.end_time:
        raise ValueError("日期范围无效")


def _json_cell(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def serialize_rows(table: Any, *, limit: int) -> dict[str, Any]:
    """有界序列化 MetricFlowDataTable，Decimal 以字符串保留精度。"""

    columns = [{
        "name": column.column_name, "type": column.column_type.__name__,
    } for column in table.column_descriptions]
    rows = [[_json_cell(cell) for cell in row] for row in table.rows[:limit]]
    return {"columns": columns, "rows": rows, "truncated": len(table.rows) > limit}


class PlatformQueryCoordinator:
    """固定 run 上异步受理 MetricFlow 查询，结果只保存在任务目录。"""

    def __init__(self, store: PlatformJobStore, runs: Any, root: Path, profiles_dir: Path) -> None:
        self.store = store
        self.runs = runs
        self.root = root.resolve()
        self.profiles_dir = profiles_dir
        self._tasks: set[asyncio.Task[None]] = set()

    def _ready(self, run_id: UUID) -> tuple[Path, str, str]:
        snapshot = self.runs.get(run_id)
        if snapshot is None or snapshot["state"] != RunState.READY or snapshot.get("queryCapability") is not True:
            raise ValueError("固定版本尚不可查询")
        return self.runs.project_for_run(run_id), str(snapshot["schemaName"]), str(snapshot["profileBindingId"])

    def options(self, run_id: UUID, metrics: tuple[str, ...]) -> dict[str, Any]:
        from dbt_metricflow_service.platform_metricflow import invoke_programmatic

        project, schema, target = self._ready(run_id)
        directory = self.root / "options"
        directory.mkdir(parents=True, exist_ok=True)
        fingerprint = hashlib.sha256(json.dumps([str(run_id), metrics]).encode()).hexdigest()
        input_path = directory / f"{fingerprint}.input.json"
        output_path = directory / f"{fingerprint}.output.json"
        input_path.write_text(json.dumps({"mode": "OPTIONS", "metrics": metrics}), encoding="utf-8")
        return invoke_programmatic(project, self.profiles_dir, schema, target, input_path, output_path)

    def submit(self, request: PlatformQueryRequest) -> dict[str, str]:
        self._ready(request.run_id)
        fingerprint = hashlib.sha256(request.model_dump_json(exclude={"idempotency_key"}).encode()).hexdigest()
        query_id = self.store.reserve_query(request.idempotency_key, fingerprint, request.run_id)
        directory = self.root / str(query_id)
        directory.mkdir(parents=True, exist_ok=True)
        if self.store.claim_query(query_id, directory):
            (directory / "request.json").write_text(request.model_dump_json(by_alias=True), encoding="utf-8")
            task = asyncio.create_task(self._execute(query_id, request, directory))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)
        return {"queryId": str(query_id)}

    async def _execute(self, query_id: UUID, request: PlatformQueryRequest, directory: Path) -> None:
        from dbt_metricflow_service.platform_metricflow import invoke_programmatic

        try:
            project, schema, target = self._ready(request.run_id)
            input_path = directory / "input.json"
            output_path = directory / "result.json"
            input_path.write_text(
                json.dumps({"mode": request.mode.value, "request": request.model_dump(by_alias=True, mode="json")}),
                encoding="utf-8",
            )
            await asyncio.to_thread(
                invoke_programmatic, project, self.profiles_dir, schema, target, input_path, output_path
            )
            (directory / "READY").write_text("ready\n", encoding="utf-8")
            self.store.transition_query(query_id, RunState.READY, directory)
        except Exception as error:
            self.store.transition_query(query_id, RunState.FAILED, directory, type(error).__name__)

    def get(self, query_id: UUID) -> dict[str, Any] | None:
        record = self.store.find_query(query_id)
        if record is None:
            return None
        result: dict[str, Any] = {"queryId": str(query_id), "state": record.state.value}
        if record.state is RunState.READY and record.artifact_path:
            result.update(json.loads((record.artifact_path / "result.json").read_text(encoding="utf-8")))
        if record.error_code:
            result["errorCode"] = record.error_code
        return result

    def get_by_key(self, key: str) -> dict[str, Any] | None:
        record = self.store.find_query_by_key(key)
        return self.get(record.run_id) if record else None
