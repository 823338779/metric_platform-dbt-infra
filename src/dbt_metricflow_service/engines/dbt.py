"""dbt 客户端：传递原生 Manifest 与执行上下文，不解释业务字段。"""

from __future__ import annotations

import json
from collections.abc import Callable
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, cast

from dbt.artifacts.schemas.manifest import WritableManifest
from dbt.artifacts.schemas.run import RunExecutionResult
from dbt.cli.main import dbtRunner
from dbt.cli.main import dbtRunnerResult as DbtResult
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.semantic_manifest import SemanticManifest
from dbt.exceptions import InvalidSelectorError
from metricflow_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from metricflow_semantics.model.dbt_manifest_parser import transform_dbt_generated_manifest

from dbt_metricflow_service import constants as strings
from dbt_metricflow_service.engines import InvalidDimensionsRequest
from dbt_metricflow_service.engines.models import DataQueryResult
from dbt_metricflow_service.engines.results import json_cell
from dbt_metricflow_service.models.contracts import JsonObject
from dbt_metricflow_service.models.queries import ResultColumn

if TYPE_CHECKING:
    from dbt_common.events.base_types import EventMsg

DBT_LIST = "ls"
MANIFEST_REQUIRED_MESSAGE = "saved Manifest is required for this operation"
LINEAGE_FAILED_MESSAGE = "dbt ls command failed"
INVALID_SELECTOR_MESSAGE = "invalid dbt selector"
PREVIEW_RESOURCE_UNAVAILABLE_MESSAGE = "预览资源不可查询"
DBT_SHOW = "show"
DBT_SELECT = "--select"
DBT_EXCLUDE = "--exclude"
DBT_SEED_SELECTOR = "resource_type:seed"
DBT_RESOURCE_TYPE = "--resource-type"
BUSINESS_RESOURCE_TYPES = (
    "source", "seed", "snapshot", "model", "semantic_model", "metric", "saved_query", "exposure",
)
DBT_OUTPUT = "--output"


class DbtClient:
    def __init__(self, manifest: bytes | None = None) -> None:
        self._content = manifest

    @cached_property
    def _artifact(self) -> WritableManifest:
        if self._content is None:
            raise ValueError(MANIFEST_REQUIRED_MESSAGE)
        return WritableManifest.from_dict(json.loads(self._content))

    @cached_property
    def _manifest(self) -> Manifest:
        return Manifest.from_writable_manifest(self._artifact)

    def invoke(
        self, args: list[str], *, callbacks: list[Callable[[EventMsg], None]] | None = None,
    ) -> DbtResult:
        """供执行层保留已有命令、事件与退出状态协议。"""
        return dbtRunner(
            manifest=self._manifest if self._content is not None else None, callbacks=callbacks,
        ).invoke(args)

    def build(
        self, project: Path, profiles: Path, *, target: str | None = None,
        target_path: Path | None = None, callbacks: list[Callable[[EventMsg], None]] | None = None,
    ) -> DbtResult:
        args = [strings.DBT_BUILD_COMMAND, strings.DBT_PROJECT_DIR_OPTION, str(project),
                strings.DBT_PROFILES_DIR_OPTION, str(profiles)]
        if target is not None:
            args.extend([strings.DBT_TARGET_OPTION, target])
        if target_path is not None:
            args.extend([strings.DBT_TARGET_PATH_OPTION, str(target_path)])
        return self.invoke(args, callbacks=callbacks)

    def resource(self, resource_id: str) -> JsonObject:
        return cast(JsonObject, self._manifest.expect(resource_id).to_dict(omit_none=False))

    def lineage(self, selector: str, *, project: Path, profiles: Path) -> list[JsonObject]:
        """使用 dbt 图选择表达式和业务资源白名单，返回原生节点 JSON。"""
        result = self.invoke([
            DBT_LIST, strings.DBT_PROJECT_DIR_OPTION, str(project), strings.DBT_PROFILES_DIR_OPTION, str(profiles),
            f"{DBT_SELECT}={selector}", DBT_OUTPUT, strings.JSON_MODE,
            DBT_RESOURCE_TYPE, *BUSINESS_RESOURCE_TYPES,
        ])
        if isinstance(result.exception, InvalidSelectorError):
            raise InvalidDimensionsRequest(INVALID_SELECTOR_MESSAGE) from result.exception
        if result.exception is not None:
            raise result.exception
        if not result.success:
            raise ValueError(LINEAGE_FAILED_MESSAGE)
        return [cast(JsonObject, json.loads(item)) for item in cast(list[str], result.result)]

    def source_path(self, resource_id: str) -> str:
        return self._manifest.expect(resource_id).original_file_path.replace("\\", "/")

    def semantic_manifest(self) -> PydanticSemanticManifest:
        """直接从构建 Manifest 转换内存语义模型，保留 MetricFlow 的原生转换规则。"""
        return transform_dbt_generated_manifest(SemanticManifest(self._manifest)._get_pydantic_semantic_manifest())

    @property
    def adapter_type(self) -> str:
        """读取构建时记录的 SQL 方言，不加载本地项目配置。"""
        return self._artifact.metadata.adapter_type

    def preview(self, selector: str, *, project: Path, profiles: Path, limit: int) -> DataQueryResult:
        result = self.invoke([
            DBT_SHOW, strings.DBT_PROJECT_DIR_OPTION, str(project), strings.DBT_PROFILES_DIR_OPTION, str(profiles),
            DBT_SELECT, selector, DBT_EXCLUDE, DBT_SEED_SELECTOR,
            strings.ADMIN_LIMIT_OPTION, str(limit + 1), DBT_OUTPUT, strings.JSON_MODE,
        ])
        if not result.success:
            raise ValueError(PREVIEW_RESOURCE_UNAVAILABLE_MESSAGE) from result.exception
        results = cast(RunExecutionResult, result.result).results
        if len(results) != 1 or results[0].agate_table is None:
            raise InvalidDimensionsRequest(PREVIEW_RESOURCE_UNAVAILABLE_MESSAGE)
        table = results[0].agate_table
        columns = [
            ResultColumn(name=name, type=type(table[0][index]).__name__ if table.rows else strings.UNKNOWN_VALUE_TYPE)
            for index, name in enumerate(table.column_names)
        ]
        rows = [[json_cell(cell) for cell in row] for row in table.rows[:limit]]
        return DataQueryResult(columns=columns, rows=rows, truncated=len(table.rows) > limit)
